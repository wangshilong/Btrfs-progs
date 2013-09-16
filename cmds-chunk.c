/*
 * Copyright (C) 2013 Fujitsu.  All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License v2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA.
 */
#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include <stdio.h>
#include <stdio_ext.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <uuid/uuid.h>

#include "kerncompat.h"
#include "list.h"
#include "radix-tree.h"
#include "ctree.h"
#include "extent-cache.h"
#include "disk-io.h"
#include "volumes.h"
#include "transaction.h"
#include "crc32c.h"
#include "utils.h"
#include "version.h"
#include "btrfsck.h"
#include "commands.h"

#define BTRFS_CHUNK_TREE_REBUILD_ABORTED	-7500
#define BTRFS_STRIPE_LEN			(64 * 1024)
#define BTRFS_NUM_MIRRORS			2

struct recover_control {
	int verbose;
	int yes;

	u16 csum_size;
	u32 sectorsize;
	u32 leafsize;
	u64 generation;
	u64 chunk_root_generation;

	struct btrfs_fs_devices *fs_devices;

	struct cache_tree chunk;
	struct block_group_tree bg;
	struct device_extent_tree devext;
	struct cache_tree eb_cache;

	struct list_head good_chunks;
	struct list_head bad_chunks;
	struct list_head unrepaired_chunks;
};

struct extent_record {
	struct cache_extent cache;
	u64 generation;
	u8 csum[BTRFS_CSUM_SIZE];
	struct btrfs_device *devices[BTRFS_NUM_MIRRORS];
	u64 offsets[BTRFS_NUM_MIRRORS];
	int nmirrors;
};

static struct extent_record *btrfs_new_extent_record(struct extent_buffer *eb)
{
	struct extent_record *rec;

	rec = malloc(sizeof(*rec));
	if (!rec) {
		fprintf(stderr, "Fail to allocate memory for extent record.\n");
		exit(1);
	}

	memset(rec, 0, sizeof(*rec));
	rec->cache.start = btrfs_header_bytenr(eb);
	rec->cache.size = eb->len;
	rec->generation = btrfs_header_generation(eb);
	read_extent_buffer(eb, rec->csum, (unsigned long)btrfs_header_csum(eb),
			   BTRFS_CSUM_SIZE);
	return rec;
}

static int process_extent_buffer(struct cache_tree *eb_cache,
				 struct extent_buffer *eb,
				 struct btrfs_device *device, u64 offset)
{
	struct extent_record *rec;
	struct extent_record *exist;
	struct cache_extent *cache;
	int ret = 0;

	rec = btrfs_new_extent_record(eb);
	if (!rec->cache.size)
		goto free_out;
again:
	cache = lookup_cache_extent(eb_cache,
				    rec->cache.start,
				    rec->cache.size);
	if (cache) {
		exist = container_of(cache, struct extent_record, cache);

		if (exist->generation > rec->generation)
			goto free_out;
		if (exist->generation == rec->generation) {
			if (exist->cache.start != rec->cache.start ||
			    exist->cache.size != rec->cache.size ||
			    memcmp(exist->csum, rec->csum, BTRFS_CSUM_SIZE)) {
				ret = -EEXIST;
			} else {
				BUG_ON(exist->nmirrors >= BTRFS_NUM_MIRRORS);
				exist->devices[exist->nmirrors] = device;
				exist->offsets[exist->nmirrors] = offset;
				exist->nmirrors++;
			}
			goto free_out;
		}
		remove_cache_extent(eb_cache, cache);
		free(exist);
		goto again;
	}

	rec->devices[0] = device;
	rec->offsets[0] = offset;
	rec->nmirrors++;
	ret = insert_cache_extent(eb_cache, &rec->cache);
	BUG_ON(ret);
out:
	return ret;
free_out:
	free(rec);
	goto out;
}

static void free_extent_record(struct cache_extent *cache)
{
	struct extent_record *er;

	er = container_of(cache, struct extent_record, cache);
	free(er);
}

FREE_EXTENT_CACHE_BASED_TREE(extent_record, free_extent_record);

static struct btrfs_chunk *create_chunk_item(struct chunk_record *record)
{
	struct btrfs_chunk *ret;
	struct btrfs_stripe *chunk_stripe;
	int i;

	if (!record || record->num_stripes == 0)
		return NULL;
	ret = malloc(btrfs_chunk_item_size(record->num_stripes));
	if (!ret)
		return NULL;
	btrfs_set_stack_chunk_length(ret, record->length);
	btrfs_set_stack_chunk_owner(ret, record->owner);
	btrfs_set_stack_chunk_stripe_len(ret, record->stripe_len);
	btrfs_set_stack_chunk_type(ret, record->type_flags);
	btrfs_set_stack_chunk_io_align(ret, record->io_align);
	btrfs_set_stack_chunk_io_width(ret, record->io_width);
	btrfs_set_stack_chunk_sector_size(ret, record->sector_size);
	btrfs_set_stack_chunk_num_stripes(ret, record->num_stripes);
	btrfs_set_stack_chunk_sub_stripes(ret, record->sub_stripes);
	for (i = 0, chunk_stripe = &ret->stripe; i < record->num_stripes;
	     i++, chunk_stripe++) {
		btrfs_set_stack_stripe_devid(chunk_stripe,
				record->stripes[i].devid);
		btrfs_set_stack_stripe_offset(chunk_stripe,
				record->stripes[i].offset);
		memcpy(chunk_stripe->dev_uuid, record->stripes[i].dev_uuid,
		       BTRFS_UUID_SIZE);
	}
	return ret;
}

static void init_recover_control(struct recover_control *rc, int verbose,
		int yes)
{
	memset(rc, 0, sizeof(struct recover_control));
	cache_tree_init(&rc->chunk);
	cache_tree_init(&rc->eb_cache);
	block_group_tree_init(&rc->bg);
	device_extent_tree_init(&rc->devext);

	INIT_LIST_HEAD(&rc->good_chunks);
	INIT_LIST_HEAD(&rc->bad_chunks);
	INIT_LIST_HEAD(&rc->unrepaired_chunks);

	rc->verbose = verbose;
	rc->yes = yes;
}

static void free_recover_control(struct recover_control *rc)
{
	free_block_group_tree(&rc->bg);
	free_chunk_cache_tree(&rc->chunk);
	free_device_extent_tree(&rc->devext);
	free_extent_record_tree(&rc->eb_cache);
}

static int process_block_group_item(struct block_group_tree *bg_cache,
				    struct extent_buffer *leaf,
				    struct btrfs_key *key, int slot)
{
	struct block_group_record *rec;
	struct block_group_record *exist;
	struct cache_extent *cache;
	int ret = 0;

	rec = btrfs_new_block_group_record(leaf, key, slot);
	if (!rec->cache.size)
		goto free_out;
again:
	cache = lookup_cache_extent(&bg_cache->tree,
				    rec->cache.start,
				    rec->cache.size);
	if (cache) {
		exist = container_of(cache, struct block_group_record, cache);

		/*check the generation and replace if needed*/
		if (exist->generation > rec->generation)
			goto free_out;
		if (exist->generation == rec->generation) {
			int offset = offsetof(struct block_group_record,
					      generation);
			/*
			 * According to the current kernel code, the following
			 * case is impossble, or there is something wrong in
			 * the kernel code.
			 */
			if (memcmp(((void *)exist) + offset,
				   ((void *)rec) + offset,
				   sizeof(*rec) - offset))
				ret = -EEXIST;
			goto free_out;
		}
		remove_cache_extent(&bg_cache->tree, cache);
		list_del_init(&exist->list);
		free(exist);
		/*
		 * We must do seach again to avoid the following cache.
		 * /--old bg 1--//--old bg 2--/
		 *        /--new bg--/
		 */
		goto again;
	}

	ret = insert_block_group_record(bg_cache, rec);
	BUG_ON(ret);
out:
	return ret;
free_out:
	free(rec);
	goto out;
}

static int process_chunk_item(struct cache_tree *chunk_cache,
			      struct extent_buffer *leaf, struct btrfs_key *key,
			      int slot)
{
	struct chunk_record *rec;
	struct chunk_record *exist;
	struct cache_extent *cache;
	int ret = 0;

	rec = btrfs_new_chunk_record(leaf, key, slot);
	if (!rec->cache.size)
		goto free_out;
again:
	cache = lookup_cache_extent(chunk_cache, rec->offset, rec->length);
	if (cache) {
		exist = container_of(cache, struct chunk_record, cache);

		if (exist->generation > rec->generation)
			goto free_out;
		if (exist->generation == rec->generation) {
			int num_stripes = rec->num_stripes;
			int rec_size = btrfs_chunk_record_size(num_stripes);
			int offset = offsetof(struct chunk_record, generation);

			if (exist->num_stripes != rec->num_stripes ||
			    memcmp(((void *)exist) + offset,
				   ((void *)rec) + offset,
				   rec_size - offset))
				ret = -EEXIST;
			goto free_out;
		}
		remove_cache_extent(chunk_cache, cache);
		free(exist);
		goto again;
	}
	ret = insert_cache_extent(chunk_cache, &rec->cache);
	BUG_ON(ret);
out:
	return ret;
free_out:
	free(rec);
	goto out;
}

static int process_device_extent_item(struct device_extent_tree *devext_cache,
				      struct extent_buffer *leaf,
				      struct btrfs_key *key, int slot)
{
	struct device_extent_record *rec;
	struct device_extent_record *exist;
	struct cache_extent *cache;
	int ret = 0;

	rec = btrfs_new_device_extent_record(leaf, key, slot);
	if (!rec->cache.size)
		goto free_out;
again:
	cache = lookup_cache_extent2(&devext_cache->tree,
				     rec->cache.objectid,
				     rec->cache.start,
				     rec->cache.size);
	if (cache) {
		exist = container_of(cache, struct device_extent_record, cache);
		if (exist->generation > rec->generation)
			goto free_out;
		if (exist->generation == rec->generation) {
			int offset = offsetof(struct device_extent_record,
					      generation);
			if (memcmp(((void *)exist) + offset,
				   ((void *)rec) + offset,
				   sizeof(*rec) - offset))
				ret = -EEXIST;
			goto free_out;
		}
		remove_cache_extent(&devext_cache->tree, cache);
		list_del_init(&exist->chunk_list);
		list_del_init(&exist->device_list);
		free(exist);
		goto again;
	}

	ret = insert_device_extent_record(devext_cache, rec);
	BUG_ON(ret);
out:
	return ret;
free_out:
	free(rec);
	goto out;
}

static void print_block_group_info(struct block_group_record *rec, char *prefix)
{
	if (prefix)
		printf("%s", prefix);
	printf("Block Group: start = %llu, len = %llu, flag = %llx\n",
	       rec->objectid, rec->offset, rec->flags);
}

static void print_block_group_tree(struct block_group_tree *tree)
{
	struct cache_extent *cache;
	struct block_group_record *rec;

	printf("All Block Groups:\n");
	for (cache = first_cache_extent(&tree->tree); cache;
	     cache = next_cache_extent(cache)) {
		rec = container_of(cache, struct block_group_record, cache);
		print_block_group_info(rec, "\t");
	}
	printf("\n");
}

static void print_stripe_info(struct stripe *data, char *prefix1, char *prefix2,
			      int index)
{
	if (prefix1)
		printf("%s", prefix1);
	if (prefix2)
		printf("%s", prefix2);
	printf("[%2d] Stripe: devid = %llu, offset = %llu\n",
	       index, data->devid, data->offset);
}

static void print_chunk_self_info(struct chunk_record *rec, char *prefix)
{
	int i;

	if (prefix)
		printf("%s", prefix);
	printf("Chunk: start = %llu, len = %llu, type = %llx, num_stripes = %u\n",
	       rec->offset, rec->length, rec->type_flags, rec->num_stripes);
	if (prefix)
		printf("%s", prefix);
	printf("    Stripes list:\n");
	for (i = 0; i < rec->num_stripes; i++)
		print_stripe_info(&rec->stripes[i], prefix, "    ", i);
}

static void print_chunk_tree(struct cache_tree *tree)
{
	struct cache_extent *n;
	struct chunk_record *entry;

	printf("All Chunks:\n");
	for (n = first_cache_extent(tree); n;
	     n = next_cache_extent(n)) {
		entry = container_of(n, struct chunk_record, cache);
		print_chunk_self_info(entry, "\t");
	}
	printf("\n");
}

static void print_device_extent_info(struct device_extent_record *rec,
				     char *prefix)
{
	if (prefix)
		printf("%s", prefix);
	printf("Device extent: devid = %llu, start = %llu, len = %llu, chunk offset = %llu\n",
	       rec->objectid, rec->offset, rec->length, rec->chunk_offset);
}

static void print_device_extent_tree(struct device_extent_tree *tree)
{
	struct cache_extent *n;
	struct device_extent_record *entry;

	printf("All Device Extents:\n");
	for (n = first_cache_extent(&tree->tree); n;
	     n = next_cache_extent(n)) {
		entry = container_of(n, struct device_extent_record, cache);
		print_device_extent_info(entry, "\t");
	}
	printf("\n");
}

static void print_device_info(struct btrfs_device *device, char *prefix)
{
	if (prefix)
		printf("%s", prefix);
	printf("Device: id = %llu, name = %s\n",
	       device->devid, device->name);
}

static void print_all_devices(struct list_head *devices)
{
	struct btrfs_device *dev;

	printf("All Devices:\n");
	list_for_each_entry(dev, devices, dev_list)
		print_device_info(dev, "\t");
	printf("\n");
}

static void print_scan_result(struct recover_control *rc)
{
	if (!rc->verbose)
		return;

	printf("DEVICE SCAN RESULT:\n");
	printf("Filesystem Information:\n");
	printf("\tsectorsize: %d\n", rc->sectorsize);
	printf("\tleafsize: %d\n", rc->leafsize);
	printf("\ttree root generation: %llu\n", rc->generation);
	printf("\tchunk root generation: %llu\n", rc->chunk_root_generation);
	printf("\n");

	print_all_devices(&rc->fs_devices->devices);
	print_block_group_tree(&rc->bg);
	print_chunk_tree(&rc->chunk);
	print_device_extent_tree(&rc->devext);
}

static void print_chunk_info(struct chunk_record *chunk, char *prefix)
{
	struct device_extent_record *devext;
	int i;

	print_chunk_self_info(chunk, prefix);
	if (prefix)
		printf("%s", prefix);
	if (chunk->bg_rec)
		print_block_group_info(chunk->bg_rec, "    ");
	else
		printf("    No block group.\n");
	if (prefix)
		printf("%s", prefix);
	if (list_empty(&chunk->dextents)) {
		printf("    No device extent.\n");
	} else {
		printf("    Device extent list:\n");
		i = 0;
		list_for_each_entry(devext, &chunk->dextents, chunk_list) {
			if (prefix)
				printf("%s", prefix);
			printf("%s[%2d]", "        ", i);
			print_device_extent_info(devext, NULL);
			i++;
		}
	}
}

static void print_check_result(struct recover_control *rc)
{
	struct chunk_record *chunk;
	struct block_group_record *bg;
	struct device_extent_record *devext;
	int total = 0;
	int good = 0;
	int bad = 0;

	if (!rc->verbose)
		return;

	printf("CHECK RESULT:\n");
	printf("Healthy Chunks:\n");
	list_for_each_entry(chunk, &rc->good_chunks, list) {
		print_chunk_info(chunk, "  ");
		good++;
		total++;
	}
	printf("Bad Chunks:\n");
	list_for_each_entry(chunk, &rc->bad_chunks, list) {
		print_chunk_info(chunk, "  ");
		bad++;
		total++;
	}
	printf("\n");
	printf("Total Chunks:\t%d\n", total);
	printf("  Heathy:\t%d\n", good);
	printf("  Bad:\t%d\n", bad);

	printf("\n");
	printf("Orphan Block Groups:\n");
	list_for_each_entry(bg, &rc->bg.block_groups, list)
		print_block_group_info(bg, "  ");

	printf("\n");
	printf("Orphan Device Extents:\n");
	list_for_each_entry(devext, &rc->devext.no_chunk_orphans, chunk_list)
		print_device_extent_info(devext, "  ");
}

static int check_chunk_by_metadata(struct recover_control *rc,
				   struct btrfs_root *root,
				   struct chunk_record *chunk, int bg_only)
{
	int ret;
	int i;
	int slot;
	struct btrfs_path path;
	struct btrfs_key key;
	struct btrfs_root *dev_root;
	struct stripe *stripe;
	struct btrfs_dev_extent *dev_extent;
	struct btrfs_block_group_item *bg_ptr;
	struct extent_buffer *l;

	btrfs_init_path(&path);

	if (bg_only)
		goto bg_check;

	dev_root = root->fs_info->dev_root;
	for (i = 0; i < chunk->num_stripes; i++) {
		stripe = &chunk->stripes[i];

		key.objectid = stripe->devid;
		key.offset = stripe->offset;
		key.type = BTRFS_DEV_EXTENT_KEY;

		ret = btrfs_search_slot(NULL, dev_root, &key, &path, 0, 0);
		if (ret < 0) {
			fprintf(stderr, "Search device extent failed(%d)\n",
				ret);
			btrfs_release_path(&path);
			return ret;
		} else if (ret > 0) {
			if (rc->verbose)
				fprintf(stderr,
					"No device extent[%llu, %llu]\n",
					stripe->devid, stripe->offset);
			btrfs_release_path(&path);
			return -ENOENT;
		}
		l = path.nodes[0];
		slot = path.slots[0];
		dev_extent = btrfs_item_ptr(l, slot, struct btrfs_dev_extent);
		if (chunk->offset !=
		    btrfs_dev_extent_chunk_offset(l, dev_extent)) {
			if (rc->verbose)
				fprintf(stderr,
					"Device tree unmatch with chunks dev_extent[%llu, %llu], chunk[%llu, %llu]\n",
					btrfs_dev_extent_chunk_offset(l,
								dev_extent),
					btrfs_dev_extent_length(l, dev_extent),
					chunk->offset, chunk->length);
			btrfs_release_path(&path);
			return -ENOENT;
		}
		btrfs_release_path(&path);
	}

bg_check:
	key.objectid = chunk->offset;
	key.type = BTRFS_BLOCK_GROUP_ITEM_KEY;
	key.offset = chunk->length;

	ret = btrfs_search_slot(NULL, root->fs_info->extent_root, &key, &path,
				0, 0);
	if (ret < 0) {
		fprintf(stderr, "Search block group failed(%d)\n", ret);
		btrfs_release_path(&path);
		return ret;
	} else if (ret > 0) {
		if (rc->verbose)
			fprintf(stderr, "No block group[%llu, %llu]\n",
				key.objectid, key.offset);
		btrfs_release_path(&path);
		return -ENOENT;
	}

	l = path.nodes[0];
	slot = path.slots[0];
	bg_ptr = btrfs_item_ptr(l, slot, struct btrfs_block_group_item);
	if (chunk->type_flags != btrfs_disk_block_group_flags(l, bg_ptr)) {
		if (rc->verbose)
			fprintf(stderr,
				"Chunk[%llu, %llu]'s type(%llu) is differemt with Block Group's type(%llu)\n",
				chunk->offset, chunk->length, chunk->type_flags,
				btrfs_disk_block_group_flags(l, bg_ptr));
		btrfs_release_path(&path);
		return -ENOENT;
	}
	btrfs_release_path(&path);
	return 0;
}

static int check_all_chunks_by_metadata(struct recover_control *rc,
					struct btrfs_root *root)
{
	struct chunk_record *chunk;
	struct chunk_record *next;
	LIST_HEAD(orphan_chunks);
	int ret = 0;
	int err;

	list_for_each_entry_safe(chunk, next, &rc->good_chunks, list) {
		err = check_chunk_by_metadata(rc, root, chunk, 0);
		if (err) {
			if (err == -ENOENT)
				list_move_tail(&chunk->list, &orphan_chunks);
			else if (err && !ret)
				ret = err;
		}
	}

	list_for_each_entry_safe(chunk, next, &rc->unrepaired_chunks, list) {
		err = check_chunk_by_metadata(rc, root, chunk, 1);
		if (err == -ENOENT)
			list_move_tail(&chunk->list, &orphan_chunks);
		else if (err && !ret)
			ret = err;
	}

	list_for_each_entry(chunk, &rc->bad_chunks, list) {
		err = check_chunk_by_metadata(rc, root, chunk, 1);
		if (err != -ENOENT && !ret)
			ret = err ? err : -EINVAL;
	}
	list_splice(&orphan_chunks, &rc->bad_chunks);
	return ret;
}

static int extract_metadata_record(struct recover_control *rc,
				   struct extent_buffer *leaf)
{
	struct btrfs_key key;
	int ret = 0;
	int i;
	u32 nritems;

	nritems = btrfs_header_nritems(leaf);
	for (i = 0; i < nritems; i++) {
		btrfs_item_key_to_cpu(leaf, &key, i);
		switch (key.type) {
		case BTRFS_BLOCK_GROUP_ITEM_KEY:
			ret = process_block_group_item(&rc->bg, leaf, &key, i);
			break;
		case BTRFS_CHUNK_ITEM_KEY:
			ret = process_chunk_item(&rc->chunk, leaf, &key, i);
			break;
		case BTRFS_DEV_EXTENT_KEY:
			ret = process_device_extent_item(&rc->devext, leaf,
							 &key, i);
			break;
		}
		if (ret)
			break;
	}
	return ret;
}

static inline int is_super_block_address(u64 offset)
{
	int i;

	for (i = 0; i < BTRFS_SUPER_MIRROR_MAX; i++) {
		if (offset == btrfs_sb_offset(i))
			return 1;
	}
	return 0;
}

static int scan_one_device(struct recover_control *rc, int fd,
			   struct btrfs_device *device)
{
	struct extent_buffer *buf;
	u64 bytenr;
	int ret = 0;

	buf = malloc(sizeof(*buf) + rc->leafsize);
	if (!buf)
		return -ENOMEM;
	buf->len = rc->leafsize;

	bytenr = 0;
	while (1) {
		if (is_super_block_address(bytenr))
			bytenr += rc->sectorsize;

		if (pread64(fd, buf->data, rc->leafsize, bytenr) <
		    rc->leafsize)
			break;

		if (memcmp_extent_buffer(buf, rc->fs_devices->fsid,
					 (unsigned long)btrfs_header_fsid(buf),
					 BTRFS_FSID_SIZE)) {
			bytenr += rc->sectorsize;
			continue;
		}

		if (verify_tree_block_csum_silent(buf, rc->csum_size)) {
			bytenr += rc->sectorsize;
			continue;
		}

		ret = process_extent_buffer(&rc->eb_cache, buf, device, bytenr);
		if (ret)
			goto out;

		if (btrfs_header_level(buf) != 0)
			goto next_node;

		switch (btrfs_header_owner(buf)) {
		case BTRFS_EXTENT_TREE_OBJECTID:
		case BTRFS_DEV_TREE_OBJECTID:
			/* different tree use different generation */
			if (btrfs_header_generation(buf) > rc->generation)
				break;
			ret = extract_metadata_record(rc, buf);
			if (ret)
				goto out;
			break;
		case BTRFS_CHUNK_TREE_OBJECTID:
			if (btrfs_header_generation(buf) >
			    rc->chunk_root_generation)
				break;
			ret = extract_metadata_record(rc, buf);
			if (ret)
				goto out;
			break;
		}
next_node:
		bytenr += rc->leafsize;
	}
out:
	free(buf);
	return ret;
}

static int scan_devices(struct recover_control *rc)
{
	int ret = 0;
	int fd;
	struct btrfs_device *dev;
	int e;

	list_for_each_entry(dev, &rc->fs_devices->devices, dev_list) {
		fd = open(dev->name, O_RDONLY);
		if (fd < 0) {
			e = errno;
			fprintf(stderr, "Failed to open device %s\n",
				dev->name);
			return -e;
		}
		ret = scan_one_device(rc, fd, dev);
		close(fd);
		if (ret)
			return ret;
	}
	return ret;
}

static int build_device_map_by_chunk_record(struct btrfs_root *root,
					    struct chunk_record *chunk)
{
	int ret = 0;
	int i;
	u64 devid;
	u8 uuid[BTRFS_UUID_SIZE];
	u16 num_stripes;
	struct btrfs_mapping_tree *map_tree;
	struct map_lookup *map;
	struct stripe *stripe;

	map_tree = &root->fs_info->mapping_tree;
	num_stripes = chunk->num_stripes;
	map = malloc(btrfs_map_lookup_size(num_stripes));
	if (!map)
		return -ENOMEM;
	map->ce.start = chunk->offset;
	map->ce.size = chunk->length;
	map->num_stripes = num_stripes;
	map->io_width = chunk->io_width;
	map->io_align = chunk->io_align;
	map->sector_size = chunk->sector_size;
	map->stripe_len = chunk->stripe_len;
	map->type = chunk->type_flags;
	map->sub_stripes = chunk->sub_stripes;

	for (i = 0, stripe = chunk->stripes; i < num_stripes; i++, stripe++) {
		devid = stripe->devid;
		memcpy(uuid, stripe->dev_uuid, BTRFS_UUID_SIZE);
		map->stripes[i].physical = stripe->offset;
		map->stripes[i].dev = btrfs_find_device(root, devid,
							uuid, NULL);
		if (!map->stripes[i].dev) {
			kfree(map);
			return -EIO;
		}
	}

	ret = insert_cache_extent(&map_tree->cache_tree, &map->ce);
	return ret;
}

static int build_device_maps_by_chunk_records(struct recover_control *rc,
					      struct btrfs_root *root)
{
	int ret = 0;
	struct chunk_record *chunk;

	list_for_each_entry(chunk, &rc->good_chunks, list) {
		ret = build_device_map_by_chunk_record(root, chunk);
		if (ret)
			return ret;
	}
	return ret;
}

static int block_group_remove_all_extent_items(struct btrfs_trans_handle *trans,
					       struct btrfs_root *root,
					       struct block_group_record *bg)
{
	struct btrfs_fs_info *fs_info = root->fs_info;
	struct btrfs_key key;
	struct btrfs_path path;
	struct extent_buffer *leaf;
	u64 start = bg->objectid;
	u64 end = bg->objectid + bg->offset;
	u64 old_val;
	int nitems;
	int ret;
	int i;
	int del_s, del_nr;

	btrfs_init_path(&path);
	root = root->fs_info->extent_root;

	key.objectid = start;
	key.offset = 0;
	key.type = BTRFS_EXTENT_ITEM_KEY;
again:
	ret = btrfs_search_slot(trans, root, &key, &path, -1, 1);
	if (ret < 0)
		goto err;
	else if (ret > 0)
		ret = 0;

	leaf = path.nodes[0];
	nitems = btrfs_header_nritems(leaf);
	if (!nitems) {
		/* The tree is empty. */
		ret = 0;
		goto err;
	}

	if (path.slots[0] >= nitems) {
		ret = btrfs_next_leaf(root, &path);
		if (ret < 0)
			goto err;
		if (ret > 0) {
			ret = 0;
			goto err;
		}
		leaf = path.nodes[0];
		btrfs_item_key_to_cpu(leaf, &key, 0);
		if (key.objectid >= end)
			goto err;
		btrfs_release_path(&path);
		goto again;
	}

	del_nr = 0;
	del_s = -1;
	for (i = path.slots[0]; i < nitems; i++) {
		btrfs_item_key_to_cpu(leaf, &key, i);
		if (key.objectid >= end)
			break;

		if (key.type == BTRFS_BLOCK_GROUP_ITEM_KEY) {
			if (del_nr == 0)
				continue;
			else
				break;
		}

		if (del_s == -1)
			del_s = i;
		del_nr++;
		if (key.type == BTRFS_EXTENT_ITEM_KEY ||
		    key.type == BTRFS_METADATA_ITEM_KEY) {
			old_val = btrfs_super_bytes_used(fs_info->super_copy);
			if (key.type == BTRFS_METADATA_ITEM_KEY)
				old_val += root->leafsize;
			else
				old_val += key.offset;
			btrfs_set_super_bytes_used(fs_info->super_copy,
						   old_val);
		}
	}

	if (del_nr) {
		ret = btrfs_del_items(trans, root, &path, del_s, del_nr);
		if (ret)
			goto err;
	}

	if (key.objectid < end) {
		if (key.type == BTRFS_BLOCK_GROUP_ITEM_KEY) {
			key.objectid += root->sectorsize;
			key.type = BTRFS_EXTENT_ITEM_KEY;
			key.offset = 0;
		}
		btrfs_release_path(&path);
		goto again;
	}
err:
	btrfs_release_path(&path);
	return ret;
}

static int block_group_free_all_extent(struct btrfs_trans_handle *trans,
				       struct btrfs_root *root,
				       struct block_group_record *bg)
{
	struct btrfs_block_group_cache *cache;
	struct btrfs_fs_info *info;
	u64 start;
	u64 end;

	info = root->fs_info;
	cache = btrfs_lookup_block_group(info, bg->objectid);
	if (!cache)
		return -ENOENT;

	start = cache->key.objectid;
	end = start + cache->key.offset - 1;

	set_extent_bits(&info->block_group_cache, start, end,
			BLOCK_GROUP_DIRTY, GFP_NOFS);
	set_extent_dirty(&info->free_space_cache, start, end, GFP_NOFS);

	btrfs_set_block_group_used(&cache->item, 0);

	return 0;
}

static int remove_chunk_extent_item(struct btrfs_trans_handle *trans,
				    struct recover_control *rc,
				    struct btrfs_root *root)
{
	struct chunk_record *chunk;
	int ret = 0;

	list_for_each_entry(chunk, &rc->good_chunks, list) {
		if (!(chunk->type_flags & BTRFS_BLOCK_GROUP_SYSTEM))
			continue;
		ret = block_group_remove_all_extent_items(trans, root,
							  chunk->bg_rec);
		if (ret)
			return ret;

		ret = block_group_free_all_extent(trans, root, chunk->bg_rec);
		if (ret)
			return ret;
	}
	return ret;
}

static int __rebuild_chunk_root(struct btrfs_trans_handle *trans,
				struct recover_control *rc,
				struct btrfs_root *root)
{
	u64 min_devid = -1;
	struct btrfs_device *dev;
	struct extent_buffer *cow;
	struct btrfs_disk_key disk_key;
	int ret = 0;

	list_for_each_entry(dev, &rc->fs_devices->devices, dev_list) {
		if (min_devid > dev->devid)
			min_devid = dev->devid;
	}
	btrfs_set_disk_key_objectid(&disk_key, BTRFS_DEV_ITEMS_OBJECTID);
	btrfs_set_disk_key_type(&disk_key, BTRFS_DEV_ITEM_KEY);
	btrfs_set_disk_key_offset(&disk_key, min_devid);

	cow = btrfs_alloc_free_block(trans, root, root->sectorsize,
				     BTRFS_CHUNK_TREE_OBJECTID,
				     &disk_key, 0, 0, 0);
	btrfs_set_header_bytenr(cow, cow->start);
	btrfs_set_header_generation(cow, trans->transid);
	btrfs_set_header_nritems(cow, 0);
	btrfs_set_header_level(cow, 0);
	btrfs_set_header_backref_rev(cow, BTRFS_MIXED_BACKREF_REV);
	btrfs_set_header_owner(cow, BTRFS_CHUNK_TREE_OBJECTID);
	write_extent_buffer(cow, root->fs_info->fsid,
			(unsigned long)btrfs_header_fsid(cow),
			BTRFS_FSID_SIZE);

	write_extent_buffer(cow, root->fs_info->chunk_tree_uuid,
			(unsigned long)btrfs_header_chunk_tree_uuid(cow),
			BTRFS_UUID_SIZE);

	root->node = cow;
	btrfs_mark_buffer_dirty(cow);

	return ret;
}

static int __rebuild_device_items(struct btrfs_trans_handle *trans,
				  struct recover_control *rc,
				  struct btrfs_root *root)
{
	struct btrfs_device *dev;
	struct btrfs_key key;
	struct btrfs_dev_item *dev_item;
	int ret = 0;

	dev_item = malloc(sizeof(struct btrfs_dev_item));
	if (!dev_item)
		return -ENOMEM;

	list_for_each_entry(dev, &rc->fs_devices->devices, dev_list) {
		key.objectid = BTRFS_DEV_ITEMS_OBJECTID;
		key.type = BTRFS_DEV_ITEM_KEY;
		key.offset = dev->devid;

		btrfs_set_stack_device_generation(dev_item, 0);
		btrfs_set_stack_device_type(dev_item, dev->type);
		btrfs_set_stack_device_id(dev_item, dev->devid);
		btrfs_set_stack_device_total_bytes(dev_item, dev->total_bytes);
		btrfs_set_stack_device_bytes_used(dev_item, dev->bytes_used);
		btrfs_set_stack_device_io_align(dev_item, dev->io_align);
		btrfs_set_stack_device_io_width(dev_item, dev->io_width);
		btrfs_set_stack_device_sector_size(dev_item, dev->sector_size);
		memcpy(dev_item->uuid, dev->uuid, BTRFS_UUID_SIZE);
		memcpy(dev_item->fsid, dev->fs_devices->fsid, BTRFS_UUID_SIZE);

		ret = btrfs_insert_item(trans, root, &key,
					dev_item, sizeof(*dev_item));
	}

	free(dev_item);
	return ret;
}

static int __rebuild_chunk_items(struct btrfs_trans_handle *trans,
				 struct recover_control *rc,
				 struct btrfs_root *root)
{
	struct btrfs_key key;
	struct btrfs_chunk *chunk = NULL;
	struct btrfs_root *chunk_root;
	struct chunk_record *chunk_rec;
	int ret;

	chunk_root = root->fs_info->chunk_root;

	list_for_each_entry(chunk_rec, &rc->good_chunks, list) {
		chunk = create_chunk_item(chunk_rec);
		if (!chunk)
			return -ENOMEM;

		key.objectid = BTRFS_FIRST_CHUNK_TREE_OBJECTID;
		key.type = BTRFS_CHUNK_ITEM_KEY;
		key.offset = chunk_rec->offset;

		ret = btrfs_insert_item(trans, chunk_root, &key, chunk,
				btrfs_chunk_item_size(chunk_rec->num_stripes));
		free(chunk);
		if (ret)
			return ret;
	}
	return 0;
}

static int rebuild_chunk_tree(struct btrfs_trans_handle *trans,
			      struct recover_control *rc,
			      struct btrfs_root *root)
{
	int ret = 0;

	root = root->fs_info->chunk_root;

	ret = __rebuild_chunk_root(trans, rc, root);
	if (ret)
		return ret;

	ret = __rebuild_device_items(trans, rc, root);
	if (ret)
		return ret;

	ret = __rebuild_chunk_items(trans, rc, root);

	return ret;
}

static int rebuild_sys_array(struct recover_control *rc,
			     struct btrfs_root *root)
{
	struct btrfs_chunk *chunk;
	struct btrfs_key key;
	struct chunk_record *chunk_rec;
	int ret = 0;
	u16 num_stripes;

	btrfs_set_super_sys_array_size(root->fs_info->super_copy, 0);

	list_for_each_entry(chunk_rec, &rc->good_chunks, list) {
		if (!(chunk_rec->type_flags & BTRFS_BLOCK_GROUP_SYSTEM))
			continue;

		num_stripes = chunk_rec->num_stripes;
		chunk = create_chunk_item(chunk_rec);
		if (!chunk) {
			ret = -ENOMEM;
			break;
		}

		key.objectid = BTRFS_FIRST_CHUNK_TREE_OBJECTID;
		key.type = BTRFS_CHUNK_ITEM_KEY;
		key.offset = chunk_rec->offset;

		ret = btrfs_add_system_chunk(NULL, root, &key, chunk,
				btrfs_chunk_item_size(num_stripes));
		free(chunk);
		if (ret)
			break;
	}
	return ret;

}

static struct btrfs_root *
open_ctree_with_broken_chunk(struct recover_control *rc)
{
	struct btrfs_fs_info *fs_info;
	struct btrfs_super_block *disk_super;
	struct extent_buffer *eb;
	u32 sectorsize;
	u32 nodesize;
	u32 leafsize;
	u32 stripesize;
	int ret;

	fs_info = btrfs_new_fs_info(1, BTRFS_SUPER_INFO_OFFSET);
	if (!fs_info) {
		fprintf(stderr, "Failed to allocate memory for fs_info\n");
		return ERR_PTR(-ENOMEM);
	}

	fs_info->fs_devices = rc->fs_devices;
	ret = btrfs_open_devices(fs_info->fs_devices, O_RDWR);
	if (ret)
		goto out;

	disk_super = fs_info->super_copy;
	ret = btrfs_read_dev_super(fs_info->fs_devices->latest_bdev,
				   disk_super, fs_info->super_bytenr);
	if (ret) {
		fprintf(stderr, "No valid btrfs found\n");
		goto out_devices;
	}

	memcpy(fs_info->fsid, &disk_super->fsid, BTRFS_FSID_SIZE);

	ret = btrfs_check_fs_compatibility(disk_super, 1);
	if (ret)
		goto out_devices;

	nodesize = btrfs_super_nodesize(disk_super);
	leafsize = btrfs_super_leafsize(disk_super);
	sectorsize = btrfs_super_sectorsize(disk_super);
	stripesize = btrfs_super_stripesize(disk_super);

	__setup_root(nodesize, leafsize, sectorsize, stripesize,
		     fs_info->chunk_root, fs_info, BTRFS_CHUNK_TREE_OBJECTID);

	ret = build_device_maps_by_chunk_records(rc, fs_info->chunk_root);
	if (ret)
		goto out_cleanup;

	ret = btrfs_setup_all_roots(fs_info, 0, 0);
	if (ret)
		goto out_failed;

	eb = fs_info->tree_root->node;
	read_extent_buffer(eb, fs_info->chunk_tree_uuid,
			   (unsigned long)btrfs_header_chunk_tree_uuid(eb),
			   BTRFS_UUID_SIZE);

	return fs_info->fs_root;
out_failed:
	btrfs_release_all_roots(fs_info);
out_cleanup:
	btrfs_cleanup_all_caches(fs_info);
out_devices:
	btrfs_close_devices(fs_info->fs_devices);
out:
	btrfs_free_fs_info(fs_info);
	return ERR_PTR(ret);
}

static int recover_prepare(struct recover_control *rc, char *path)
{
	int ret;
	int fd;
	struct btrfs_super_block *sb;
	struct btrfs_fs_devices *fs_devices;

	ret = 0;
	fd = open(path, O_RDONLY);
	if (fd < 0) {
		fprintf(stderr, "open %s\n error.\n", path);
		return -1;
	}

	sb = malloc(sizeof(struct btrfs_super_block));
	if (!sb) {
		fprintf(stderr, "allocating memory for sb failed.\n");
		ret = -ENOMEM;
		goto fail_close_fd;
	}

	ret = btrfs_read_dev_super(fd, sb, BTRFS_SUPER_INFO_OFFSET);
	if (ret) {
		fprintf(stderr, "read super block error\n");
		goto fail_free_sb;
	}

	rc->sectorsize = btrfs_super_sectorsize(sb);
	rc->leafsize = btrfs_super_leafsize(sb);
	rc->generation = btrfs_super_generation(sb);
	rc->chunk_root_generation = btrfs_super_chunk_root_generation(sb);
	rc->csum_size = btrfs_super_csum_size(sb);

	/* if seed, the result of scanning below will be partial */
	if (btrfs_super_flags(sb) & BTRFS_SUPER_FLAG_SEEDING) {
		fprintf(stderr, "this device is seed device\n");
		ret = -1;
		goto fail_free_sb;
	}

	ret = btrfs_scan_fs_devices(fd, path, &fs_devices, 0, 1);
	if (ret)
		goto fail_free_sb;

	rc->fs_devices = fs_devices;

	if (rc->verbose)
		print_all_devices(&rc->fs_devices->devices);

fail_free_sb:
	free(sb);
fail_close_fd:
	close(fd);
	return ret;
}

/*
 * This reads a line from the stdin and only returns non-zero if the
 * first whitespace delimited token is a case insensitive match with yes
 * or y.
 */
static int ask_user(char *question)
{
	char buf[30] = {0,};
	char *saveptr = NULL;
	char *answer;

	printf("%s [y/N]: ", question);

	return fgets(buf, sizeof(buf) - 1, stdin) &&
	       (answer = strtok_r(buf, " \t\n\r", &saveptr)) &&
	       (!strcasecmp(answer, "yes") || !strcasecmp(answer, "y"));
}

static int btrfs_get_device_extents(u64 chunk_object,
				    struct list_head *orphan_devexts,
				    struct list_head *ret_list)
{
	struct device_extent_record *devext;
	struct device_extent_record *next;
	int count = 0;

	list_for_each_entry_safe(devext, next, orphan_devexts, chunk_list) {
		if (devext->chunk_offset == chunk_object) {
			list_move_tail(&devext->chunk_list, ret_list);
			count++;
		}
	}
	return count;
}

static int calc_num_stripes(u64 type)
{
	if (type & (BTRFS_BLOCK_GROUP_RAID0 |
		    BTRFS_BLOCK_GROUP_RAID10 |
		    BTRFS_BLOCK_GROUP_RAID5 |
		    BTRFS_BLOCK_GROUP_RAID6))
		return 0;
	else if (type & (BTRFS_BLOCK_GROUP_RAID1 |
			 BTRFS_BLOCK_GROUP_DUP))
		return 2;
	else
		return 1;
}

static inline int calc_sub_nstripes(u64 type)
{
	if (type & BTRFS_BLOCK_GROUP_RAID10)
		return 2;
	else
		return 1;
}

static int btrfs_verify_device_extents(struct block_group_record *bg,
				       struct list_head *devexts, int ndevexts)
{
	struct device_extent_record *devext;
	u64 strpie_length;
	int expected_num_stripes;

	expected_num_stripes = calc_num_stripes(bg->flags);
	if (expected_num_stripes && expected_num_stripes != ndevexts)
		return 1;

	strpie_length = calc_stripe_length(bg->flags, bg->offset, ndevexts);
	list_for_each_entry(devext, devexts, chunk_list) {
		if (devext->length != strpie_length)
			return 1;
	}
	return 0;
}

static int btrfs_rebuild_unordered_chunk_stripes(struct recover_control *rc,
						 struct chunk_record *chunk)
{
	struct device_extent_record *devext;
	struct btrfs_device *device;
	int i;

	devext = list_first_entry(&chunk->dextents, struct device_extent_record,
				  chunk_list);
	for (i = 0; i < chunk->num_stripes; i++) {
		chunk->stripes[i].devid = devext->objectid;
		chunk->stripes[i].offset = devext->offset;
		device = btrfs_find_device_by_devid(rc->fs_devices,
						    devext->objectid,
						    0);
		if (!device)
			return -ENOENT;
		BUG_ON(btrfs_find_device_by_devid(rc->fs_devices,
						  devext->objectid,
						  1));
		memcpy(chunk->stripes[i].dev_uuid, device->uuid,
		       BTRFS_UUID_SIZE);
		devext = list_next_entry(devext, chunk_list);
	}
	return 0;
}

static int btrfs_calc_stripe_index(struct chunk_record *chunk, u64 logical)
{
	u64 offset = logical - chunk->offset;
	int stripe_nr;
	int nr_data_stripes;
	int index;

	stripe_nr = offset / chunk->stripe_len;
	if (chunk->type_flags & BTRFS_BLOCK_GROUP_RAID0) {
		index = stripe_nr % chunk->num_stripes;
	} else if (chunk->type_flags & BTRFS_BLOCK_GROUP_RAID10) {
		index = stripe_nr % (chunk->num_stripes / chunk->sub_stripes);
		index *= chunk->sub_stripes;
	} else if (chunk->type_flags & BTRFS_BLOCK_GROUP_RAID5) {
		nr_data_stripes = chunk->num_stripes - 1;
		index = stripe_nr % nr_data_stripes;
		stripe_nr /= nr_data_stripes;
		index = (index + stripe_nr) % chunk->num_stripes;
	} else if (chunk->type_flags & BTRFS_BLOCK_GROUP_RAID6) {
		nr_data_stripes = chunk->num_stripes - 2;
		index = stripe_nr % nr_data_stripes;
		stripe_nr /= nr_data_stripes;
		index = (index + stripe_nr) % chunk->num_stripes;
	} else {
		BUG_ON(1);
	}
	return index;
}

/* calc the logical offset which is the start of the next stripe. */
static inline u64 btrfs_next_stripe_logical_offset(struct chunk_record *chunk,
						   u64 logical)
{
	u64 offset = logical - chunk->offset;

	offset /= chunk->stripe_len;
	offset *= chunk->stripe_len;
	offset += chunk->stripe_len;

	return offset + chunk->offset;
}

static int is_extent_record_in_device_extent(struct extent_record *er,
					     struct device_extent_record *dext,
					     int *mirror)
{
	int i;

	for (i = 0; i < er->nmirrors; i++) {
		if (er->devices[i]->devid == dext->objectid &&
		    er->offsets[i] >= dext->offset &&
		    er->offsets[i] < dext->offset + dext->length) {
			*mirror = i;
			return 1;
		}
	}
	return 0;
}

static int
btrfs_rebuild_ordered_meta_chunk_stripes(struct recover_control *rc,
					 struct chunk_record *chunk)
{
	u64 start = chunk->offset;
	u64 end = chunk->offset + chunk->length;
	struct cache_extent *cache;
	struct extent_record *er;
	struct device_extent_record *devext;
	struct device_extent_record *next;
	struct btrfs_device *device;
	LIST_HEAD(devexts);
	int index;
	int mirror;
	int ret;

	cache = lookup_cache_extent(&rc->eb_cache,
				    start, chunk->length);
	if (!cache) {
		/* No used space, we can reorder the stripes freely. */
		ret = btrfs_rebuild_unordered_chunk_stripes(rc, chunk);
		return ret;
	}

	list_splice_init(&chunk->dextents, &devexts);
again:
	er = container_of(cache, struct extent_record, cache);
	index = btrfs_calc_stripe_index(chunk, er->cache.start);
	if (chunk->stripes[index].devid)
		goto next;
	list_for_each_entry_safe(devext, next, &devexts, chunk_list) {
		if (is_extent_record_in_device_extent(er, devext, &mirror)) {
			chunk->stripes[index].devid = devext->objectid;
			chunk->stripes[index].offset = devext->offset;
			memcpy(chunk->stripes[index].dev_uuid,
			       er->devices[mirror]->uuid,
			       BTRFS_UUID_SIZE);
			index++;
			list_move(&devext->chunk_list, &chunk->dextents);
		}
	}
next:
	start = btrfs_next_stripe_logical_offset(chunk, er->cache.start);
	if (start >= end)
		goto no_extent_record;

	cache = lookup_cache_extent(&rc->eb_cache, start, end - start);
	if (cache)
		goto again;
no_extent_record:
	if (list_empty(&devexts))
		return 0;

	if (chunk->type_flags & (BTRFS_BLOCK_GROUP_RAID5 |
				 BTRFS_BLOCK_GROUP_RAID6)) {
		/* Fixme: try to recover the order by the parity block. */
		list_splice_tail(&devexts, &chunk->dextents);
		return -EINVAL;
	}

	/* There is no data on the lost stripes, we can reorder them freely. */
	for (index = 0; index < chunk->num_stripes; index++) {
		if (chunk->stripes[index].devid)
			continue;

		devext = list_first_entry(&devexts,
					  struct device_extent_record,
					   chunk_list);
		list_move(&devext->chunk_list, &chunk->dextents);

		chunk->stripes[index].devid = devext->objectid;
		chunk->stripes[index].offset = devext->offset;
		device = btrfs_find_device_by_devid(rc->fs_devices,
						    devext->objectid,
						    0);
		if (!device) {
			list_splice_tail(&devexts, &chunk->dextents);
			return -EINVAL;
		}
		BUG_ON(btrfs_find_device_by_devid(rc->fs_devices,
						  devext->objectid,
						  1));
		memcpy(chunk->stripes[index].dev_uuid, device->uuid,
		       BTRFS_UUID_SIZE);
	}
	return 0;
}

#define BTRFS_ORDERED_RAID	(BTRFS_BLOCK_GROUP_RAID0 |	\
				 BTRFS_BLOCK_GROUP_RAID10 |	\
				 BTRFS_BLOCK_GROUP_RAID5 |	\
				 BTRFS_BLOCK_GROUP_RAID6)

static int btrfs_rebuild_chunk_stripes(struct recover_control *rc,
				       struct chunk_record *chunk)
{
	int ret;

	/*
	 * All the data in the system metadata chunk will be dropped,
	 * so we need not guarantee that the data is right or not, that
	 * is we can reorder the stripes in the system metadata chunk.
	 */
	if ((chunk->type_flags & BTRFS_BLOCK_GROUP_METADATA) &&
	    (chunk->type_flags & BTRFS_ORDERED_RAID))
		ret =btrfs_rebuild_ordered_meta_chunk_stripes(rc, chunk);
	else if ((chunk->type_flags & BTRFS_BLOCK_GROUP_DATA) &&
		 (chunk->type_flags & BTRFS_ORDERED_RAID))
		ret = 1;	/* Be handled after the fs is opened. */
	else
		ret = btrfs_rebuild_unordered_chunk_stripes(rc, chunk);

	return ret;
}

static int btrfs_recover_chunks(struct recover_control *rc)
{
	struct chunk_record *chunk;
	struct block_group_record *bg;
	struct block_group_record *next;
	LIST_HEAD(new_chunks);
	LIST_HEAD(devexts);
	int nstripes;
	int ret;

	/* create the chunk by block group */
	list_for_each_entry_safe(bg, next, &rc->bg.block_groups, list) {
		nstripes = btrfs_get_device_extents(bg->objectid,
						    &rc->devext.no_chunk_orphans,
						    &devexts);
		chunk = malloc(btrfs_chunk_record_size(nstripes));
		if (!chunk)
			return -ENOMEM;
		memset(chunk, 0, btrfs_chunk_record_size(nstripes));
		INIT_LIST_HEAD(&chunk->dextents);
		chunk->bg_rec = bg;
		chunk->cache.start = bg->objectid;
		chunk->cache.size = bg->offset;
		chunk->objectid = BTRFS_FIRST_CHUNK_TREE_OBJECTID;
		chunk->type = BTRFS_CHUNK_ITEM_KEY;
		chunk->offset = bg->objectid;
		chunk->generation = bg->generation;
		chunk->length = bg->offset;
		chunk->owner = BTRFS_CHUNK_TREE_OBJECTID;
		chunk->stripe_len = BTRFS_STRIPE_LEN;
		chunk->type_flags = bg->flags;
		chunk->io_width = BTRFS_STRIPE_LEN;
		chunk->io_align = BTRFS_STRIPE_LEN;
		chunk->sector_size = rc->sectorsize;
		chunk->sub_stripes = calc_sub_nstripes(bg->flags);

		ret = insert_cache_extent(&rc->chunk, &chunk->cache);
		BUG_ON(ret);

		if (!nstripes) {
			list_add_tail(&chunk->list, &rc->bad_chunks);
			continue;
		}

		list_splice_init(&devexts, &chunk->dextents);

		ret = btrfs_verify_device_extents(bg, &devexts, nstripes);
		if (ret) {
			list_add_tail(&chunk->list, &rc->bad_chunks);
			continue;
		}

		chunk->num_stripes = nstripes;
		ret = btrfs_rebuild_chunk_stripes(rc, chunk);
		if (ret > 0)
			list_add_tail(&chunk->list, &rc->unrepaired_chunks);
		else if (ret < 0)
			list_add_tail(&chunk->list, &rc->bad_chunks);
		else
			list_add_tail(&chunk->list, &rc->good_chunks);
	}
	/*
	 * Don't worry about the lost orphan device extents, they don't
	 * have its chunk and block group, they must be the old ones that
	 * we have dropped.
	 */
	return 0;
}

static int btrfs_recover_chunk_tree(char *path, int verbose, int yes)
{
	int ret = 0;
	struct btrfs_root *root = NULL;
	struct btrfs_trans_handle *trans;
	struct recover_control rc;

	init_recover_control(&rc, verbose, yes);

	ret = recover_prepare(&rc, path);
	if (ret) {
		fprintf(stderr, "recover prepare error\n");
		return ret;
	}

	ret = scan_devices(&rc);
	if (ret) {
		fprintf(stderr, "scan chunk headers error\n");
		goto fail_rc;
	}

	if (cache_tree_empty(&rc.chunk) &&
	    cache_tree_empty(&rc.bg.tree) &&
	    cache_tree_empty(&rc.devext.tree)) {
		fprintf(stderr, "no recoverable chunk\n");
		goto fail_rc;
	}

	print_scan_result(&rc);

	ret = check_chunks(&rc.chunk, &rc.bg, &rc.devext, &rc.good_chunks,
			   &rc.bad_chunks, 1);
	print_check_result(&rc);
	if (ret) {
		if (!list_empty(&rc.bg.block_groups) ||
		    !list_empty(&rc.devext.no_chunk_orphans)) {
			ret = btrfs_recover_chunks(&rc);
			if (ret)
				goto fail_rc;
		}
		/*
		 * If the chunk is healthy, its block group item and device
		 * extent item should be written on the disks. So, it is very
		 * likely that the bad chunk is a old one that has been
		 * droppped from the fs. Don't deal with them now, we will
		 * check it after the fs is opened.
		 */
	}

	root = open_ctree_with_broken_chunk(&rc);
	if (IS_ERR(root)) {
		fprintf(stderr, "open with broken chunk error\n");
		ret = PTR_ERR(root);
		goto fail_rc;
	}

	ret = check_all_chunks_by_metadata(&rc, root);
	if (ret) {
		fprintf(stderr, "The chunks in memory can not match the metadata of the fs. Repair failed.\n");
		goto fail_close_ctree;
	}

	if (!rc.yes) {
		ret = ask_user("We are going to rebuild the chunk tree on disk, it might destroy the old metadata on the disk, Are you sure?");
		if (!ret) {
			ret = BTRFS_CHUNK_TREE_REBUILD_ABORTED;
			goto fail_close_ctree;
		}
	}

	trans = btrfs_start_transaction(root, 1);
	ret = remove_chunk_extent_item(trans, &rc, root);
	BUG_ON(ret);

	ret = rebuild_chunk_tree(trans, &rc, root);
	BUG_ON(ret);

	ret = rebuild_sys_array(&rc, root);
	BUG_ON(ret);

	btrfs_commit_transaction(trans, root);
fail_close_ctree:
	close_ctree(root);
fail_rc:
	free_recover_control(&rc);
	return ret;
}

const char * const cmd_chunk_recover_usage[] = {
	"btrfs chunk-recover [options] <device>",
	"Recover the chunk tree by scanning the devices one by one.",
	"",
	"-y	Assume an answer of `yes' to all questions",
	"-v	Verbose mode",
	"-h	Help",
	NULL
};

int cmd_chunk_recover(int argc, char *argv[])
{
	int ret = 0;
	char *file;
	int yes = 0;
	int verbose = 0;

	while (1) {
		int c = getopt(argc, argv, "yvh");
		if (c < 0)
			break;
		switch (c) {
		case 'y':
			yes = 1;
			break;
		case 'v':
			verbose = 1;
			break;
		case 'h':
		default:
			usage(cmd_chunk_recover_usage);
		}
	}

	argc = argc - optind;
	if (argc == 0)
		usage(cmd_chunk_recover_usage);

	file = argv[optind];

	ret = check_mounted(file);
	if (ret) {
		fprintf(stderr, "the device is busy\n");
		goto out;
	}

	ret = btrfs_recover_chunk_tree(file, verbose, yes);
	if (!ret) {
		fprintf(stdout, "Recover the chunk tree successfully.\n");
	} else if (ret == BTRFS_CHUNK_TREE_REBUILD_ABORTED) {
		ret = 0;
		fprintf(stdout, "Abort to rebuild the on-disk chunk tree.\n");
	} else {
		fprintf(stdout, "Fail to recover the chunk tree.\n");
	}
out:
	return !!ret;
}
