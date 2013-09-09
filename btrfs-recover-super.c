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
#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <uuid/uuid.h>
#include <errno.h>
#include <unistd.h>
#include <ctype.h>
#include <getopt.h>

#include "kerncompat.h"
#include "ctree.h"
#include "disk-io.h"
#include "list.h"
#include "utils.h"
#include "crc32c.h"
#include "volumes.h"
#include "version.h"
#include "transaction.h"
#include "commands.h"

struct btrfs_recover_superblock {
	struct btrfs_fs_devices *fs_devices;

	struct list_head devices;

	/* max generation in fs*/
	u64 max_generation;
	int recover_flag;

	/* max generation superblock in fs*/
	struct btrfs_super_block *recover_super;
};

struct super_block_record {

	struct list_head list;
	struct btrfs_super_block sb;

	u64 bytenr;
};

struct device_record {
	char *device_name;

	struct list_head good_supers;
	struct list_head bad_supers;

	struct list_head list;

	/* max generation in disk*/
	u64 max_generation;
};

static
void init_recover_superblock(struct btrfs_recover_superblock *recover)
{
	INIT_LIST_HEAD(&recover->devices);

	recover->fs_devices = NULL;
	recover->recover_super = NULL;

	recover->max_generation = 0;
	recover->recover_flag = 0;
}

static void free_device_record(struct device_record *record)
{
	struct super_block_record *super_record;

	while (!list_empty(&record->good_supers)) {
		super_record = list_entry(record->good_supers.next,
				struct super_block_record, list);
		list_del_init(&super_record->list);
		free(super_record);
	}
	while (!list_empty(&record->bad_supers)) {
		super_record = list_entry(record->bad_supers.next,
				struct super_block_record, list);
		list_del_init(&super_record->list);
		free(super_record);
	}
	free(record->device_name);
	free(record);
}

static
void free_recover_superblock(struct btrfs_recover_superblock *recover)
{
	struct btrfs_device *device;
	struct device_record *device_record;

	if (!recover->fs_devices)
		return;

	while (!list_empty(&recover->fs_devices->devices)) {
		device = list_entry(recover->fs_devices->devices.next,
				struct btrfs_device, dev_list);
		list_del_init(&device->dev_list);
		free(device->name);
		free(device);
	}
	free(recover->fs_devices);

	while (!list_empty(&recover->devices)) {
		device_record = list_entry(recover->devices.next,
				struct device_record, list);
		list_del_init(&device_record->list);
		free_device_record(device_record);
	}
}

static int check_super(u64 bytenr, struct btrfs_super_block *sb)
{
	int csum_size = btrfs_super_csum_size(sb);
	char result[csum_size];
	u32 crc = ~(u32)0;

	if (btrfs_super_bytenr(sb) != bytenr)
		return 0;
	if (sb->magic != cpu_to_le64(BTRFS_MAGIC))
		return 0;

	crc = btrfs_csum_data(NULL, (char *)sb + BTRFS_CSUM_SIZE,
			crc, BTRFS_SUPER_INFO_SIZE - BTRFS_CSUM_SIZE);
	btrfs_csum_final(crc, result);

	return !memcmp(sb, &result, csum_size);
}

static int add_superblock_record(struct btrfs_super_block *sb, u64 bytenr,
				 struct device_record *device_record, int good)
{
	struct super_block_record *record;
	u64 gen;

	record = malloc(sizeof(struct super_block_record));
	if (!record)
		return -ENOMEM;

	memcpy(&record->sb, sb, sizeof(*sb));
	record->bytenr = bytenr;

	if (good) {
		list_add_tail(&record->list, &device_record->good_supers);
		gen = btrfs_super_generation(sb);
		if (gen > device_record->max_generation)
			device_record->max_generation = gen;
	} else {
			list_add_tail(&record->list, &device_record->bad_supers);
	}

	return 0;
}

struct device_record *add_device_record(char *device_name, struct list_head *devices)
{
	struct device_record *device_record;

	device_record = malloc(sizeof(struct device_record));
	if (!device_record)
		return NULL;

	device_record->device_name = strdup(device_name);
	if (!device_record->device_name) {
		free(device_record);
		return NULL;
	}

	INIT_LIST_HEAD(&device_record->good_supers);
	INIT_LIST_HEAD(&device_record->bad_supers);
	device_record->max_generation = 0;

	list_add_tail(&device_record->list, devices);

	return device_record;
}

static int
read_dev_supers(char *filename, struct btrfs_recover_superblock *recover)
{
	int i, ret, fd;
	u8 buf[BTRFS_SUPER_INFO_SIZE];
	u64 max_gen, bytenr;
	struct device_record *device_record;

	struct btrfs_super_block *sb = (struct btrfs_super_block *)buf;

	fd = open(filename, O_RDONLY, 0666);
	if (fd < 0)
		return -errno;

	device_record = add_device_record(filename, &recover->devices);
	if (!device_record)
		return -ENOMEM;

	for (i = 0; i < BTRFS_SUPER_MIRROR_MAX; i++) {
		bytenr = btrfs_sb_offset(i);
		ret = pread64(fd, buf, sizeof(buf), bytenr);
		if (ret < sizeof(buf)) {
			close(fd);
			return -errno;
		}
		ret = check_super(bytenr, sb);
		if (ret) {
			ret = add_superblock_record(sb, bytenr,
						    device_record, 1);
			if (ret)
				return ret;
			max_gen = btrfs_super_generation(sb);
			if (max_gen > recover->max_generation)
				recover->max_generation = max_gen;
		} else {
			ret = add_superblock_record(sb, bytenr,
						    device_record, 0);
			if (ret)
				return ret;
		}
	}
	close(fd);
	return ret;
}

static void update_read_result(struct btrfs_recover_superblock *recover)
{
	struct device_record *device_record;
	struct super_block_record *super_record;
	struct super_block_record *next;
	u64 gen;

	list_for_each_entry(device_record, &recover->devices, list) {
		list_for_each_entry_safe(super_record, next,
				&device_record->good_supers, list) {
			gen = btrfs_super_generation(&super_record->sb);
			if (gen < device_record->max_generation)
				list_move_tail(&super_record->list,
						&device_record->bad_supers);
			/* find max generation super in fs*/
			if (gen == recover->max_generation && !recover->recover_super)
				recover->recover_super = &super_record->sb;
		}
	}
}

static int read_fs_supers(struct btrfs_recover_superblock *recover)
{
	struct btrfs_device *dev;
	int ret;

	list_for_each_entry(dev, &recover->fs_devices->devices,
				dev_list) {
		ret = read_dev_supers(dev->name, recover);
		if (ret)
			return ret;
	}
	update_read_result(recover);

	return 0;
}

static int correct_bad_super(struct btrfs_dev_item *dev_item, u64 bytenr, int fd,
			struct btrfs_super_block *bad, struct btrfs_super_block *good)
{
	u32 crc;
	int ret;

	memcpy(bad, good, sizeof(*good));
	btrfs_set_super_bytenr(bad, bytenr);
	memcpy(&bad->dev_item, dev_item, sizeof(*dev_item));

	crc = ~(u32)0;
	crc = btrfs_csum_data(NULL, (char *)bad + BTRFS_CSUM_SIZE,
			crc, BTRFS_SUPER_INFO_SIZE - BTRFS_CSUM_SIZE);
	btrfs_csum_final(crc, (char *)&bad->csum[0]);

	ret = pwrite64(fd, bad, BTRFS_SUPER_INFO_SIZE, bytenr);
	if (ret < BTRFS_SUPER_INFO_SIZE)
		return -errno;

	return 0;
}

/*
 *1. max generation
 *2. not max generation
 */
static int correct_disk_bad_supers(struct device_record *device_record,
				   struct btrfs_recover_superblock *recover)
{
	u64 gen;
	int fd;
	int err;
	int ret = 0;
	struct super_block_record *super_record;
	struct super_block_record *next;
	struct btrfs_super_block *super;
	struct btrfs_dev_item dev_item;
	int flag = 0;

	fd = open(device_record->device_name, O_WRONLY);
	if (fd < 0)
		return -errno;

	list_for_each_entry_safe(super_record, next,
				&device_record->good_supers, list) {
		super = &super_record->sb;
		if (!flag) {
			memcpy(&dev_item, &super->dev_item,
				sizeof(struct btrfs_dev_item));
			flag = 1;
		}
		gen = btrfs_super_generation(super);
		if (gen >= recover->max_generation)
			break;

		err = correct_bad_super(&dev_item, super_record->bytenr,
					fd, super, recover->recover_super);
		if (err) {
			fprintf(stderr, "Failed to correct device: %s super offset: %llu\n",
				device_record->device_name, super_record->bytenr);
			if (super_record->bytenr == btrfs_sb_offset(0))
				recover->recover_flag = 1;
			else
				recover->recover_flag = 2;
			ret = err;
		} else if (!recover->recover_flag) {
			recover->recover_flag = 3;
		}
	}

	list_for_each_entry_safe(super_record, next,
				&device_record->bad_supers, list) {
		super = &super_record->sb;
		BUG_ON(!flag);

		err = correct_bad_super(&dev_item, super_record->bytenr,
					fd, super, recover->recover_super);
		if (err) {
			fprintf(stderr, "Failed to correct device: %s super offset: %llu\n",
				device_record->device_name, super_record->bytenr);
			if (super_record->bytenr == btrfs_sb_offset(0))
				recover->recover_flag = 1;
			else
				recover->recover_flag = 2;
			ret = err;

		} else {
			list_move_tail(&super_record->list,
				&device_record->good_supers);
			if (!recover->recover_flag)
				recover->recover_flag = 3;
		}
	}
	close(fd);
	return ret;
}

/*
 * iterate every disk and recover bad supers from good copies
 */
static int recover_fs_bad_supers(struct btrfs_recover_superblock *recover)
{

	struct device_record *device_record;
	int ret = 0;
	int err;

	if (list_empty(&recover->devices))
		return 0;

	list_for_each_entry(device_record, &recover->devices, list) {
		err = correct_disk_bad_supers(device_record, recover);
		if (err)
			ret = err;
	}
	return ret;
}

static void err_recover_result(int recover_flag)
{
	switch (recover_flag) {
	case 0:
		fprintf(stdout,
			"All superblocks are valid, no need to recover\n");
		break;
	case 1:
		fprintf(stdout,
			"Some fatal superblocks failed to recover\n");
		break;
	case 2:
		fprintf(stdout,
			"some backup superblocks failed to recover\n");
		break;
	case 3:
		fprintf(stdout,
			"recover all bad superblocks successfully\n");
		break;
	default:
		fprintf(stdout, "Unknown recover result\n");
		break;
	}
}

static void print_usage()
{
	fprintf(stderr,
		"usage: btrfs-recover-super [options] <device>\n\n");
	fprintf(stderr, "\trecover bad superblocks from copies\n\n");
	fprintf(stderr, "\t-v	Verbose mode\n");
}

static void print_all_devices(struct list_head *devices)
{
	struct btrfs_device *dev;

	printf("All Devices:\n");
	list_for_each_entry(dev, devices, dev_list) {
		printf("\t");
		printf("Device: id = %llu, name = %s\n",
			dev->devid, dev->name);
	}
	printf("\n");
}

static void print_disk_info(struct device_record *record)
{

	struct super_block_record *super_record;

	printf("[device name] = %s\n", record->device_name);

	printf("\tgood supers:\n");
	list_for_each_entry(super_record, &record->good_supers, list) {
		printf("\t\tsuperblock bytenr = %llu\n", super_record->bytenr);
	}
	printf("\n");

	printf("\tbad supers:\n");
	list_for_each_entry(super_record, &record->bad_supers, list) {
		printf("\t\tsuperblock bytenr = %llu\n", super_record->bytenr);
	}
	printf("\n");
}

static void print_all_supers(struct btrfs_recover_superblock *recover)
{
	struct device_record *device_record;

	list_for_each_entry(device_record, &recover->devices, list) {
		print_disk_info(device_record);
	}
}

int main(int argc, char **argv)
{
	struct btrfs_recover_superblock recover;
	struct btrfs_trans_handle *trans;
	struct btrfs_root *root = NULL;
	int ret, fd;
	int verbose = 0;
	char *dname;

	while (1) {
		int c = getopt(argc, argv, "v");
		if (c < 0)
			break;
		switch (c) {
		case 'v':
			verbose = 1;
			break;
		default:
			print_usage();
			return 1;
		}
	}
	argc = argc - optind;
	if (argc != 1) {
		print_usage();
		return 1;
	}

	dname = argv[optind];
	ret = check_mounted(dname);
	if (ret) {
		fprintf(stderr, "the device is busy\n");
		return ret;
	}

	fd = open(dname, O_RDONLY);
	if (fd < 0) {
		fprintf(stderr, "open %s error\n", dname);
		return 1;
	}
	init_recover_superblock(&recover);

	ret = btrfs_scan_fs_devices(fd, dname, &recover.fs_devices, 0);
	close(fd);
	if (ret)
		goto out;

	ret = read_fs_supers(&recover);
	if (ret)
		goto out;
	if (verbose) {
		print_all_devices(&recover.fs_devices->devices);
		printf("Before Recovering:\n");
		print_all_supers(&recover);
	}

	ret = recover_fs_bad_supers(&recover);
	if (ret)
		goto out;

	root = open_ctree(dname, 0, O_RDWR);
	BUG_ON(!root);

	trans = btrfs_start_transaction(root, 0);
	BUG_ON(!trans);

	ret = btrfs_commit_transaction(trans, root);
	if (ret)
		printf("Trying to recalucate device item fails\n");
out:
	if (verbose) {
		printf("After Recovering:\n");
		print_all_supers(&recover);
	}
	err_recover_result(recover.recover_flag);
	close_ctree(root);
	free_recover_superblock(&recover);
	return !!ret;
}
