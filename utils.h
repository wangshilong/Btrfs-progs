/*
 * Copyright (C) 2007 Oracle.  All rights reserved.
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

#ifndef __UTILS__
#define __UTILS__

#include <sys/stat.h>
#include "ctree.h"
#include <dirent.h>

#define BTRFS_MKFS_SYSTEM_GROUP_SIZE (4 * 1024 * 1024)

int make_btrfs(int fd, const char *device, const char *label,
	       u64 blocks[6], u64 num_bytes, u32 nodesize,
	       u32 leafsize, u32 sectorsize, u32 stripesize);
int btrfs_make_root_dir(struct btrfs_trans_handle *trans,
			struct btrfs_root *root, u64 objectid);
int btrfs_prepare_device(int fd, char *file, int zero_end, u64 *block_count_ret,
			 u64 max_block_count, int *mixed, int nodiscard);
int btrfs_add_to_fsid(struct btrfs_trans_handle *trans,
		      struct btrfs_root *root, int fd, char *path,
		      u64 block_count, u32 io_width, u32 io_align,
		      u32 sectorsize);
int btrfs_scan_for_fsid(int run_ioctls);
void btrfs_register_one_device(char *fname);
int btrfs_scan_one_dir(char *dirname, int run_ioctl);
int check_mounted(const char *devicename);
int check_mounted_where(int fd, const char *file, char *where, int size,
			struct btrfs_fs_devices **fs_devices_mnt);
int btrfs_device_already_in_root(struct btrfs_root *root, int fd,
				 int super_offset);

void pretty_size_snprintf(u64 size, char *str, size_t str_bytes);
#define pretty_size(size) 						\
	({								\
		static __thread char _str[24];				\
		pretty_size_snprintf((size), _str, sizeof(_str));	\
		_str;							\
	})

int get_mountpt(char *dev, char *mntpt, size_t size);
int btrfs_scan_block_devices(int run_ioctl);
u64 parse_size(char *s);
int open_file_or_dir(const char *fname, DIR **dirstream);
void close_file_or_dir(int fd, DIR *dirstream);
int get_device_info(int fd, u64 devid,
		    struct btrfs_ioctl_dev_info_args *di_args);
int get_fs_info(char *path, struct btrfs_ioctl_fs_info_args *fi_args,
		struct btrfs_ioctl_dev_info_args **di_ret);
int get_label(const char *btrfs_dev);
int set_label(const char *btrfs_dev, const char *label);

char *__strncpy__null(char *dest, const char *src, size_t n);
int is_block_device(const char *file);
int get_btrfs_mount(const char *path, char *mp, size_t mp_size);
int open_path_or_dev_mnt(const char *path, DIR **dirstream);
int is_swap_device(const char *file);
u64 btrfs_device_size(int fd, struct stat *st);
/* Helper to always get proper size of the destination string */
#define strncpy_null(dest, src) __strncpy__null(dest, src, sizeof(dest))
int test_dev_for_mkfs(char *file, int force_overwrite, char *estr);

#endif
