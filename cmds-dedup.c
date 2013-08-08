/*
 * Copyright (C) 2013 Oracle.  All rights reserved.
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

#include <sys/ioctl.h>
#include <unistd.h>

#include "ctree.h"
#include "ioctl.h"

#include "commands.h"
#include "utils.h"

static const char * const dedup_cmd_group_usage[] = {
	"btrfs dedup <command> [options] <path>",
	NULL
};

int dedup_ctl(int cmd, int argc, char **argv)
{
	int ret = 0;
	int fd;
	int e;
	char *path = argv[1];
	DIR *dirstream;

	if (check_argc_exact(argc, 2))
		return -1;

	fd = open_file_or_dir(path, &dirstream);
	if (fd < 0) {
		fprintf(stderr, "ERROR: can't access '%s'\n", path);
		return -EACCES;
	}

	ret = ioctl(fd, BTRFS_IOC_DEDUP_CTL, cmd);
	e = errno;
	close_file_or_dir(fd, dirstream);
	if (ret < 0) {
		fprintf(stderr, "ERROR: dedup command failed: %s\n",
			strerror(e));
		if (cmd == BTRFS_DEDUP_CTL_UNREG)
			fprintf(stderr, "please refer to 'dmesg | tail' for more info\n");
		return -EINVAL;
	}
	return 0;
}

static const char * const cmd_dedup_reg_usage[] = {
	"btrfs dedup register <path>",
	"Enable data deduplication support for a filesystem.",
	NULL
};

static int cmd_dedup_reg(int argc, char **argv)
{
	int ret = dedup_ctl(BTRFS_DEDUP_CTL_REG, argc, argv);
	if (ret < 0)
		usage(cmd_dedup_reg_usage);
	return ret;
}

static const char * const cmd_dedup_unreg_usage[] = {
	"btrfs dedup unregister <path>",
	"Disable data deduplication support for a filesystem.",
	NULL
};

static int cmd_dedup_unreg(int argc, char **argv)
{
	int ret = dedup_ctl(BTRFS_DEDUP_CTL_UNREG, argc, argv);
	if (ret < 0)
		usage(cmd_dedup_unreg_usage);
	return ret;
}

const struct cmd_group dedup_cmd_group = {
	dedup_cmd_group_usage, NULL, {
		{ "register", cmd_dedup_reg, cmd_dedup_reg_usage, NULL, 0 },
		{ "unregister", cmd_dedup_unreg, cmd_dedup_unreg_usage, 0, 0 },
		{ 0, 0, 0, 0, 0 }
	}
};

int cmd_dedup(int argc, char **argv)
{
	return handle_command_group(&dedup_cmd_group, argc, argv);
}
