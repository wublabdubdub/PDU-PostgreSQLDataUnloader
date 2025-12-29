/*
 * PDU - PostgreSQL Data Unloader
 * Copyright (c) 2024-2025 ZhangChen
 *
 * Licensed under the Business Source License 1.1 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://mariadb.com/bsl11/
 *
 * Change Date: 2029-01-01
 * Change License: Apache License, Version 2.0
 *
 * See the License for the specific language governing permissions.
 *
 * dropscan_fs.h - File system operations interface for DROPSCAN feature
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <linux/fs.h>
#include <linux/fiemap.h>
#include <errno.h>

#define FIEMAP_EXTENT_SIZE  sizeof(struct fiemap_extent)

#define FIEMAP_MAX_OFFSET      (~0ULL)
#define FIEMAP_FLAG_SYNC       0x00000001
#define FIEMAP_FLAG_XATTR      0x00000002

#ifndef FS_IOC_FIEMAP
#define FS_IOC_FIEMAP      _IOWR('f', 11, struct fiemap)
#endif


int get_file_physical_offsets(const char *filepath,const char *filename);

int traverse_directory_recursive(const char *dirpath);

int process_directory(const char *dirpath);
