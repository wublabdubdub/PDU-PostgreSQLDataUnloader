/*
 * PDU - PostgreSQL Data Unloader
 * Copyright (c) 2024-2025 ZhangChen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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