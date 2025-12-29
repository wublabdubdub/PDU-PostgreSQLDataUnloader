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
 */

#define _GNU_SOURCE
// #define _XOPEN_SOURCE 700
#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <string.h>
#include <ctype.h>
#include <sys/mman.h>  // mmap/munmap
#include <pthread.h>   // pthread_create, pthread_join, pthread_mutex_*
#if defined(_WIN32)
#include <windows.h>
#endif
#include "basic.h"
#include "tools.h"
#include "parray.h"
#include "decode.h"


/**
 * readItems - Parse data file and decode records
 *
 * @taboid:       Table structure with OID info
 * @filename:     Data file path to parse
 * @attr2Decode:  Column type definitions
 * @bootFileName: Output file name
 * @BOOTTYPE:     Read type (TABLE_BOOT, etc.)
 * @logPathSucc:  Success log file path
 * @logPathErr:   Error log file path
 *
 * Returns: 1 on success, 0 on failure
 */
int readItems(TABstruct *taboid,char *filename,char attr2Decode[],char *bootFileName,char *BOOTTYPE,char *logPathSucc,char *logPathErr);

DBstruct* bootDBStruct(char *filename,int isCompleted);

TABstruct* bootTabStruct(char *filename,int isCompleted);

harray* bootAttrStruct(char *filename);

SCHstruct* bootSCHStruct(char *filename);


void execCmd(int USR_CMD,char *former,char *latter,char *third,char *fourth);

void bootstrap();

void flushFinalCLass(TABstruct *taboid,int tabsize);

void bootMetaInfo2File(char *pgSchemaFile,char *pgClassFile,char *pgTypeFile,char *pgAttrFile);

void boot2File(char ATTR_ARRAY[],char *sourceFile, char *SOME_BOOT, char *SOME_BOOTTYPE);


void useDB(char *former,char *latter);

void setSCH(char *former,char *latter);

void getTabSize(TABSIZEstruct *tabsize);

void getFormVol(char *vol,char *ret);

void SHOW(char *former);

void DESC(char *former,char *latter);

void SCAN(char *former,char *latter);

void SET_PARAM(char *former,char *latter,char *third,char *fourth);

void RESET_PARAM(char *former,char *latter,char *third);

void RESTORE(char *former,char *latter,char *third,char *fourth);

void ADD_TAB(char *former,char *latter,char *third,char *fourth);

void UNLOAD(char *former,char *latter,char *third);

void unloadSCHDDL();

int unloadTAB(char *tabname);

int unloadSCH(char *schemaname);

void unloadCOPY(char *schemaname);


void EXIT();


char* getinput();

int getInit();

void setSrtWalname(char *third);

void setEndWalname(char *third);

void setExmode(char *third);

void setEncoding(char *third);

int unloadDB(char *databasename);

void setResmode(char *third);

void setTime(char *third,char *fourth,int flag);

void SHOW_PARAM();


int getToastHash(FILE *fp,unsigned int blockSize,FILE *destfp,int hundred);

parray* bootDropContext();

void INFO(char *latter);


int dcProc(int itemNum,dropContext *dc,char *page,int *consecutiveUnmatched,int *scanState,off_t *currOffset,int isIdx);

int dcProcforToast(int itemNum,dropContext *dc,char *page,int *consecutiveUnmatched,int *scanState,off_t *currOffset,int isIdx);

int dcProcFinal(int itemNum,dropContext *dc,char *page);

void dropScanIdx(parray *dcs,int isToastRound);

void dropScanNoIdx(parray *dcs,int isToastRound);

int initToastHash(char *CUR_DB,char *toastnode);

bool HeapTupleSatisfiesVisibility(HeapTupleHeader tuple);

void initWalScan(int flag,WALFILE *archDirFiles_array,WALFILE *walDirFiles_array);

void bootforDropScan(char *CUR_DB);

void META(char *type,char *objname);

int dcProcIso(int itemNum,dropContext *dc,char *page,int *consecutiveUnmatched,int *scanState,off_t *currOffset,int isIdx);

void bootstrap_abnormal();

void dropScanISO(parray *dcs,int isToastRound);

void DROP_SCAN(char *latter,char *third);

int dcProcforToastISO(int itemNum,dropContext *dc,char *page,int *consecutiveUnmatched,int *scanState,off_t *currOffset,int isIdx);

void dropscanIsoDict(char *phase);