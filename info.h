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
 */

#include "tools.h"

void printUsage();
typedef uint64 XLogRecPtr;
void infoWalRange(char *start_archfilename,char *end_archfilename);

void infoTxScanResult(DELstruct *elem,char *resStr);

void infoTimeScanResult(DELstruct *elem,char *resStr,TimestampTz *SrtTime,TimestampTz *EndTime);

void infoRestoreResult(char *tabname,int nTotal,int nItemsSucc,int nItemsErr,int FPIUpdateSame,char *csvpath,int resTyp_there);

void infoBootstrap(int stage,char *pgDatabaseFile,char *CUR_DB,char *pgSchemaFile,char *pgClassFile,
                    int tabSize,char *pgAttrFile,int attroidsize,char *CUR_SCH,int tabSizeOuput);

void infoUnlodResult(char *tabname,char *oidpath,int nPages,int nTotal,int nItemsSucc,int nItemsErr,char *csvpath);

void infoDBHeader();

void infoDescHeader(int flag);

void infoTabHeader();

void infoSchHeader();

void infoParamHeader();

void ErrorWalname();

void ErrorArchivePath(char *initArchPath);

void infoRestoreMode(char *item);

void ErrBlkNotFound(int blk,XLogRecPtr lsn);

void infoTimeRange(char *start,char *end);

void ErrorUpdDelWrong(int flag);

pg_time_t timestamptz_to_time_t_og(TimestampTz t);

uintptr_t timestamptz_to_str_og(TimestampTz dt);

void infoRestoreRecs(int FPIcount);

void InfoStartwalMeaning();

void warningUseDBFirst();


void infoDecodeLive(char *bootFileName,int nPages,int nItemsSucc);

void ErrUnknownParam(char *latter);

void infoUCopySucc(char *copyFilename,int file_count);

void infoUddlSucc(char *ddlFilename,int tabSize);

void infoUSchSucc(char *schemaname,int tabSize,int nNodata,int nErr,char *logPathErr,char *logPathSucc);

void ErrorFileNotExist(char *filename);

void ErrorDropScanOff();

void ErrorTabNotExist(char *tabname);

void infoPhysicalBlkHeader(char *filename,uint32 st_size,int flag);

void infoPhysycalBlkContect(long long a,long long b,long long c,int i);

void infoDirCleaned(char *dir);

void infoDropScanHeader(char *header,char *color);

void ErrorToastNoExist(Oid toastname);

void cprDeclaration(void);

void infoScanDropResult(dropElem *elem);

void ErrorDiskPathNotSet();

void ErrorSchNotExist(char *schname);

void infoRestoreDB();

void ErrLpIsEmpty(OffsetNumber offnum,BlockNumber blk);

void ErrorBlkLsnInfo(parray *LsnBlkInfos);

void warningInvalidLp();

void ErrorArchPathNotExist(char *path);

void infoDsIdxHeader();

void infoIsoScanModeHeader();

void ErrorOpenDisk(char *diskPath);

void infoDsIdxInfoHeader();

void ErrorISONotExist();

void infoIdxScanModeHeader();