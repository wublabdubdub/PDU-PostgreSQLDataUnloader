/*
 * This file contains code derived from PostgreSQL.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2024-2025 ZhangChen
 *
 * PostgreSQL-derived portions licensed under the PostgreSQL License;
 * see LICENSE-PostgreSQL.
 *
 * Original portions by ZhangChen licensed under BSL 1.1;
 * see LICENSE.
 */
#include "pg_xlogreader.h"


static inline void *
pg_malloc_internal(size_t size, int flags);

void *
palloc_extended(Size size, int flags);

void
WALOpenSegmentInit(WALOpenSegment *seg, WALSegmentContext *segcxt,
				   int segsize, const char *waldir);

bool
allocate_recordbuf(XLogReaderState *state, uint32 reclength);

XLogReaderState *
XLogReaderAllocate(int wal_segment_size, const char *waldir,
				   XLogReaderRoutine *routine, void *private_data);

int
open_file_in_directory(const char *directory, const char *fname);


bool search_directory(const char *directory, const char *fname);

char *pg_strdup(const char *in);

char *identify_target_directory(char *directory, char *fname);

void XLogDumpRecordLen(XLogReaderState *record, uint32 *rec_len, uint32 *fpi_len);

void XLogScanRecordForDisplay(XLogDumpConfig *config, XLogReaderState *record);

void WALDumpOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo,
				   TimeLineID *tli_p);

bool WALRead(XLogReaderState *state,
		char *buf, XLogRecPtr startptr, Size count, TimeLineID tli,
		WALReadError *errinfo);

int WALDumpReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetPtr, char *readBuff);

void WALDumpCloseSegment(XLogReaderState *state);

int ReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen);

XLogRecord *XLogReadRecord(XLogReaderState *state, char **errormsg);

XLogRecPtr XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr);

void pgGetTxforArch();

void setRestoreMode_there(int setting);

void setExportMode_there(int setting);

void setResTyp_there(int setting);

int restoreUPDATE(pg_attributeDesc *allDesc,XLogReaderState *record,parray *Tx_parray,FILE *bootFile,decodeFunc *array2Process,char *tabname,char *page,BlockNumber blk,bool hot_update,TransactionId currentTx);

void xact_desc_pg_drop(TimestampTz *TimeFromRecord,Oid *datafileOid,Oid *toastOid, XLogReaderState *record,RmgrId rmid,parray *TxTime_parray);

void mergeTxDelElems(parray *TxTime_parray);

bool FPWfileExist(BlockNumber blk, RelFileNumber filenode);

void FPW2File(BlockNumber blk,char* page,RelFileNumber filenode);

int FPWfromFile(BlockNumber blk, char* page, RelFileNumber filenode);

void FPWHashCleanup();

int XLogRecordRedoDropFPW(systemDropContext *sdc,XLogReaderState *record);
