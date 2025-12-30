/*
 * This file contains code derived from PostgreSQL.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2024-2025 ZhangChen
 *
 * PostgreSQL-derived portions licensed under the PostgreSQL License;
 * see LICENSE-PostgreSQL.
 *
 * Original portions by ZhangChen licensed under the Apache License, Version 2.0;
 * see LICENSE.
 */
#include <stdbool.h>
#include "pg_xlogdefs.h"
#include "stringinfo.h"


size_t pg_pread(int fd, void *buf, size_t size, off_t offset);
#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */
#define MAXDATELEN		128


typedef uint64 XLogSegNo;
typedef uint32 TimeLineID;
// typedef uint64 XLogRecPtr;

#define MAXPGPATH		1024

#if __GNUC__ >= 3
#define likely(x)	__builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#else
#define likely(x)	((x) != 0)
#define unlikely(x) ((x) != 0)
#endif

#ifndef XLOGREADER_H
#define XLOGREADER_H

typedef Oid RelFileNumber;

typedef struct RelFileLocator
{
	Oid			spcOid;			/* tablespace */
	Oid			dbOid;			/* database */
	RelFileNumber relNumber;	/* relation */
} RelFileLocator;

typedef union PGAlignedBlock
{
	char		data[BLCKSZ];
	double		force_align_d;
	int64		force_align_i64;
} PGAlignedBlock;

/* WALOpenSegment represents a WAL segment being read. */
typedef struct WALOpenSegment
{
	int			ws_file;		/* segment file descriptor */
	XLogSegNo	ws_segno;		/* segment number */
	TimeLineID	ws_tli;			/* timeline ID of the currently open file */
} WALOpenSegment;

/* WALSegmentContext carries context information about WAL segments to read */
typedef struct WALSegmentContext
{
	char		ws_dir[MAXPGPATH];
	int			ws_segsize;
} WALSegmentContext;

typedef struct XLogReaderState XLogReaderState;

/* Function type definitions for various xlogreader interactions */
typedef int (*XLogPageReadCB) (XLogReaderState *xlogreader,
							   XLogRecPtr targetPagePtr,
							   int reqLen,
							   XLogRecPtr targetRecPtr,
							   char *readBuf);
typedef void (*WALSegmentOpenCB) (XLogReaderState *xlogreader,
								  XLogSegNo nextSegNo,
								  TimeLineID *tli_p);
typedef void (*WALSegmentCloseCB) (XLogReaderState *xlogreader);
#define MAXFNAMELEN		64
#define XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)	\
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,		\
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId(wal_segsz_bytes)), \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId(wal_segsz_bytes)))


#define XLogRecMaxBlockId(decoder) ((decoder)->record->max_block_id)


#define XLogSegNoOffsetToRecPtr(segno, offset, wal_segsz_bytes, dest) \
		(dest) = (segno) * (wal_segsz_bytes) + (offset)


typedef struct XLogReaderRoutine
{
	XLogPageReadCB page_read;
	WALSegmentOpenCB segment_open;
	WALSegmentCloseCB segment_close;
} XLogReaderRoutine;

#define XL_ROUTINE(...) &(XLogReaderRoutine){__VA_ARGS__}

typedef unsigned int Oid;

typedef struct RelFileNode
{
	Oid			spcNode;		/* tablespace */
	Oid			dbNode;			/* database */
	Oid			relNode;		/* relation */
} RelFileNode;

typedef enum ForkNumber
{
	InvalidForkNumber = -1,
	MAIN_FORKNUM = 0,
	FSM_FORKNUM,
	VISIBILITYMAP_FORKNUM,
	INIT_FORKNUM
} ForkNumber;
typedef uint32 BlockNumber;
#define MAX_FORKNUM		INIT_FORKNUM



typedef uint16 RepOriginId;
typedef uint32 TransactionId;
#define XLR_MAX_BLOCK_ID			32
typedef uint32 pg_crc32c;
typedef uint8 RmgrId;
typedef struct XLogRecord
{
	uint32		xl_tot_len;		/* total len of entire record */
	TransactionId xl_xid;		/* xact id */
	XLogRecPtr	xl_prev;		/* ptr to previous record in log */
	uint8		xl_info;		/* flag bits, see below */
	RmgrId		xl_rmid;		/* resource manager for this record */
	/* 2 bytes of padding here, initialize to zero */
	pg_crc32c	xl_crc;			/* CRC for this record */

	/* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */

} XLogRecord;

#if PG_VERSION_NUM > 14
typedef struct
{
	/* Is this block ref in use? */
	bool		in_use;

	/* Identify the block this refers to */
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blkno;

	/* Prefetching workspace. */
	Buffer		prefetch_buffer;

	/* copy of the fork_flags field from the XLogRecordBlockHeader */
	uint8		flags;

	/* Information on full-page image, if any */
	bool		has_image;		/* has image, even for consistency checking */
	bool		apply_image;	/* has image that should be restored */
	char	   *bkp_image;
	uint16		hole_offset;
	uint16		hole_length;
	uint16		bimg_len;
	uint8		bimg_info;

	/* Buffer holding the rmgr-specific data associated with this block */
	bool		has_data;
	char	   *data;
	uint16		data_len;
	uint16		data_bufsz;
} DecodedBkpBlock;
typedef struct DecodedXLogRecord
{
	/* Private member used for resource management. */
	size_t		size;			/* total size of decoded record */
	bool		oversized;		/* outside the regular decode buffer? */
	struct DecodedXLogRecord *next; /* decoded record queue link */

	/* Public members. */
	XLogRecPtr	lsn;			/* location */
	XLogRecPtr	next_lsn;		/* location of next record */
	XLogRecord	header;			/* header */
	RepOriginId record_origin;
	TransactionId toplevel_xid; /* XID of top-level transaction */
	char	   *main_data;		/* record's main data portion */
	uint32		main_data_len;	/* main data portion's length */
	int			max_block_id;	/* highest block_id in use (-1 if none) */
	DecodedBkpBlock blocks[FLEXIBLE_ARRAY_MEMBER];
} DecodedXLogRecord;

struct XLogReaderState
{
	XLogReaderRoutine routine;
	uint64		system_identifier;

	void	   *private_data;

	XLogRecPtr	ReadRecPtr;		/* start of last record read */
	XLogRecPtr	EndRecPtr;		/* end+1 of last record read */

	XLogRecPtr	abortedRecPtr;
	XLogRecPtr	missingContrecPtr;
	XLogRecPtr	overwrittenRecPtr;

	XLogRecPtr	DecodeRecPtr;	/* start of last record decoded */
	XLogRecPtr	NextRecPtr;		/* end+1 of last record decoded */
	XLogRecPtr	PrevRecPtr;		/* start of previous record decoded */

	DecodedXLogRecord *record;

	char	   *decode_buffer;
	size_t		decode_buffer_size;
	bool		free_decode_buffer; /* need to free? */
	char	   *decode_buffer_head; /* data is read from the head */
	char	   *decode_buffer_tail; /* new data is written at the tail */

	DecodedXLogRecord *decode_queue_head;	/* oldest decoded record */
	DecodedXLogRecord *decode_queue_tail;	/* newest decoded record */

	char	   *readBuf;
	uint32		readLen;

	WALSegmentContext segcxt;
	WALOpenSegment seg;
	uint32		segoff;

	XLogRecPtr	latestPagePtr;
	TimeLineID	latestPageTLI;

	XLogRecPtr	currRecPtr;
	TimeLineID	currTLI;


	XLogRecPtr	currTLIValidUntil;
	TimeLineID	nextTLI;
	char	   *readRecordBuf;
	uint32		readRecordBufSize;
	char	   *errormsg_buf;
	bool		errormsg_deferred;
	bool		nonblocking;
};

#else
#pragma pack(push, 1)
typedef struct
{
	/* Is this block ref in use? */
	bool		in_use;

	/* Identify the block this refers to */
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blkno;
	uint8		flags;
	bool		has_image;		/* has image, even for consistency checking */
	bool		apply_image;	/* has image that should be restored */
	char	   *bkp_image;
	uint16		hole_offset;
	uint16		hole_length;
	uint16		bimg_len;
	uint8		bimg_info;
	bool		has_data;
	char	   *data;
	uint16		data_len;
	uint16		data_bufsz;
} DecodedBkpBlock;
#pragma pack(pop)

struct XLogReaderState
{
	XLogReaderRoutine routine;
	uint64		system_identifier;
	void	   *private_data;
	XLogRecPtr	ReadRecPtr;		/* start of last record read */
	XLogRecPtr	EndRecPtr;		/* end+1 of last record read */
	XLogRecord *decoded_record; /* currently decoded record */

	char	   *main_data;		/* record's main data portion */
	uint32		main_data_len;	/* main data portion's length */
	uint32		main_data_bufsz;	/* allocated size of the buffer */

	RepOriginId record_origin;

	TransactionId toplevel_xid; /* XID of top-level transaction */
	DecodedBkpBlock blocks[XLR_MAX_BLOCK_ID + 1];
	int			max_block_id;	/* highest block_id in use (-1 if none) */
	char	   *readBuf;
	uint32		readLen;

	WALSegmentContext segcxt;
	WALOpenSegment seg;
	uint32		segoff;

	XLogRecPtr	latestPagePtr;
	TimeLineID	latestPageTLI;

	XLogRecPtr	currRecPtr;
	TimeLineID	currTLI;

	XLogRecPtr	currTLIValidUntil;

	TimeLineID	nextTLI;

	char	   *readRecordBuf;
	uint32		readRecordBufSize;

	char	   *errormsg_buf;

	XLogRecPtr	abortedRecPtr;
	XLogRecPtr	missingContrecPtr;
	XLogRecPtr	overwrittenRecPtr;
};
#endif
/* Get a new XLogReader */
extern XLogReaderState *XLogReaderAllocate(int wal_segment_size,
										   const char *waldir,
										   XLogReaderRoutine *routine,
										   void *private_data);
extern XLogReaderRoutine *LocalXLogReaderRoutine(void);

void XLogReaderInvalReadState(XLogReaderState *state);

void ResetDecoder(XLogReaderState *state);

bool ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr,
					  XLogRecPtr PrevRecPtr, XLogRecord *record,
					  bool randAccess);

/* Free an XLogReader */
extern void XLogReaderFree(XLogReaderState *state);

/* Position the XLogReader to given record */
extern void XLogBeginRead(XLogReaderState *state, XLogRecPtr RecPtr);

extern XLogRecPtr XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr);

#if PG_VERSION_NUM > 14
bool
XLogRecGetBlockTagExtended(XLogReaderState *record, uint8 block_id,
						   RelFileNode *rnode, ForkNumber *forknum,
						   BlockNumber *blknum,
						   Buffer *prefetch_buffer);
void
XLogRecGetBlockTag(XLogReaderState *record, uint8 block_id,
				   RelFileNode *rnode, ForkNumber *forknum, BlockNumber *blknum);
#else
bool
XLogRecGetBlockTag(XLogReaderState *record, uint8 block_id,
				   RelFileNode *rnode, ForkNumber *forknum, BlockNumber *blknum);
#endif

/* Read the next XLog record. Returns NULL on end-of-WAL or failure */
extern struct XLogRecord *XLogReadRecord(XLogReaderState *state,
										 char **errormsg);



/* Validate a page */
extern bool XLogReaderValidatePageHeader(XLogReaderState *state,
										 XLogRecPtr recptr, char *phdr);

typedef struct WALReadError
{
	int			wre_errno;		/* errno set by the last pg_pread() */
	int			wre_off;		/* Offset we tried to read from. */
	int			wre_req;		/* Bytes requested to be read. */
	int			wre_read;		/* Bytes read by the last read(). */
	WALOpenSegment wre_seg;		/* Segment we tried to read from. */
} WALReadError;

typedef struct XLogDumpConfig
{
	/* display options */
	bool		quiet;
	bool		bkp_details;
	int			stop_after_records;
	int			already_displayed_records;
	bool		follow;
	bool		stats;
	bool		stats_per_record;

	/* filter options */
	int			filter_by_rmgr;
	TransactionId filter_by_xid;
	bool		filter_by_xid_enabled;
} XLogDumpConfig;


extern bool WALRead(XLogReaderState *state,
					char *buf, XLogRecPtr startptr, Size count,
					TimeLineID tli, WALReadError *errinfo);





#if PG_VERSION_NUM > 14
extern bool DecodeXLogRecord(XLogReaderState *state,
	DecodedXLogRecord *decoded,
	XLogRecord *record,
	XLogRecPtr lsn,
	char **errmsg);
#define XLogRecGetTotalLen(decoder) ((decoder)->record->header.xl_tot_len)
#define XLogRecGetPrev(decoder) ((decoder)->record->header.xl_prev)
#define XLogRecGetInfo(decoder) ((decoder)->record->header.xl_info)
#define XLogRecGetRmid(decoder) ((decoder)->record->header.xl_rmid)
#define XLogRecGetXid(decoder) ((decoder)->record->header.xl_xid)
#define XLogRecGetOrigin(decoder) ((decoder)->record->record_origin)
#define XLogRecGetTopXid(decoder) ((decoder)->record->toplevel_xid)
#define XLogRecGetData(decoder) ((decoder)->record->main_data)
#define XLogRecGetDataLen(decoder) ((decoder)->record->main_data_len)
#define XLogRecHasAnyBlockRefs(decoder) ((decoder)->record->max_block_id >= 0)
#define XLogRecMaxBlockId(decoder) ((decoder)->record->max_block_id)
#define XLogRecGetBlock(decoder, i) (&(decoder)->record->blocks[(i)])
#define XLogRecHasBlockRef(decoder, block_id)			\
	(((decoder)->record->max_block_id >= (block_id)) &&	\
	 ((decoder)->record->blocks[block_id].in_use))
#define XLogRecHasBlockImage(decoder, block_id)		\
	((decoder)->record->blocks[block_id].has_image)
#define XLogRecBlockImageApply(decoder, block_id)		\
	((decoder)->record->blocks[block_id].apply_image)
#else
extern bool DecodeXLogRecord(XLogReaderState *state, XLogRecord *record,
	char **errmsg);							 
#define XLogRecGetTotalLen(decoder) ((decoder)->decoded_record->xl_tot_len)
#define XLogRecGetPrev(decoder) ((decoder)->decoded_record->xl_prev)
#define XLogRecGetInfo(decoder) ((decoder)->decoded_record->xl_info)
#define XLogRecGetRmid(decoder) ((decoder)->decoded_record->xl_rmid)
#define XLogRecGetXid(decoder) ((decoder)->decoded_record->xl_xid)
#define XLogRecGetOrigin(decoder) ((decoder)->record_origin)
#define XLogRecGetTopXid(decoder) ((decoder)->toplevel_xid)
#define XLogRecGetData(decoder) ((decoder)->main_data)
#define XLogRecGetDataLen(decoder) ((decoder)->main_data_len)
#define XLogRecHasAnyBlockRefs(decoder) ((decoder)->max_block_id >= 0)
#define XLogRecMaxBlockId(decoder) ((decoder)->max_block_id)
#define XLogRecGetBlock(decoder, i) (&(decoder)->blocks[(i)])
#define XLogRecHasBlockRef(decoder, block_id) \
	((decoder)->blocks[block_id].in_use)
#define XLogRecHasBlockImage(decoder, block_id) \
	((decoder)->blocks[block_id].has_image)
#define XLogRecBlockImageApply(decoder, block_id) \
	((decoder)->blocks[block_id].apply_image)
#endif

typedef struct FullTransactionId
{
	uint64		value;
} FullTransactionId;


typedef int64 TimestampTz;


typedef struct xl_xact_commit
{
	TimestampTz xact_time;		/* time of commit */

	/* xl_xact_xinfo follows if XLOG_XACT_HAS_INFO */
	/* xl_xact_dbinfo follows if XINFO_HAS_DBINFO */
	/* xl_xact_subxacts follows if XINFO_HAS_SUBXACT */
	/* xl_xact_relfilenodes follows if XINFO_HAS_RELFILENODES */
	/* xl_xact_invals follows if XINFO_HAS_INVALS */
	/* xl_xact_twophase follows if XINFO_HAS_TWOPHASE */
	/* twophase_gid follows if XINFO_HAS_GID. As a null-terminated string. */
	/* xl_xact_origin follows if XINFO_HAS_ORIGIN, stored unaligned! */
} xl_xact_commit;

typedef struct
{
	int8		id;				/* cache ID --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared relation */
	uint32		hashValue;		/* hash value of key for this catcache */
} SharedInvalCatcacheMsg;

#define SHAREDINVALCATALOG_ID	(-1)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared catalog */
	Oid			catId;			/* ID of catalog whose contents are invalid */
} SharedInvalCatalogMsg;

#define SHAREDINVALRELCACHE_ID	(-2)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared relation */
	Oid			relId;			/* relation ID, or 0 if whole relcache */
} SharedInvalRelcacheMsg;

#define SHAREDINVALSMGR_ID		(-3)

typedef struct
{
	/* note: field layout chosen to pack into 16 bytes */
	int8		id;				/* type field --- must be first */
	int8		backend_hi;		/* high bits of backend ID, if temprel */
	uint16		backend_lo;		/* low bits of backend ID, if temprel */
	RelFileNode rnode;			/* spcNode, dbNode, relNode */
} SharedInvalSmgrMsg;

#define SHAREDINVALRELMAP_ID	(-4)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 for shared catalogs */
} SharedInvalRelmapMsg;

#define SHAREDINVALSNAPSHOT_ID	(-5)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared relation */
	Oid			relId;			/* relation ID */
} SharedInvalSnapshotMsg;

typedef union
{
	int8		id;				/* type field --- must be first */
	SharedInvalCatcacheMsg cc;
	SharedInvalCatalogMsg cat;
	SharedInvalRelcacheMsg rc;
	SharedInvalSmgrMsg sm;
	SharedInvalRelmapMsg rm;
	SharedInvalSnapshotMsg sn;
} SharedInvalidationMessage;

#define GIDSIZE 200

typedef struct xl_xact_parsed_commit
{
	TimestampTz xact_time;
	uint32		xinfo;

	Oid			dbId;			/* MyDatabaseId */
	Oid			tsId;			/* MyDatabaseTableSpace */

	int			nsubxacts;
	TransactionId *subxacts;

	int			nrels;
	RelFileNode *xnodes;

	int			nmsgs;
	SharedInvalidationMessage *msgs;

	TransactionId twophase_xid; /* only for 2PC */
	char		twophase_gid[GIDSIZE];	/* only for 2PC */
	int			nabortrels;		/* only for 2PC */
	RelFileNode *abortnodes;	/* only for 2PC */

	XLogRecPtr	origin_lsn;
	TimestampTz origin_timestamp;
} xl_xact_parsed_commit;

typedef struct xl_xact_xinfo
{
	/*
	 * Even though we right now only require 1 byte of space in xinfo we use
	 * four so following records don't have to care about alignment. Commit
	 * records can be large, so copying large portions isn't attractive.
	 */
	uint32		xinfo;
} xl_xact_xinfo;

typedef struct xl_xact_dbinfo
{
	Oid			dbId;			/* MyDatabaseId */
	Oid			tsId;			/* MyDatabaseTableSpace */
} xl_xact_dbinfo;
#define MinSizeOfXactCommit (offsetof(xl_xact_commit, xact_time) + sizeof(TimestampTz))

typedef struct xl_xact_subxacts
{
	int			nsubxacts;		/* number of subtransaction XIDs */
	TransactionId subxacts[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_subxacts;
#define MinSizeOfXactSubxacts offsetof(xl_xact_subxacts, subxacts)

typedef struct xl_xact_relfilenodes
{
	int			nrels;			/* number of relations */
	RelFileNode xnodes[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_relfilenodes;
#define MinSizeOfXactRelfilenodes offsetof(xl_xact_relfilenodes, xnodes)

typedef struct xl_xact_invals
{
	int			nmsgs;			/* number of shared inval msgs */
	SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_invals;
#define MinSizeOfXactInvals offsetof(xl_xact_invals, msgs)

typedef struct xl_xact_twophase
{
	TransactionId xid;
} xl_xact_twophase;

typedef struct xl_xact_origin
{
	XLogRecPtr	origin_lsn;
	TimestampTz origin_timestamp;
} xl_xact_origin;

void xact_desc_pg_del(TimestampTz *TimeFromRecord,Oid *datafileOid, XLogReaderState *record,RmgrId rmid);
void xact_desc_pg_upd(TimestampTz *TimeFromRecord,Oid *datafileOid, XLogReaderState *record,RmgrId rmid);
#endif							/* XLOGREADER_H */


int32 pglz_decompress(const char *source, int32 slen, char *dest,int32 rawsize, bool check_complete);

// RmgrDescData *
// GetRmgrDesc(RmgrId rmid)
bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2);

uintptr_t XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len);

typedef int64 pg_time_t;

pg_time_t timestamptz_to_time_t(TimestampTz t);

uintptr_t timestamptz_to_str(TimestampTz dt);

void XlogRecGetBlkInfo(XLogReaderState *record,int	block_id, BlockNumber *blk, RelFileNode *rnode, ForkNumber	*fork);

bool isUpdate(uint8 info);
