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
#ifndef XLOG_DEFS_H
#define XLOG_DEFS_H

// #define PG_VERSION_NUM 16

#include <fcntl.h>				/* need open() flags */
#include "basic.h"
/*
 * Pointer to a location in the XLOG.  These pointers are 64 bits wide,
 * because we don't want them ever to overflow.
 */
typedef uint64 XLogRecPtr;
typedef uint32 TimeLineID;
#define InvalidBuffer	0
typedef int Buffer;

/*
 * Zero is used indicate an invalid pointer. Bootstrap skips the first possible
 * WAL segment, initializing the first WAL page at WAL segment size, so no XLOG
 * record can begin at zero.
 */
#define InvalidXLogRecPtr	0
#define XLogRecPtrIsInvalid(r)	((r) == InvalidXLogRecPtr)
#define MCXT_ALLOC_HUGE			0x01	/* allow huge allocation (> 1 GB) */
#define MCXT_ALLOC_NO_OOM		0x02	/* no failure if out-of-memory */
#define MCXT_ALLOC_ZERO			0x04	/* zero allocated memory */
#define XLOG_BLCKSZ 8192
#define MAX_ERRORMSG_LEN 1000
#define XLOG_PAGE_MAGIC 0xD10D	/* can be used as WAL version indicator */
#define XLR_INFO_MASK			0x0F

/* compression methods supported */
#define BKPIMAGE_COMPRESS_PGLZ	0x04
#define BKPIMAGE_COMPRESS_LZ4	0x08
#define BKPIMAGE_COMPRESS_ZSTD	0x10

#define	BKPIMAGE_COMPRESSED(info) \
	((info & (BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | \
			  BKPIMAGE_COMPRESS_ZSTD)) != 0)


#define BKPIMAGE_IS_COMPRESSED		0x02	/* page image is compressed */


#define XLOG_XACT_COMMIT			0x00
#define XLOG_XACT_PREPARE			0x10
#define XLOG_XACT_ABORT				0x20
#define XLOG_XACT_COMMIT_PREPARED	0x30
#define XLOG_XACT_ABORT_PREPARED	0x40
#define XLOG_XACT_ASSIGNMENT		0x50
#define XLOG_XACT_INVALIDATIONS		0x60
/* free opcode 0x70 */

/* mask for filtering opcodes out of xl_info */
#define XLOG_XACT_OPMASK			0x70
#define XACT_XINFO_HAS_ORIGIN			(1U << 5)
#define XACT_XINFO_HAS_DBINFO			(1U << 0)
#define XACT_XINFO_HAS_SUBXACTS			(1U << 1)
#define XACT_XINFO_HAS_RELFILENODES		(1U << 2)
#define XACT_XINFO_HAS_INVALS			(1U << 3)
#define XACT_XINFO_HAS_TWOPHASE			(1U << 4)
#define XACT_XINFO_HAS_ORIGIN			(1U << 5)
#define XACT_XINFO_HAS_AE_LOCKS			(1U << 6)
#define XACT_XINFO_HAS_GID				(1U << 7)
#define XLOG_XACT_HAS_INFO			0x80

#define XLOG_SWITCH						0x40
#define SizeOfXLogRecord	(offsetof(XLogRecord, xl_crc) + sizeof(pg_crc32c))
#define XLogPageHeaderSize(hdr)		\
	(((hdr)->xlp_info & XLP_LONG_HEADER) ? SizeOfXLogLongPHD : SizeOfXLogShortPHD)

#define XLogSegmentsPerXLogId(wal_segsz_bytes)	\
	(UINT64CONST(0x100000000) / (wal_segsz_bytes))
#define UINT64CONST(x) (x##UL)


#define XLogSegmentOffset(xlogptr, wal_segsz_bytes)	\
((xlogptr) & ((wal_segsz_bytes) - 1))

#define XLByteInSeg(xlrp, logSegNo, wal_segsz_bytes) \
	(((xlrp) / (wal_segsz_bytes)) == (logSegNo))

#define DEFAULT_DECODE_BUFFER_SIZE (64 * 1024)


typedef enum XLogPageReadResult
{
	XLREAD_SUCCESS = 0,			/* record is successfully read */
	XLREAD_FAIL = -1,			/* failed during reading a record */
	XLREAD_WOULDBLOCK = -2		/* nonblocking mode only, no data */
} XLogPageReadResult;

typedef struct XLogDumpPrivate
{
	TimeLineID	timeline;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
} XLogDumpPrivate;

#define IsXLogFileName(fname) \
	(strlen(fname) == XLOG_FNAME_LEN && \
	 strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN)

typedef union PGAlignedXLogBlock
{
	char		data[XLOG_BLCKSZ];
	double		force_align_d;
	int64		force_align_i64;
} PGAlignedXLogBlock;


typedef struct XLogPageHeaderData
{
	uint16		xlp_magic;		/* magic value for correctness checks 2 bytes */
	uint16		xlp_info;		/* flag bits, see below 2 bytes*/
	TimeLineID	xlp_tli;		/* TimeLineID of first record on page 4 bytes*/
	XLogRecPtr	xlp_pageaddr;	/* XLOG address of this page 8 bytes*/
	uint32		xlp_rem_len;	/* total len of remaining data for record 4 bytes*/
} XLogPageHeaderData;

typedef struct XLogLongPageHeaderData
{
	XLogPageHeaderData std;		/* standard header fields 20 bytes */
	uint64		xlp_sysid;		/* system identifier from pg_control 8 bytes*/
	uint32		xlp_seg_size;	/* just as a cross-check 4 bytes*/
	uint32		xlp_xlog_blcksz;	/* just as a cross-check 4 bytes*/
} XLogLongPageHeaderData;


typedef XLogPageHeaderData *XLogPageHeader;
#define SizeOfXLogShortPHD	MAXALIGN(sizeof(XLogPageHeaderData))
#define SizeOfXLogLongPHD	MAXALIGN(sizeof(XLogLongPageHeaderData))

typedef XLogLongPageHeaderData *XLogLongPageHeader;
/*
 * First LSN to use for "fake" LSNs.
 *
 * Values smaller than this can be used for special per-AM purposes.
 */
#define FirstNormalUnloggedLSN	((XLogRecPtr) 1000)

#define WalSegMinSize 1024 * 1024
#define WalSegMaxSize 1024 * 1024 * 1024
#define IsPowerOf2(x) (x > 0 && ((x) & ((x)-1)) == 0)
#define IsValidWalSegSize(size) \
	 (IsPowerOf2(size) && \
	 ((size) >= WalSegMinSize && (size) <= WalSegMaxSize))

// const char *const forkNames[] = {
// 	"main",						/* MAIN_FORKNUM */
// 	"fsm",						/* FSM_FORKNUM */
// 	"vm",						/* VISIBILITYMAP_FORKNUM */
// 	"init"						/* INIT_FORKNUM */
// };
#define XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes) \
	logSegNo = (xlrp) / (wal_segsz_bytes)
#define BKPBLOCK_FORK_MASK	0x0F
#define BKPBLOCK_FLAG_MASK	0xF0
#define BKPBLOCK_HAS_IMAGE	0x10	/* block data is an XLogRecordBlockImage */
#define BKPBLOCK_HAS_DATA	0x20
#define BKPBLOCK_WILL_INIT	0x40	/* redo will re-init the page */
#define BKPBLOCK_SAME_REL	0x80	/* RelFileNode omitted, same as previous */
#define XLR_MAX_BLOCK_ID			32

#define XLR_BLOCK_ID_DATA_SHORT		255
#define XLR_BLOCK_ID_DATA_LONG		254
#define XLR_BLOCK_ID_ORIGIN			253
#define XLR_BLOCK_ID_TOPLEVEL_XID	252

#define InvalidRepOriginId 0
#define InvalidTransactionId		((TransactionId) 0)
#define TransactionIdIsValid(xid)		((xid) != InvalidTransactionId)

#define XLogFromFileName(fname, tli, logSegNo, wal_segsz_bytes)	\
	do {												\
		uint32 log;										\
		uint32 seg;										\
		sscanf(fname, "%08X%08X%08X", tli, &log, &seg); \
		*logSegNo = (uint64) log * XLogSegmentsPerXLogId(wal_segsz_bytes) + seg; \
	} while (0)

#define XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)	\
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,		\
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId(wal_segsz_bytes)), \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId(wal_segsz_bytes)))

/* When record crosses page boundary, set this flag in new page's header */
#define XLP_FIRST_IS_CONTRECORD		0x0001
/* This flag indicates a "long" page header */
#define XLP_LONG_HEADER				0x0002
/* This flag indicates backup blocks starting in this page are optional */
#define XLP_BKP_REMOVABLE			0x0004
/* Replaces a missing contrecord; see CreateOverwriteContrecordRecord */
#define XLP_FIRST_IS_OVERWRITE_CONTRECORD 0x0008
/* All defined flag bits in xlp_info (used for validity checking of header) */
#define XLP_ALL_FLAGS				0x000F

/* Information stored in bimg_info */
#define BKPIMAGE_HAS_HOLE		0x01	/* page image has "hole" */
#define BKPIMAGE_IS_COMPRESSED		0x02	/* page image is compressed */
#define BKPIMAGE_APPLY		0x04	/* page image should be restored during
									 * replay */
/*
 * Handy macro for printing XLogRecPtr in conventional format, e.g.,
 *
 * printf("%X/%X", LSN_FORMAT_ARGS(lsn));
 */
#define LSN_FORMAT_ARGS(lsn)  (uint32) ((lsn) >> 32), ((uint32) (lsn))

/*
 * XLogSegNo - physical log file sequence number.
 */
typedef uint64 XLogSegNo;

/*
 * TimeLineID (TLI) - identifies different database histories to prevent
 * confusion after restoring a prior state of a database installation.
 * TLI does not change in a normal stop/restart of the database (including
 * crash-and-recover cases); but we must assign a new TLI after doing
 * a recovery to a prior state, a/k/a point-in-time recovery.  This makes
 * the new WAL logfile sequence we generate distinguishable from the
 * sequence that was generated in the previous incarnation.
 */


/*
 * Replication origin id - this is located in this file to avoid having to
 * include origin.h in a bunch of xlog related places.
 */
typedef uint16 RepOriginId;

/*
 *	Because O_DIRECT bypasses the kernel buffers, and because we never
 *	read those buffers except during crash recovery or if wal_level != minimal,
 *	it is a win to use it in all cases where we sync on each write().  We could
 *	allow O_DIRECT with fsync(), but it is unclear if fsync() could process
 *	writes not buffered in the kernel.  Also, O_DIRECT is never enough to force
 *	data to the drives, it merely tries to bypass the kernel cache, so we still
 *	need O_SYNC/O_DSYNC.
 */
#ifdef O_DIRECT
#define PG_O_DIRECT				O_DIRECT
#else
#define PG_O_DIRECT				0
#endif

/*
 * This chunk of hackery attempts to determine which file sync methods
 * are available on the current platform, and to choose an appropriate
 * default method.  We assume that fsync() is always available, and that
 * configure determined whether fdatasync() is.
 */
#if defined(O_SYNC)
#define OPEN_SYNC_FLAG		O_SYNC
#elif defined(O_FSYNC)
#define OPEN_SYNC_FLAG		O_FSYNC
#endif

#if defined(O_DSYNC)
#if defined(OPEN_SYNC_FLAG)
/* O_DSYNC is distinct? */
#if O_DSYNC != OPEN_SYNC_FLAG
#define OPEN_DATASYNC_FLAG		O_DSYNC
#endif
#else							/* !defined(OPEN_SYNC_FLAG) */
/* Win32 only has O_DSYNC */
#define OPEN_DATASYNC_FLAG		O_DSYNC
#endif
#endif

#if defined(PLATFORM_DEFAULT_SYNC_METHOD)
#define DEFAULT_SYNC_METHOD		PLATFORM_DEFAULT_SYNC_METHOD
#elif defined(OPEN_DATASYNC_FLAG)
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_OPEN_DSYNC
#elif defined(HAVE_FDATASYNC)
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_FDATASYNC
#else
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_FSYNC
#endif

#endif							/* XLOG_DEFS_H */

#define LONG_ALIGN_MASK (sizeof(long) - 1)
#define MEMSET_LOOP_LIMIT 1024

#define XLOG_HEAP_INSERT		0x00
#define XLOG_HEAP_DELETE		0x10
#define XLOG_HEAP_UPDATE		0x20
#define XLOG_HEAP_TRUNCATE		0x30
#define XLOG_HEAP_HOT_UPDATE	0x40
#define XLOG_HEAP_CONFIRM		0x50
#define XLOG_HEAP_LOCK			0x60
#define XLOG_HEAP_INPLACE		0x70
#define XLOG_HEAP_INIT_PAGE		0x80


#define XLOG_HEAP2_REWRITE		0x00
#define XLOG_HEAP2_PRUNE		0x10
#define XLOG_HEAP2_VACUUM		0x20
#define XLOG_HEAP2_FREEZE_PAGE	0x30
#define XLOG_HEAP2_VISIBLE		0x40
#define XLOG_HEAP2_MULTI_INSERT 0x50
#define XLOG_HEAP2_LOCK_UPDATED 0x60
#define XLOG_HEAP2_NEW_CID		0x70

/* XLOG info values for XLOG rmgr */
#define XLOG_CHECKPOINT_SHUTDOWN		0x00
#define XLOG_CHECKPOINT_ONLINE			0x10
#define XLOG_NOOP						0x20
#define XLOG_NEXTOID					0x30
#define XLOG_SWITCH						0x40
#define XLOG_BACKUP_END					0x50
#define XLOG_PARAMETER_CHANGE			0x60
#define XLOG_RESTORE_POINT				0x70
#define XLOG_FPW_CHANGE					0x80
#define XLOG_END_OF_RECOVERY			0x90
#define XLOG_FPI_FOR_HINT				0xA0
#define XLOG_FPI						0xB0
/* 0xC0 is used in Postgres 9.5-11 */
#define XLOG_OVERWRITE_CONTRECORD		0xD0


#define XLOG_redo 0
#define TRANSACTION_redo 1
#define HEAP2_redo 9
#define HEAP_redo 10
#define BTREE_redo 11

#define MemSet(start, val, len) \
	do \
	{ \
		/* must be void* because we don't know if it is integer aligned yet */ \
		void   *_vstart = (void *) (start); \
		int		_val = (val); \
		Size	_len = (len); \
\
		if ((((uintptr_t) _vstart) & LONG_ALIGN_MASK) == 0 && \
			(_len & LONG_ALIGN_MASK) == 0 && \
			_val == 0 && \
			_len <= MEMSET_LOOP_LIMIT && \
			/* \
			 *	If MEMSET_LOOP_LIMIT == 0, optimizer should find \
			 *	the whole "if" false at compile time. \
			 */ \
			MEMSET_LOOP_LIMIT != 0) \
		{ \
			long *_start = (long *) _vstart; \
			long *_stop = (long *) ((char *) _start + _len); \
			while (_start < _stop) \
				*_start++ = 0; \
		} \
		else \
			memset(_vstart, _val, _len); \
	} while (0)

#define XLOG_FNAME_LEN	   24


typedef struct xl_heap_delete
{
	TransactionId xmax;			/* xmax of the deleted tuple */
	OffsetNumber offnum;		/* deleted tuple's offset */
	uint8		infobits_set;	/* infomask bits */
	uint8		flags;
} xl_heap_delete;
#define SizeOfHeapDelete	(offsetof(xl_heap_delete, flags) + sizeof(uint8))

#define XLH_DELETE_ALL_VISIBLE_CLEARED			(1<<0)
#define XLH_DELETE_CONTAINS_OLD_TUPLE			(1<<1)
#define XLH_DELETE_CONTAINS_OLD_KEY				(1<<2)
#define XLH_DELETE_IS_SUPER						(1<<3)
#define XLH_DELETE_IS_PARTITION_MOVE			(1<<4)

typedef struct xl_heap_header
{
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		t_hoff;
} xl_heap_header;


/*
 * The vacuum page record is similar to the prune record, but can only mark
 * already dead items as unused
 *
 * Used by heap vacuuming only.  Does not require a super-exclusive lock.
 */
typedef struct xl_heap_vacuum
{
	uint16		nunused;
	/* OFFSET NUMBERS are in the block reference 0 */
} xl_heap_vacuum;


typedef struct xl_heap_clean
{
	TransactionId latestRemovedXid;
	uint16		nredirected;
	uint16		ndead;
	/* OFFSET NUMBERS are in the block reference 0 */
} xl_heap_clean;

/*
 * This is what we need to know about page pruning (both during VACUUM and
 * during opportunistic pruning)
 *
 * The array of OffsetNumbers following the fixed part of the record contains:
 *	* for each redirected item: the item offset, then the offset redirected to
 *	* for each now-dead item: the item offset
 *	* for each now-unused item: the item offset
 * The total number of OffsetNumbers is therefore 2*nredirected+ndead+nunused.
 * Note that nunused is not explicitly stored, but may be found by reference
 * to the total record length.
 *
 * Requires a super-exclusive lock.
 */
#if PG_VERSION_NUM < 17
typedef struct xl_heap_prune
{
	TransactionId latestRemovedXid;
	uint16		nredirected;
	uint16		ndead;
	/* OFFSET NUMBERS are in the block reference 0 */
} xl_heap_prune;
#else
/*
 * Does replaying the record require a cleanup-lock?
 *
 * Pruning, in VACUUM's first pass or when otherwise accessing a page,
 * requires a cleanup lock.  For freezing, and VACUUM's second pass which
 * marks LP_DEAD line pointers as unused without moving any tuple data, an
 * ordinary exclusive lock is sufficient.
 */
#define		XLHP_CLEANUP_LOCK	       (1 << 2)

/*
 * If we remove or freeze any entries that contain xids, we need to include a
 * snapshot conflict horizon.  It's used in Hot Standby mode to ensure that
 * there are no queries running for which the removed tuples are still
 * visible, or which still consider the frozen XIDs as running.
 */
#define		XLHP_HAS_CONFLICT_HORIZON   (1 << 3)

/*
 * Indicates that an xlhp_freeze_plans sub-record and one or more
 * xlhp_freeze_plan sub-records are present.
 */
#define		XLHP_HAS_FREEZE_PLANS		(1 << 4)

/*
 * XLHP_HAS_REDIRECTIONS, XLHP_HAS_DEAD_ITEMS, and XLHP_HAS_NOW_UNUSED_ITEMS
 * indicate that xlhp_prune_items sub-records with redirected, dead, and
 * unused item offsets are present.
 */
#define		XLHP_HAS_REDIRECTIONS		(1 << 5)
#define		XLHP_HAS_DEAD_ITEMS	        (1 << 6)
#define		XLHP_HAS_NOW_UNUSED_ITEMS   (1 << 7)

typedef struct xlhp_prune_items
{
	uint16		ntargets;
	OffsetNumber data[FLEXIBLE_ARRAY_MEMBER];
} xlhp_prune_items;

typedef struct xlhp_freeze_plan
{
	TransactionId xmax;
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		frzflags;

	/* Length of individual page offset numbers array for this plan */
	uint16		ntuples;
} xlhp_freeze_plan;

typedef struct HeapTupleFreeze
{
	/* Fields describing how to process tuple */
	TransactionId xmax;
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		frzflags;

	/* xmin/xmax check flags */
	uint8		checkflags;
	/* Page offset number for tuple */
	OffsetNumber offset;
} HeapTupleFreeze;

typedef struct xlhp_freeze_plans
{
	uint16		nplans;
	xlhp_freeze_plan plans[FLEXIBLE_ARRAY_MEMBER];
} xlhp_freeze_plans;

typedef struct xl_heap_prune
{
	uint8		reason;
	uint8		flags;
} xl_heap_prune;

#define SizeOfHeapPrune (offsetof(xl_heap_prune, flags) + sizeof(uint8))
#endif

typedef struct xl_heap_inplace
{
	OffsetNumber offnum;		/* updated tuple's offset on page */
	/* TUPLE DATA FOLLOWS AT END OF STRUCT */
} xl_heap_inplace;

#define SizeOfHeapInplace	(offsetof(xl_heap_inplace, offnum) + sizeof(OffsetNumber))
/*
 * xl_heap_update flag values, 8 bits are available.
 */
/* PD_ALL_VISIBLE was cleared */
#define XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED		(1<<0)
/* PD_ALL_VISIBLE was cleared in the 2nd page */
#define XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED		(1<<1)
#define XLH_UPDATE_CONTAINS_OLD_TUPLE			(1<<2)
#define XLH_UPDATE_CONTAINS_OLD_KEY				(1<<3)
#define XLH_UPDATE_CONTAINS_NEW_TUPLE			(1<<4)
#define XLH_UPDATE_PREFIX_FROM_OLD				(1<<5)
#define XLH_UPDATE_SUFFIX_FROM_OLD				(1<<6)

/* convenience macro for checking whether any form of old tuple was logged */
#define XLH_UPDATE_CONTAINS_OLD						\
	(XLH_UPDATE_CONTAINS_OLD_TUPLE | XLH_UPDATE_CONTAINS_OLD_KEY)

typedef struct xl_heap_update
{
	TransactionId old_xmax;		/* xmax of the old tuple */
	OffsetNumber old_offnum;	/* old tuple's offset */
	uint8		old_infobits_set;	/* infomask bits to set on old tuple */
	uint8		flags;
	TransactionId new_xmax;		/* xmax of the new tuple */
	OffsetNumber new_offnum;	/* new tuple's offset */

	/*
		* If XLH_UPDATE_CONTAINS_OLD_TUPLE or XLH_UPDATE_CONTAINS_OLD_KEY flags
		* are set, xl_heap_header and tuple data for the old tuple follow.
		*/
} xl_heap_update;

#define SizeOfHeapUpdate	(offsetof(xl_heap_update, new_offnum) + sizeof(OffsetNumber))
#define FirstCommandId	((CommandId) 0)
#define InvalidCommandId	(~(CommandId)0)

typedef enum
{
	BLK_NEEDS_REDO,				/* changes from WAL record need to be applied */
	BLK_DONE,					/* block is already up-to-date */
	BLK_RESTORED,				/* block was restored from a full-page image */
	BLK_NOTFOUND				/* block was not found (and hence does not
								 * need to be replayed) */
} XLogRedoAction;
#define MaxHeapTupleSize  (BLCKSZ - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData)))
#define HEAP_KEYS_UPDATED		0x2000	/* tuple was updated and key cols
										 * modified, or tuple deleted */
#define HEAP_HOT_UPDATED		0x4000	/* tuple was HOT-updated */

#define HEAP_XMAX_LOCK_ONLY		0x0080	/* xmax, if valid, is only a locker */

 /* xmax is a shared locker */
 #define HEAP_XMAX_SHR_LOCK	(HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)
 #define HEAP_LOCK_MASK	(HEAP_XMAX_SHR_LOCK | HEAP_XMAX_EXCL_LOCK | \
						  HEAP_XMAX_KEYSHR_LOCK)
#define HEAP_HASNULL			0x0001	/* has null attribute(s) */
#define HEAP_HASVARWIDTH		0x0002	/* has variable-width attribute(s) */
#define HEAP_HASEXTERNAL		0x0004	/* has external stored attribute(s) */
#define HEAP_HASOID_OLD			0x0008	/* has an object-id field */
#define HEAP_XMAX_KEYSHR_LOCK	0x0010	/* xmax is a key-shared locker */
#define HEAP_COMBOCID			0x0020	/* t_cid is a combo CID */
#define HEAP_XMAX_EXCL_LOCK		0x0040	/* xmax is exclusive locker */
#define HEAP_XMAX_LOCK_ONLY		0x0080	/* xmax, if valid, is only a locker */

 /* xmax is a shared locker */
#define HEAP_XMAX_SHR_LOCK	(HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)
#define HEAP_LOCK_MASK	(HEAP_XMAX_SHR_LOCK | HEAP_XMAX_EXCL_LOCK | \
						 HEAP_XMAX_KEYSHR_LOCK)
#define HEAP_XMIN_COMMITTED		0x0100	/* t_xmin committed */
#define HEAP_XMIN_INVALID		0x0200	/* t_xmin invalid/aborted */
#define HEAP_XMIN_FROZEN		(HEAP_XMIN_COMMITTED|HEAP_XMIN_INVALID)
#define HEAP_XMAX_COMMITTED		0x0400	/* t_xmax committed */
#define HEAP_XMAX_INVALID		0x0800	/* t_xmax invalid/aborted */
#define HEAP_XMAX_IS_MULTI		0x1000	/* t_xmax is a MultiXactId */
#define HEAP_UPDATED			0x2000	/* this is UPDATEd version of row */
#define HEAP_MOVED_OFF			0x4000	/* moved to another place by pre-9.0
										 * VACUUM FULL; kept for binary
										 * upgrade support */
#define HEAP_MOVED_IN			0x8000	/* moved from another place by pre-9.0
										 * VACUUM FULL; kept for binary
										 * upgrade support */
#define HEAP_MOVED (HEAP_MOVED_OFF | HEAP_MOVED_IN)
#define XLHL_XMAX_IS_MULTI		0x01
#define XLHL_XMAX_LOCK_ONLY		0x02
#define XLHL_XMAX_EXCL_LOCK		0x04
#define XLHL_XMAX_KEYSHR_LOCK	0x08
#define XLHL_KEYS_UPDATED		0x10
#define HEAP_XACT_MASK			0xFFF0	/* visibility-related bits */
 
 #define HEAP_XACT_MASK			0xFFF0	/* visibility-related bits */
 /* turn these all off when Xmax is to change */
#define HEAP_XMAX_BITS (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | \
	HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK | HEAP_XMAX_LOCK_ONLY)


#define BlockIdSet(blockId, blockNumber) \
( \
	AssertMacro(PointerIsValid(blockId)), \
	(blockId)->bi_hi = (blockNumber) >> 16, \
	(blockId)->bi_lo = (blockNumber) & 0xffff \
)

#define ItemPointerSet(pointer, blockNumber, offNum) \
( \
	AssertMacro(PointerIsValid(pointer)), \
	BlockIdSet(&((pointer)->ip_blkid), blockNumber), \
	(pointer)->ip_posid = offNum \
)

#define ItemIdIsNormal(itemId) \
	((itemId)->lp_flags == LP_NORMAL)
#define HeapTupleHeaderClearHotUpdated(tup) \
	( \
		(tup)->t_infomask2 &= ~HEAP_HOT_UPDATED \
	)
#define HeapTupleHeaderSetHotUpdated(tup) \
	( \
		(tup)->t_infomask2 |= HEAP_HOT_UPDATED \
	)
#define PageClearAllVisible(page) \
	(((PageHeader) (page))->pd_flags &= ~PD_ALL_VISIBLE)
#define PageXLogRecPtrSet(ptr, lsn) \
	((ptr).xlogid = (uint32) ((lsn) >> 32), (ptr).xrecoff = (uint32) (lsn))


#define PageSetLSN(page, lsn) \
	PageXLogRecPtrSet(((PageHeader) (page))->pd_lsn, lsn)

#define PageAddItem(page, item, size, offsetNumber, overwrite, is_heap) \
	PageAddItemExtended(page, item, size, offsetNumber, \
						((overwrite) ? PAI_OVERWRITE : 0) | \
						((is_heap) ? PAI_IS_HEAP : 0))
/* flags for PageAddItemExtended() */
#define PAI_OVERWRITE			(1 << 0)
#define PAI_IS_HEAP				(1 << 1)
#define FirstOffsetNumber		((OffsetNumber) 1)
#define MaxOffsetNumber			((OffsetNumber) (BLCKSZ / sizeof(ItemIdData)))
#define MaxHeapTuplesPerPage	\
	((int) ((BLCKSZ - SizeOfPageHeaderData) / \
			(MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData))))

#define HEAP_COMBOCID			0x0020	/* t_cid is a combo CID */
#define OffsetNumberIsValid(offsetNumber) \
	((bool) ((offsetNumber != InvalidOffsetNumber) && \
			 (offsetNumber <= MaxOffsetNumber)))


#define HeapTupleHeaderSetCmax(tup, cid, iscombo) \
do { \
	Assert(!((tup)->t_infomask & HEAP_MOVED)); \
	(tup)->t_choice.t_heap.t_field3.t_cid = (cid); \
	if (iscombo) \
		(tup)->t_infomask |= HEAP_COMBOCID; \
	else \
		(tup)->t_infomask &= ~HEAP_COMBOCID; \
} while (0)


#define HeapTupleHeaderSetCmin(tup, cid) \
do { \
	Assert(!((tup)->t_infomask & HEAP_MOVED)); \
	(tup)->t_choice.t_heap.t_field3.t_cid = (cid); \
	(tup)->t_infomask &= ~HEAP_COMBOCID; \
} while (0)

#define VALGRIND_CHECK_MEM_IS_DEFINED(addr, size)			do {} while (0)

#define PageSetPageSizeAndVersion(page, size, version) \
( \
	AssertMacro(((size) & 0xFF00) == (size)), \
	AssertMacro(((version) & 0x00FF) == (version)), \
	((PageHeader) (page))->pd_pagesize_version = (size) | (version) \
)
#define PG_PAGE_LAYOUT_VERSION		4
#define PG_DATA_CHECKSUM_VERSION	1
#define ItemIdIsValid(itemId)	PointerIsValid(itemId)

/*
 * ItemIdIsUsed
 *		True iff item identifier is in use.
 */
#define ItemIdIsUsed(itemId) \
	((itemId)->lp_flags != LP_UNUSED)

/*
 * ItemIdIsNormal
 *		True iff item identifier is in state NORMAL.
 */
#define ItemIdIsNormal(itemId) \
	((itemId)->lp_flags == LP_NORMAL)

/*
 * ItemIdIsRedirected
 *		True iff item identifier is in state REDIRECT.
 */
#define ItemIdIsRedirected(itemId) \
	((itemId)->lp_flags == LP_REDIRECT)

/*
 * ItemIdIsDead
 *		True iff item identifier is in state DEAD.
 */
#define ItemIdIsDead(itemId) \
	((itemId)->lp_flags == LP_DEAD)

/*
 * ItemIdHasStorage
 *		True iff item identifier has associated storage.
 */
#define ItemIdHasStorage(itemId) \
	((itemId)->lp_len != 0)

/*
 * ItemIdSetUnused
 *		Set the item identifier to be UNUSED, with no storage.
 *		Beware of multiple evaluations of itemId!
 */
#define ItemIdSetUnused(itemId) \
( \
	(itemId)->lp_flags = LP_UNUSED, \
	(itemId)->lp_off = 0, \
	(itemId)->lp_len = 0 \
)

/*
 * ItemIdSetNormal
 *		Set the item identifier to be NORMAL, with the specified storage.
 *		Beware of multiple evaluations of itemId!
 */
#define ItemIdSetNormal(itemId, off, len) \
( \
	(itemId)->lp_flags = LP_NORMAL, \
	(itemId)->lp_off = (off), \
	(itemId)->lp_len = (len) \
)

/*
 * ItemIdSetRedirect
 *		Set the item identifier to be REDIRECT, with the specified link.
 *		Beware of multiple evaluations of itemId!
 */
#define ItemIdSetRedirect(itemId, link) \
( \
	(itemId)->lp_flags = LP_REDIRECT, \
	(itemId)->lp_off = (link), \
	(itemId)->lp_len = 0 \
)

/*
 * ItemIdSetDead
 *		Set the item identifier to be DEAD, with no storage.
 *		Beware of multiple evaluations of itemId!
 */
#define ItemIdSetDead(itemId) \
( \
	(itemId)->lp_flags = LP_DEAD, \
	(itemId)->lp_off = 0, \
	(itemId)->lp_len = 0 \
)
typedef signed int Offset;

typedef struct itemIdCompactData
{
	uint16		offsetindex;	/* linp array index */
	int16		itemoff;		/* page offset of item data */
	uint16		alignedlen;		/* MAXALIGN(item data len) */
} itemIdCompactData;
typedef itemIdCompactData *itemIdCompact;

/* This is what we need to know about lock */
typedef struct xl_heap_lock
{
	TransactionId locking_xid;	/* might be a MultiXactId not xid */
	OffsetNumber offnum;		/* locked tuple's offset on page */
	int8		infobits_set;	/* infomask and infomask2 bits to set */
	uint8		flags;			/* XLH_LOCK_* flag bits */
} xl_heap_lock;

typedef struct xl_heap_insert
{
	OffsetNumber offnum;		/* inserted tuple's offset */
	uint8		flags;

	/* xl_heap_header & TUPLE DATA in backup block 0 */
} xl_heap_insert;

#define SizeOfHeapInsert	(offsetof(xl_heap_insert, flags) + sizeof(uint8))

typedef struct xl_heap_multi_insert
{
	uint8		flags;
	uint16		ntuples;
	OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} xl_heap_multi_insert;

typedef struct xl_multi_insert_tuple
{
	uint16		datalen;		/* size of tuple data that follows */
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		t_hoff;
	/* TUPLE DATA FOLLOWS AT END OF STRUCT */
} xl_multi_insert_tuple;

#define SizeOfMultiInsertTuple	(offsetof(xl_multi_insert_tuple, t_hoff) + sizeof(uint8))


#define PageSetPageSizeAndVersion(page, size, version) \
( \
	AssertMacro(((size) & 0xFF00) == (size)), \
	AssertMacro(((version) & 0x00FF) == (version)), \
	((PageHeader) (page))->pd_pagesize_version = (size) | (version) \
)

#define XLH_INSERT_ALL_VISIBLE_CLEARED			(1<<0)
#define XLH_INSERT_LAST_IN_MULTI				(1<<1)
#define XLH_INSERT_IS_SPECULATIVE				(1<<2)
#define XLH_INSERT_CONTAINS_NEW_TUPLE			(1<<3)
#define XLH_INSERT_ON_TOAST_RELATION			(1<<4)
#define XLH_INSERT_ALL_FROZEN_SET				(1<<5)
#define ItemPointerSetBlockNumber(pointer, blockNumber) \
( \
	AssertMacro(PointerIsValid(pointer)), \
	BlockIdSet(&((pointer)->ip_blkid), blockNumber) \
)

/*
 * ItemPointerSetOffsetNumber
 *		Sets a disk item pointer to the specified offset.
 */
#define ItemPointerSetOffsetNumber(pointer, offsetNumber) \
( \
	AssertMacro(PointerIsValid(pointer)), \
	(pointer)->ip_posid = (offsetNumber) \
)

#define PageIsAllVisible(page) \
	(((PageHeader) (page))->pd_flags & PD_ALL_VISIBLE)
#define PageSetAllVisible(page) \
	(((PageHeader) (page))->pd_flags |= PD_ALL_VISIBLE)
#define PageClearAllVisible(page) \
	(((PageHeader) (page))->pd_flags &= ~PD_ALL_VISIBLE)
#define FirstNormalTransactionId	((TransactionId) 3)
#define TransactionIdIsNormal(xid)		((xid) >= FirstNormalTransactionId)

#define MovedPartitionsOffsetNumber 0xfffd
#define MovedPartitionsBlockNumber	InvalidBlockNumber
#define ItemPointerSetMovedPartitions(pointer) \
	ItemPointerSet((pointer), MovedPartitionsBlockNumber, MovedPartitionsOffsetNumber)
#define HeapTupleHeaderSetMovedPartitions(tup) \
	ItemPointerSetMovedPartitions(&(tup)->t_ctid)

#define PageSetPrunable(page, xid) \
do { \
	Assert(TransactionIdIsNormal(xid)); \
	if (!TransactionIdIsValid(((PageHeader) (page))->pd_prune_xid) || \
		TransactionIdPrecedes(xid, ((PageHeader) (page))->pd_prune_xid)) \
		((PageHeader) (page))->pd_prune_xid = (xid); \
} while (0)
#define PageClearPrunable(page) \
	(((PageHeader) (page))->pd_prune_xid = InvalidTransactionId)
