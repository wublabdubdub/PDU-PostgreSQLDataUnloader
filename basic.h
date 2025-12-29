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
 *
 * This header file defines fundamental data structures from PostgreSQL,
 * including page header layouts, tuple header structures, varlena types,
 * TOAST compression formats, and various utility macros. These definitions
 * are essential for correctly interpreting PostgreSQL's on-disk data format.
 *
 * Key PostgreSQL structures defined here:
 * - PageHeaderData, HeapPageHeaderData: Page layout structures
 * - HeapTupleHeaderData: Heap tuple header for row data
 * - ItemIdData, ItemPointerData: Line pointer and tuple identification
 * - varattrib_4b, varattrib_1b, varatt_external: Variable-length data types
 * - toast_compress_header: TOAST compression header
 * - NumericVar: Decimal/numeric data representation
 */
#ifndef BASIC_H
#define BASIC_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <assert.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <lz4.h>

#define PG_VERSION_NUM 15

//#define CN
#define EN

#define NUM_THREADS 1
#define MIN_SIZE_FOR_THREADING (10 * 1024 * 1024)
#define CKEXIST 1


#define PKGTIME "2025-12-11"
#define PDUVERSION "3.0.25.12"

/* Enterprise Edition URL - Update this with your actual URL */
#define PDU_ENTERPRISE_URL "https://pduzc.com"

#define NUM1G 1000
#define MAX_COL_NUM 200
#define MAX_TAB_OBJ 100000

/* DROPSCAN parameters */
#define dropscanDir "restore/dropscan"

#define MAINDEBUG 0
#define DEBUG 0
#define debugStr "d261f45e39244306b7d7ffc305542c51"

#define LSNDSP 0
#define LSNSTR "525/56F9CB08"



#if PG_VERSION_NUM == 18 
#define DB_ATTR "oid,name,oid,int,char,bool,bool,bool,int,xid,xid,oid,pass"
#define SCH_ATTR "oid,name,pass"
#define CLASS_ATTR "oid,name,oid,oid,oid,oid,oid,oid,oid,int,bool,int,int,oid,bool,bool,char,char,smallint,pass"
#define ATTR_ATTR "oid,name,oid,smallint,smallint,int,smallint,bool,char,pass"
#define TYPE_ATTR "oid,name,pass"

#elif PG_VERSION_NUM == 17 
#define DB_ATTR "oid,name,oid,int,char,bool,bool,bool,int,xid,xid,oid,pass"
#define SCH_ATTR "oid,name,pass"
#define CLASS_ATTR "oid,name,oid,oid,oid,oid,oid,oid,oid,int,int,int,oid,bool,bool,char,char,smallint,pass"
#define ATTR_ATTR "oid,name,oid,smallint,smallint,int,int,smallint,bool,char,pass"
#define TYPE_ATTR "oid,name,pass"

#elif PG_VERSION_NUM == 16
#define DB_ATTR "oid,name,oid,int,char,bool,bool,int,xid,xid,oid,pass"
#define SCH_ATTR "oid,name,pass"
#define CLASS_ATTR "oid,name,oid,oid,oid,oid,oid,oid,oid,int,int,int,oid,bool,bool,char,char,smallint,pass"
#define ATTR_ATTR "oid,name,oid,smallint,smallint,int,int,smallint,bool,char,pass"
#define TYPE_ATTR "oid,name,pass"

#elif PG_VERSION_NUM == 15
#define DB_ATTR "oid,name,oid,int,char,bool,bool,int,xid,xid,oid,pass"
#define SCH_ATTR "oid,name,pass"
#define CLASS_ATTR "oid,name,oid,oid,oid,oid,oid,oid,oid,int,int,int,oid,bool,bool,char,char,smallint,pass"
#define ATTR_ATTR "oid,name,oid,int,smallint,smallint,int,int,int,bool,char,char,pass"
#define TYPE_ATTR "oid,name,pass"

#elif PG_VERSION_NUM == 14
#define DB_ATTR "oid,name,oid,int,name,name,bool,bool,int,oid,xid,xid,oid,pass"
#define SCH_ATTR "oid,name,pass"
#define CLASS_ATTR "oid,name,oid,oid,oid,oid,oid,oid,oid,int,int,int,oid,bool,bool,char,char,smallint,pass"
#define ATTR_ATTR "oid,name,oid,int,smallint,smallint,int,int,int,bool,char,char,pass"
#define TYPE_ATTR "oid,name,pass"
#endif

#define BLCKSZ 8192
#define INVALID_CONSCTV_ZERO 20


#define NAMEDATALEN 64

#define XLOGDIR				"pg_wal"
#define PG_BINARY	0
#define	ENOENT		 2	/* No such file or directory */
#define XLOG_FNAME_LEN	   24
#define SUCCESS_RET 1
#define FAILURE_RET 0
#define FAILOPEN_RET -1

#define MATCHED 3
#define DUPLICATED 2
#define CALLBACK 1
#define NOCALLBACK 0

#define CONTINUE_RET 1
#define BREAK_RET 0

#define readItemLog 0
#define xmanDecodeLog 1
#define EXCLUDE_PAGES_IDXFILE "restore/.dsiso/exclu_idxfile"
#define ISOFILE "restore/.dsiso/pgiso"

#define MAINTABINIT 0
#define TOASTTABINIT 1
#define DSREPAIRINIT 2
#define DSTOASTINIT 3
#define DSDICTINIT 4
#define DSISO_MAINTABINIT 5
#define DSISO_TOASTTABINIT 6

typedef unsigned char uint8;	/* == 8 bits */
typedef unsigned short uint16;	/* == 16 bits */
typedef unsigned int uint32;	/* == 32 bits */
typedef unsigned long long int uint64;


typedef signed char int8;		/* == 8 bits */
typedef signed short int16;		/* == 16 bits */
typedef signed int int32;		/* == 32 bits */
typedef signed long long int int64;	/* == 64 bits */


// typedef unsigned long long int uint64;
typedef size_t Size;
typedef uint32 BlockNumber;
typedef int errno_t;



typedef int16 int2;

typedef uint8 bits8;			/* >= 8 bits */
typedef uint16 bits16;			/* >= 16 bits */
typedef uint32 bits32;			/* >= 32 bits */

typedef char *Pointer;
typedef Pointer Page;
typedef uint32 CommandId;
typedef uint16 LocationIndex;
typedef uint32 TransactionId;
typedef uint64 LongTransactionId;
typedef unsigned int Oid;
#define MONTHS_PER_YEAR 12
#define PG_INT32_MIN	(-0x7FFFFFFF-1)
#define PG_INT32_MAX	(0x7FFFFFFF)
#define POSTGRES_EPOCH_JDATE	2451545 /* == date2j(2000, 1, 1) */
#define INT64CONST(x)  (x##LL)
#define PG_INT64_MIN	(-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX	INT64CONST(0x7FFFFFFFFFFFFFFF)
#define DT_NOBEGIN		PG_INT64_MIN
#define DT_NOEND		PG_INT64_MAX
#define USECS_PER_DAY	INT64CONST(86400000000)
#define USECS_PER_HOUR	INT64CONST(3600000000)
#define USECS_PER_MINUTE INT64CONST(60000000)
#define USECS_PER_SEC	INT64CONST(1000000)
#define UNIX_EPOCH_JDATE		2440588 /* == date2j(1970, 1, 1) */
#define SECS_PER_DAY	86400



#define HEAP_HASOID 0x0008
#ifdef __cplusplus
#define InvalidOid (Oid(0))
#else
#define InvalidOid ((Oid)0)
#endif
#define FLEXIBLE_ARRAY_MEMBER	/* empty */

/* Which __func__ symbol do we have, if any? */
#ifdef HAVE_FUNCNAME__FUNC
#define PG_FUNCNAME_MACRO	__func__
#else
#ifdef HAVE_FUNCNAME__FUNCTION
#define PG_FUNCNAME_MACRO	__FUNCTION__
#else
#define PG_FUNCNAME_MACRO	NULL
#endif
#endif

#define Assert(condition)	((void)true)

#define Max(x, y)		((x) > (y) ? (x) : (y))
#define Min(x, y)		((x) < (y) ? (x) : (y))

#define INT64_MODIFIER "ll"
#define INT64_FORMAT "%" INT64_MODIFIER "d"
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define TYPEALIGN_DOWN(ALIGNVAL, LEN) (((uintptr_t)(LEN)) & ~((uintptr_t)((ALIGNVAL)-1)))

#define ALIGNOF_BUFFER	32
#define ALIGNOF_TINY 1
#define ALIGNOF_SHORT 2
#define ALIGNOF_INT 4
#define ALIGNOF_LONG 8
#define ALIGNOF_DOUBLE 8
#define MAXIMUM_ALIGNOF 8
#define TINYALIGN(LEN)			TYPEALIGN(ALIGNOF_TINY, (LEN))
#define SHORTALIGN(LEN)			TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(ALIGNOF_INT, (LEN))
#define LONGALIGN(LEN)			TYPEALIGN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN(LEN)		TYPEALIGN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
/* MAXALIGN covers only built-in types, not buffers */
#define BUFFERALIGN(LEN)		TYPEALIGN(ALIGNOF_BUFFER, (LEN))
#define CACHELINEALIGN(LEN)		TYPEALIGN(PG_CACHE_LINE_SIZE, (LEN))
#define MAXALIGN_DOWN(LEN) TYPEALIGN_DOWN(MAXIMUM_ALIGNOF, (LEN))


#define LP_UNUSED		0		/* unused (should always have lp_len=0) */
#define LP_NORMAL		1		/* used (should always have lp_len>0) */
#define LP_REDIRECT		2		/* HOT redirect (should have lp_len=0) */
#define LP_DEAD			3		/* dead, may or may not have storage */


#define EOF_ENCOUNTERED (-1)
// typedef uint64 XLogRecPtr;
#define PD_HAS_FREE_LINES	0x0001	/* are there any unused line pointers? */
#define PD_PAGE_FULL		0x0002	/* not enough free space for new tuple? */
#define PD_ALL_VISIBLE		0x0004	/* all tuples on page are visible to
									 * everyone */
#define PD_VALID_FLAG_BITS	0x0007	/* OR of all valid pd_flags bits */
#define HEAP_HASNULL			0x0001	/* has null attribute(s) */
#define att_isnull(ATT, BITS) (!((BITS)[(ATT) >> 3] & (1 << ((ATT) & 0x07))))

#define unlikely(x) __builtin_expect((x) != 0, 0)
#define Min(x, y)		((x) < (y) ? (x) : (y))

#define RELMAPPER_FILESIZE    512

/* Relmapper structs */
typedef struct RelMapping
{
  Oid     mapoid;     /* OID of a catalog */
  Oid     mapfilenode;  /* its filenode number */
} RelMapping;


typedef struct RelMapFile
{
  int32   magic;      /* always RELMAPPER_FILEMAGIC */
  int32   num_mappings; /* number of valid RelMapping entries */
  RelMapping  mappings[FLEXIBLE_ARRAY_MEMBER];
} RelMapFile;


typedef struct
{
	uint32		xlogid;			/* high bits */
	uint32		xrecoff;		/* low bits */
} PageXLogRecPtr;


typedef struct ItemIdData
{
	unsigned	lp_off:15,		/* offset to tuple (from start of page) */
				lp_flags:2,		/* state of line pointer, see below */
				lp_len:15;		/* byte length of tuple */
} ItemIdData;
typedef ItemIdData *ItemId;

typedef int64 TimestampTz;

typedef enum ToastCompressionId
{
	TOAST_PGLZ_COMPRESSION_ID = 0,
	TOAST_LZ4_COMPRESSION_ID = 1,
	TOAST_INVALID_COMPRESSION_ID = 2
} ToastCompressionId;


/* PostgreSQL page structure definitions */
/*
 * HeapPageHeaderData -- data that stored at the begin of each new version heap page.
 *		pd_xid_base - base value for transaction IDs on page
 *		pd_multi_base - base value for multixact IDs on page
 *
 */

typedef struct HeapPageHeaderData
{
    /* XXX LSN is member of *any* block, not only page-organized ones */
	PageXLogRecPtr pd_lsn;		/* LSN: next byte after last byte of xlog
								 * record for last change to this page */
	uint16		pd_checksum;	/* checksum */
	uint16		pd_flags;		/* flag bits, see below */
	LocationIndex pd_lower;		/* offset to start of free space */
	LocationIndex pd_upper;		/* offset to end of free space */
	LocationIndex pd_special;	/* offset to start of special space */
	uint16		pd_pagesize_version;
	TransactionId pd_prune_xid; /* oldest prunable XID, or zero if none */
	ItemIdData	pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* line pointer array */
} HeapPageHeaderData;

typedef HeapPageHeaderData* HeapPageHeader;

typedef struct PageHeaderData
{
	/* XXX LSN is member of *any* block, not only page-organized ones */
	PageXLogRecPtr pd_lsn;		/* LSN: next byte after last byte of xlog
								 * record for last change to this page */
	uint16		pd_checksum;	/* checksum */
	uint16		pd_flags;		/* flag bits, see below */
	LocationIndex pd_lower;		/* offset to start of free space */
	LocationIndex pd_upper;		/* offset to end of free space */
	LocationIndex pd_special;	/* offset to start of special space */
	uint16		pd_pagesize_version;
	TransactionId pd_prune_xid; /* oldest prunable XID, or zero if none */
	ItemIdData	pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* line pointer array */
} PageHeaderData;
typedef PageHeaderData *PageHeader;

#define SizeOfPageHeaderData (offsetof(PageHeaderData, pd_linp))

#define PageGetPageSize(page) \
	((Size) (((PageHeader) (page))->pd_pagesize_version & (uint16) 0xFF00))
	
#define PageGetPageLayoutVersion(page) \
	(((PageHeader) (page))->pd_pagesize_version & 0x00FF)

#define PageGetMaxOffsetNumber(page) \
	(((PageHeader) (page))->pd_lower <= SizeOfPageHeaderData ? 0 : \
	 ((((PageHeader) (page))->pd_lower - SizeOfPageHeaderData) \
	  / sizeof(ItemIdData)))
#define OffsetNumberNext(offsetNumber) \
	((OffsetNumber) (1 + (offsetNumber)))
#define OffsetNumberPrev(offsetNumber) \
	  ((OffsetNumber) (-1 + (offsetNumber)))
  
#define PageGetItemId(page, offsetNumber) \
	((ItemId) (&((PageHeader) (page))->pd_linp[(offsetNumber) - 1]))
#define PageHasFreeLinePointers(page) \
	(((PageHeader) (page))->pd_flags & PD_HAS_FREE_LINES)
#define PageClearHasFreeLinePointers(page) \
	(((PageHeader) (page))->pd_flags &= ~PD_HAS_FREE_LINES)
#define PageSetHasFreeLinePointers(page) \
	(((PageHeader) (page))->pd_flags |= PD_HAS_FREE_LINES)
#define PD_HAS_FREE_LINES	0x0001	/* are there any unused line pointers? */
#define PD_PAGE_FULL		0x0002	/* not enough free space for new tuple? */
#define PD_ALL_VISIBLE		0x0004	/* all tuples on page are visible to
										* everyone */

#define PD_VALID_FLAG_BITS	0x0007	/* OR of all valid pd_flags bits */
	

#define AssertMacro(condition)	((void)true)
#define ItemIdHasStorage(itemId) ((itemId)->lp_len != 0)
#define ItemIdIsUsed(itemId) \
	((itemId)->lp_flags != LP_UNUSED)
#define ItemIdSetNormal(itemId, off, len) \
	( \
		(itemId)->lp_flags = LP_NORMAL, \
		(itemId)->lp_off = (off), \
		(itemId)->lp_len = (len) \
	)
#define ItemIdGetOffset(itemId) ((itemId)->lp_off)
typedef Pointer Item;

#define PageGetItem(page, itemId) \
( \
	AssertMacro(PageIsValid(page)), \
	AssertMacro(ItemIdHasStorage(itemId)), \
	(Item)(((char *)(page)) + ItemIdGetOffset(itemId)) \
)

#define PageXLogRecPtrGet(val) \
	((uint64) (val).xlogid << 32 | (val).xrecoff)

#define PageGetLSN(page) \
	PageXLogRecPtrGet(((PageHeader) (page))->pd_lsn)

#define HeapTupleHeaderGetOid(tup) \
    (((tup)->t_infomask & HEAP_HASOID) ? *((Oid*)((char*)(tup) + (tup)->t_hoff - sizeof(Oid))) : InvalidOid)

#define HEAP_XMAX_IS_MULTI 0x1000  /* t_xmax is a MultiXactId */

#define HeapTupleHeaderSetXmin(tup, xid) ((tup)->t_choice.t_heap.t_xmin = (xid))
#define HeapTupleHeaderSetXmax(tup, xid) ((tup)->t_choice.t_heap.t_xmax = (xid))
#define HeapTupleHeaderGetRawXmax(tup) ((tup)->t_choice.t_heap.t_xmax)
#define HeapTupleHeaderGetRawXmin(tup) ((tup)->t_choice.t_heap.t_xmin)

#define SizeOfPageHeaderDataGS (offsetof(HeapPageHeaderData, pd_linp))
#define PageGetPageSizeGS(page) \
	((Size) (((HeapPageHeader) (page))->pd_pagesize_version & (uint16) 0xFF00))
	
#define PageGetPageLayoutVersionGS(page) \
	(((HeapPageHeader) (page))->pd_pagesize_version & 0x00FF)

#define PageGetMaxOffsetNumberGS(page) \
	(((HeapPageHeader) (page))->pd_lower <= SizeOfPageHeaderDataGS ? 0 : \
	 ((((HeapPageHeader) (page))->pd_lower - SizeOfPageHeaderDataGS) \
	  / sizeof(ItemIdData)))

#define PageGetItemIdGS(page, offsetNumber) \
	((ItemId) (&((HeapPageHeader) (page))->pd_linp[(offsetNumber) - 1]))

#define PageGetLSNGS(page) \
	PageXLogRecPtrGet(((HeapPageHeader) (page))->pd_lsn)


#define BlockIdSet(blockId, blockNumber) \
( \
	AssertMacro(PointerIsValid(blockId)), \
	(blockId)->bi_hi = (blockNumber) >> 16, \
	(blockId)->bi_lo = (blockNumber) & 0xffff \
)

#define InvalidOffsetNumber		((OffsetNumber) 0)
#define InvalidBlockNumber		((BlockNumber) 0xFFFFFFFF)
#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)
#define ItemPointerSetInvalid(pointer) \
( \
	AssertMacro(PointerIsValid(pointer)), \
	BlockIdSet(&((pointer)->ip_blkid), InvalidBlockNumber), \
	(pointer)->ip_posid = InvalidOffsetNumber \
)


#define ItemIdGetLength(itemId) \
   ((itemId)->lp_len)

#define ItemIdGetOffset(itemId) \
   ((itemId)->lp_off)

#define ItemIdGetFlags(itemId) \
   ((itemId)->lp_flags)



typedef struct HeapTupleFields
{
	TransactionId t_xmin;		/* inserting xact ID */
	TransactionId t_xmax;		/* deleting or locking xact ID */

	union
	{
		CommandId	t_cid;		/* inserting or deleting command ID, or both */
		TransactionId t_xvac;	/* old-style VACUUM FULL xact ID */
	}			t_field3;
} HeapTupleFields;

typedef struct DatumTupleFields
{
	int32		datum_len_;		/* varlena header (do not touch directly!) */

	int32		datum_typmod;	/* -1, or identifier of a record type */

	Oid			datum_typeid;	/* composite type OID, or RECORDOID */

	/*
	 * datum_typeid cannot be a domain over composite, only plain composite,
	 * even if the datum is meant as a value of a domain-over-composite type.
	 * This is in line with the general principle that CoerceToDomain does not
	 * change the physical representation of the base type value.
	 *
	 * Note: field ordering is chosen with thought that Oid might someday
	 * widen to 64 bits.
	 */
} DatumTupleFields;


typedef struct BlockIdData
{
	uint16		bi_hi;
	uint16		bi_lo;
} BlockIdData;
typedef BlockIdData *BlockId;	/* block identifier */

typedef uint16 OffsetNumber;

typedef struct ItemPointerData
{
	BlockIdData ip_blkid;
	OffsetNumber ip_posid;
}

/* If compiler understands packed and aligned pragmas, use those */
#if defined(pg_attribute_packed) && defined(pg_attribute_aligned)
			pg_attribute_packed()
			pg_attribute_aligned(2)
#endif
ItemPointerData;

typedef ItemPointerData *ItemPointer;

struct HeapTupleHeaderData
{
	union
	{
		HeapTupleFields t_heap;
		DatumTupleFields t_datum;
	}			t_choice;

	ItemPointerData t_ctid;		/* current TID of this or newer tuple (or a
								 * speculative insertion token) */

	/* Fields below here must match MinimalTupleData! */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2 2
	uint16		t_infomask2;	/* number of attributes + various flags */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK 3
	uint16		t_infomask;		/* various flag bits, see below */

#define FIELDNO_HEAPTUPLEHEADERDATA_HOFF 4
	uint8		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

#define FIELDNO_HEAPTUPLEHEADERDATA_BITS 5
	bits8		t_bits[FLEXIBLE_ARRAY_MEMBER];	/* bitmap of NULLs */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
};

typedef struct HeapTupleHeaderData HeapTupleHeaderData;

typedef HeapTupleHeaderData *HeapTupleHeader;

	/* Variable-length data type structures */
#define VARDATA_COMPRESSED_GET_EXTSIZE(PTR) (((varattrib_4b *) (PTR))->va_compressed.va_tcinfo & VARLENA_EXTSIZE_MASK)
#define VARDATA_COMPRESSED_GET_COMPRESS_METHOD(PTR) (((varattrib_4b *) (PTR))->va_compressed.va_tcinfo >> VARLENA_EXTSIZE_BITS)

typedef union
{
	struct						/* Normal varlena (4-byte length) */
	{
		uint32		va_header;
		char		va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
	struct						/* Compressed-in-line format */
	{
		uint32		va_header;
		uint32		va_tcinfo;	/* Original data size (excludes header) and
								 * compression method; see va_extinfo */
		char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
	}			va_compressed;
} varattrib_4b;

typedef struct
{
	uint8		va_header;
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} varattrib_1b;

/* TOAST pointers are a subset of varattrib_1b with an identifying tag byte */
typedef struct
{
	uint8		va_header;		/* Always 0x80 or 0x01 */
	uint8		va_tag;			/* Type of datum */
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} varattrib_1b_e;

typedef struct varatt_lob_external {
    int64 va_rawsize;  /* Original data size (includes header) */
    Oid va_valueid;    /* Unique ID of value within TOAST table */
    Oid va_toastrelid; /* RelID of TOAST table containing it */
} varatt_lob_external;

typedef struct PGLZ_Header {
    int32 vl_len_; /* varlena header (do not touch directly!) */
    int32 rawsize;
} PGLZ_Header;

typedef struct varatt_lob_pointer {
    Oid relid;
    int2 columid;
    int2 bucketid;
    uint16 bi_hi;
    uint16 bi_lo;
    uint16 ip_posid;
} varatt_lob_pointer;

#define VARATT_IS_4B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x00)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x00)
#define VARATT_IS_4B_C(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x02)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x01)
#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)
#define VARATT_IS_COMPRESSED(PTR) VARATT_IS_4B_C(PTR)

/* this test relies on the specific tag values above */

#define VARTAG_IS_EXPANDED(tag) (((tag) & ~1) == VARTAG_EXPANDED_RO)


#define VARTAG_SIZE(tag) \
	((tag) == VARTAG_INDIRECT ? sizeof(varatt_indirect) : \
	 VARTAG_IS_EXPANDED(tag) ? sizeof(varatt_expanded) : \
	 (tag) == VARTAG_ONDISK ? sizeof(varatt_external) : \
	 TrapMacro(true, "unrecognized TOAST vartag"))



#define TrapMacro(condition, errorType) (true)

#define VARHDRSZ_EXTERNAL		offsetof(varattrib_1b_e, va_data)
#define VARHDRSZ_COMPRESSED		offsetof(varattrib_4b, va_compressed.va_data)
#define VARHDRSZ_SHORT			offsetof(varattrib_1b, va_data)

#define VARTAG_EXTERNAL(PTR) VARTAG_1B_E(PTR)
#define VARSIZE_EXTERNAL(PTR) (VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))



typedef struct varatt_external
{
	int32		va_rawsize;		/* Original data size (includes header) */
	uint32		va_extinfo;		/* External saved size (without header) and
								 * compression method */
	Oid			va_valueid;		/* Unique ID of value within TOAST table */
	Oid			va_toastrelid;	/* RelID of TOAST table containing it */
}			varatt_external;

#define TOAST_COMPRESS_HDRSZDatum		((int32) sizeof(toast_compress_headerDatum))
#define TOAST_COMPRESS_RAWSIZEDatum(ptr) (((toast_compress_headerDatum *) (ptr))->rawsize)
#define TOAST_COMPRESS_RAWDATADatum(ptr) \
	(((char *) (ptr)) + TOAST_COMPRESS_HDRSZDatum)
typedef struct toast_compress_headerDatum
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		rawsize;
} toast_compress_headerDatum;

#define TOAST_COMPRESS_HEADER_SIZE (sizeof(uint32))
#define TOAST_COMPRESS_RAWSIZE(ptr) ((*(uint32 *) ptr) & VARLENA_EXTSIZE_MASK)
#define TOAST_COMPRESS_RAWDATA(ptr) (ptr + sizeof(uint32))


#define TOAST_COMPRESS_RAWMETHOD(ptr) ((*(uint32 *) ptr) >> VARLENA_EXTSIZE_BITS)


#define VARLENA_EXTSIZE_BITS	30
#define VARLENA_EXTSIZE_MASK	((1U << VARLENA_EXTSIZE_BITS) - 1)

struct varlena
{
	char		vl_len_[4];		/* Do not touch this field directly! */
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];	/* Data content is here */
};

typedef struct varatt_indirect
{
	struct varlena *pointer;	/* Pointer to in-memory varlena */
}			varatt_indirect;

typedef struct ExpandedObjectHeader ExpandedObjectHeader;

typedef struct varatt_expanded
{
	ExpandedObjectHeader *eohptr;
} varatt_expanded;

typedef enum vartag_external
{
	VARTAG_INDIRECT = 1,
	VARTAG_EXPANDED_RO = 2,
	VARTAG_EXPANDED_RW = 3,
	VARTAG_ONDISK = 18
} vartag_external;


#define TrapMacro(condition, errorType) (true)


/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)


#define VARSIZE_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)
#define VARSIZE(PTR) VARSIZE_4B(PTR)

#define VARDATA_4B(PTR)		(((varattrib_4b *) (PTR))->va_4byte.va_data)
#define VARDATA_4B_C(PTR)	(((varattrib_4b *) (PTR))->va_compressed.va_data)
#define VARDATA_1B(PTR)		(((varattrib_1b *) (PTR))->va_data)
#define VARDATA_1B_E(PTR)	(((varattrib_1b_e *) (PTR))->va_data)
#define VARDATA_EXTERNAL(PTR) VARDATA_1B_E(PTR)
#define VARDATA(PTR) VARDATA_4B(PTR)
#define VARATT_IS_SHORT(PTR)				VARATT_IS_1B(PTR)

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2))
#define SET_VARSIZE_4B_C(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2) | 0x02)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (((uint8) (len)) << 1) | 0x01)
#define SET_VARTAG_1B_E(PTR,tag) \
	(((varattrib_1b_e *) (PTR))->va_header = 0x01, \
	 ((varattrib_1b_e *) (PTR))->va_tag = (tag))
#define VARRAWSIZE_4B_C(PTR) (((varattrib_4b*)(PTR))->va_compressed.va_rawsize)
#define SET_VARSIZE(PTR, len)				SET_VARSIZE_4B(PTR, len)


#define VARATT_IS_EXTENDED(PTR) (!VARATT_IS_4B_U(PTR))
#define VARATT_IS_EXTERNAL(PTR) VARATT_IS_1B_E(PTR)
#define VARATT_IS_EXTERNAL_ONDISK(PTR) (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_ONDISK)
#define VARATT_IS_EXTERNAL_BUCKET(PTR) (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_BUCKET)
#define VARATT_IS_COMPRESSED(PTR)			VARATT_IS_4B_C(PTR)
#define VARATT_IS_EXTERNAL(PTR)				VARATT_IS_1B_E(PTR)
#define VARATT_IS_EXTERNAL_ONDISK(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_ONDISK)
#define VARATT_IS_EXTERNAL_INDIRECT(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_INDIRECT)
#define VARATT_IS_EXTERNAL_EXPANDED_RO(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_EXPANDED_RO)
#define VARATT_IS_EXTERNAL_EXPANDED_RW(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_EXPANDED_RW)
#define VARATT_IS_EXTERNAL_EXPANDED(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(PTR)))
#define VARATT_IS_EXTERNAL_NON_EXPANDED(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && !VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(PTR)))
#define VARSIZE_ANY(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : \
	  VARSIZE_4B(PTR)))
#define VARHDRSZ ((int32)sizeof(int32))
#define MaximumBytesPerTuple(tuplesPerPage) MAXALIGN_DOWN((BLCKSZ - MAXALIGN(SizeOfPageHeaderData + (tuplesPerPage) * sizeof(ItemIdData))) / (tuplesPerPage))

#define EXTERN_TUPLES_PER_PAGE 4 /* tweak only this */
#define EXTERN_TUPLE_MAX_SIZE MaximumBytesPerTuple(EXTERN_TUPLES_PER_PAGE)
#define MAXPGPATH 1024
#define TOAST_MAX_CHUNK_SIZE (EXTERN_TUPLE_MAX_SIZE - MAXALIGN(offsetof(HeapTupleHeaderData, t_bits)) - sizeof(Oid) - sizeof(int32) - VARHDRSZ)
#define VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr) do { \
   varattrib_1b_e *attre = (varattrib_1b_e *) (attr); \
   memcpy(&(toast_pointer), VARDATA_EXTERNAL(attre), sizeof(toast_pointer)); \
} while (0)


#define VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) \
	((toast_pointer).va_extinfo & VARLENA_EXTSIZE_MASK)
#define VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) \
	(VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) < \
	(toast_pointer).va_rawsize - VARHDRSZ)

typedef struct {
    int ndim;         /* # of dimensions */
    int32 dataoffset; /* offset to data, or 0 if no bitmap */
    Oid elemtype;     /* element type OID */
} ArrayType;

#define ARRY_GET_FOURBYTE(arr) (*(const unsigned int*)(arr) & 0xFFFFFFFF)


/*checksim computation*/
/* number of checksums to calculate in parallel */
#define N_SUMS 32
/* prime multiplier of FNV-1a hash */
#define FNV_PRIME 16777619

/* Use a union so that this code is valid under strict aliasing */
typedef union
{
	PageHeaderData phdr;
	uint32		data[BLCKSZ / (sizeof(uint32) * N_SUMS)][N_SUMS];
} PGChecksummablePage;

/*
 * Base offsets to initialize each of the parallel FNV hashes into a
 * different initial state.
 */
static const uint32 checksumBaseOffsets[N_SUMS] = {
	0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A,
	0x79FF467A, 0x9BB9F8A3, 0x217E7CD2, 0x83E13D2C,
	0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA,
	0x7B093B5D, 0x98DAFF3C, 0xF718902A, 0x0B1C9CDB,
	0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE,
	0x92BEC979, 0xCCA6C0B2, 0x304A0979, 0x85AA43D4,
	0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E,
	0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756
};

/*
 * Calculate one round of the checksum.
 */
#define CHECKSUM_COMP(checksum, value) \
do { \
	uint32 __tmp = (checksum) ^ (value); \
	(checksum) = __tmp * FNV_PRIME ^ (__tmp >> 17); \
} while (0)

/*
 * Block checksum algorithm.  The page must be adequately aligned
 * (at least on 4-byte boundary).
 */
static uint32
pg_checksum_block(const PGChecksummablePage *page)
{
	uint32		sums[N_SUMS];
	uint32		result = 0;
	uint32		i,
				j;

	/* ensure that the size is compatible with the algorithm */
	Assert(sizeof(PGChecksummablePage) == BLCKSZ);

	/* initialize partial checksums to their corresponding offsets */
	memcpy(sums, checksumBaseOffsets, sizeof(checksumBaseOffsets));

	/* main checksum calculation */
	for (i = 0; i < (uint32) (BLCKSZ / (sizeof(uint32) * N_SUMS)); i++)
		for (j = 0; j < N_SUMS; j++)
			CHECKSUM_COMP(sums[j], page->data[i][j]);

	/* finally add in two rounds of zeroes for additional mixing */
	for (i = 0; i < 2; i++)
		for (j = 0; j < N_SUMS; j++)
			CHECKSUM_COMP(sums[j], 0);

	/* xor fold partial checksums together */
	for (i = 0; i < N_SUMS; i++)
		result ^= sums[i];

	return result;
}
#define PageIsNew(page) (((PageHeader) (page))->pd_upper == 0)

typedef struct HeapTupleData
{
	uint32		t_len;			/* length of *t_data */
	ItemPointerData t_self;		/* SelfItemPointer */
	Oid			t_tableOid;		/* table the tuple came from */
#define FIELDNO_HEAPTUPLEDATA_DATA 3
	HeapTupleHeader t_data;		/* -> tuple header and data */
} HeapTupleData;

typedef struct slist_node slist_node;
struct slist_node
{
	slist_node *next;
};

 typedef struct ReorderBufferTupleBuf
{
	/* position in preallocated list */
	slist_node	node;

	/* tuple header, the interesting bit for users of logical decoding */
	HeapTupleData tuple;

	/* pre-allocated size of tuple buffer, different from tuple size */
	Size		alloc_tuple_size;

	/* actual tuple data follows */
} ReorderBufferTupleBuf;

#define SizeOfHeapHeader	(offsetof(xl_heap_header, t_hoff) + sizeof(uint8))
#define SizeofHeapTupleHeader offsetof(HeapTupleHeaderData, t_bits)

#define DatumGetCString(X) ((char *) DatumGetPointer(X))
#define DatumGetPointer(X) ((Pointer) (X))
typedef uintptr_t Datum;
#define PointerGetDatum(X) ((Datum) (X))
typedef struct toast_compress_header
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		tcinfo;			/* 2 bits for compression method and 30 bits
								 * external size; see va_extinfo */
} toast_compress_header;
#define TOAST_COMPRESS_EXTSIZE(ptr) \
	(((toast_compress_header *) (ptr))->tcinfo & VARLENA_EXTSIZE_MASK)
#define TOAST_COMPRESS_METHOD(ptr) \
	(((toast_compress_header *) (ptr))->tcinfo >> VARLENA_EXTSIZE_BITS)

#define SET_VARSIZE_COMPRESSED(PTR, len)	SET_VARSIZE_4B_C(PTR, len)
#define VARSIZE_SHORT(PTR)					VARSIZE_1B(PTR)
#define VARDATA_SHORT(PTR)					VARDATA_1B(PTR)


#define NUMERIC_DIGITS(num) (NUMERIC_HEADER_IS_SHORT(num) ? \
	(num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_NDIGITS(num) \
	((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))

#define NUMERIC_NDIGITS_JSONB(num) \
	((VARSIZE(num) - NUMERIC_HEADER_SIZE_JSONB(num)) / sizeof(NumericDigit))

#define PG_RETURN_CSTRING(x) return CStringGetDatum(x)
#define CStringGetDatum(X) PointerGetDatum(X)

#define  TYPALIGN_CHAR			'c' /* char alignment (i.e. unaligned) */
#define  TYPALIGN_SHORT			's' /* short alignment (typically 2 bytes) */
#define  TYPALIGN_INT			'i' /* int alignment (typically 4 bytes) */
#define  TYPALIGN_DOUBLE		'd' /* double alignment (often 8 bytes) */

/*
 * att_align_nominal aligns the given offset as needed for a datum of alignment
 * requirement attalign, ignoring any consideration of packed varlena datums.
 * There are three main use cases for using this macro directly:
 *	* we know that the att in question is not varlena (attlen != -1);
 *	  in this case it is cheaper than the above macros and just as good.
 *	* we need to estimate alignment padding cost abstractly, ie without
 *	  reference to a real tuple.  We must assume the worst case that
 *	  all varlenas are aligned.
 *	* within arrays, we unconditionally align varlenas (XXX this should be
 *	  revisited, probably).
 *
 * The attalign cases are tested in what is hopefully something like their
 * frequency of occurrence.
 */
#define att_align_nominal(cur_offset, attalign) \
( \
	((attalign) == 'i') ? INTALIGN(cur_offset) : \
	 (((attalign) == 'c') ? (uintptr_t) (cur_offset) : \
	  (((attalign) == 'd') ? DOUBLEALIGN(cur_offset) : \
	   ( \
			AssertMacro((attalign) == 's'), \
			SHORTALIGN(cur_offset) \
	   ))) \
)


#define att_align_pointer(cur_offset, attalign, attlen, attptr) \
( \
	((attlen) == -1 && VARATT_NOT_PAD_BYTE(attptr)) ? \
	(uintptr_t) (cur_offset) : \
	att_align_nominal(cur_offset, attalign) \
)
#define HEAP_NATTS_MASK			0x07FF	/* 11 bits for number of attributes */

#define HeapTupleHeaderGetNatts(tup) \
	((tup)->t_infomask2 & HEAP_NATTS_MASK)


#define att_addlength_pointer(cur_offset, attlen, attptr) \
( \
	((attlen) > 0) ? \
	( \
		(cur_offset) + (attlen) \
	) \
	: (((attlen) == -1) ? \
	( \
		(cur_offset) + VARSIZE_ANY(attptr) \
	) \
	: \
	( \
		AssertMacro((attlen) == -2), \
		(cur_offset) + (strlen((char *) (attptr)) + 1) \
	)) \
)


#define HeapTupleHeaderGetRawCommandId(tup) \
( \
	(tup)->t_choice.t_heap.t_field3.t_cid \
)



typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		bit_len;		/* number of valid bits */
	bits8		bit_dat[FLEXIBLE_ARRAY_MEMBER]; /* bit string, most sig. byte
												 * first */
} VarBit;
#define BITMASK 0xFF
#define BITS_PER_BYTE		8
/* Header overhead *in addition to* VARHDRSZ */
#define VARBITHDRSZ			sizeof(int32)
/* Number of bits in this bit string */
#define VARBITLEN(PTR)		(((VarBit *) (PTR))->bit_len)
/* Pointer to the first byte containing bit string data */
#define VARBITS(PTR)		(((VarBit *) (PTR))->bit_dat)
/* Number of bytes in the data section of a bit string */
#define VARBITBYTES(PTR)	(VARSIZE(PTR) - VARHDRSZ - VARBITHDRSZ)
/* Padding of the bit string at the end (in bits) */
#define VARBITPAD(PTR)		(VARBITBYTES(PTR)*BITS_PER_BYTE - VARBITLEN(PTR))
/* Number of bytes needed to store a bit string of a given length */
#define VARBITTOTALLEN(BITLEN)	(((BITLEN) + BITS_PER_BYTE-1)/BITS_PER_BYTE + \
								 VARHDRSZ + VARBITHDRSZ)


#define VARBIT_CORRECTLY_PADDED(vb) \
	do { \
		int32	pad_ = VARBITPAD(vb); \
		Assert(pad_ >= 0 && pad_ < BITS_PER_BYTE); \
		Assert(pad_ == 0 || \
			   (*(VARBITS(vb) + VARBITBYTES(vb) - 1) & ~(BITMASK << pad_)) == 0); \
	} while (0)

#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

#define HeapTupleHeaderGetXmin(tup) \
( \
	HeapTupleHeaderXminFrozen(tup) ? \
		FrozenTransactionId : HeapTupleHeaderGetRawXmin(tup) \
)

#define HeapTupleHeaderXminCommitted(tup) \
( \
	((tup)->t_infomask & HEAP_XMIN_COMMITTED) != 0 \
)

#define HeapTupleHeaderXminInvalid(tup) \
( \
	((tup)->t_infomask & (HEAP_XMIN_COMMITTED|HEAP_XMIN_INVALID)) == \
		HEAP_XMIN_INVALID \
)

typedef struct varlena bytea;


typedef enum
{
	BYTEA_OUTPUT_ESCAPE,
	BYTEA_OUTPUT_HEX
}			ByteaOutputType;


#define VARSIZE_ANY_EXHDR(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR)-VARHDRSZ_EXTERNAL : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR)-VARHDRSZ_SHORT : \
	  VARSIZE_4B(PTR)-VARHDRSZ))

#define VARDATA_ANY(PTR) \
	 (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))
#endif // BASIC_H
