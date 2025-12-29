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
#define _GNU_SOURCE
#include "decode.h"
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>
#include <errno.h>

// Forward declarations to fix implicit declaration warnings
void PageInit(Page page, Size pageSize, Size specialSize);
void fix_infomask_from_infobits(uint8 infobits, uint16 *infomask, uint16 *infomask2);
void PageTruncateLinePointerArray(Page page);
bool RestoreBlockImage(XLogReaderState *record, uint8 block_id, char *page);
void xact_desc_commit(TimestampTz *TimeFromRecord, Oid *datafileOid, Oid *toastOid, xl_xact_commit *xlrec, uint8 info);
OffsetNumber PageAddItemExtended(Page page, Item item, Size size, OffsetNumber offsetNumber, int flags);
int XLogRecordRedoDropFPW(systemDropContext *sdc, XLogReaderState *record);
void mergeTxDelElems(parray *TxTime_parray);

char lsn[30];
int FPWLoc = 0;

int	WalSegSz;
char* datadir;
int delOrDrop;
char* start_fname_pg = NULL;
char* end_fname_pg = NULL;
TimestampTz *earliestTimeLocal_pg = NULL;

char targetDatafile[50];
char targetOldDatafile[50];
char targetToastfile[50];
char targetOldToastfile[50];

char currWalName[70];
int FPIcount=0;
int FPIErrcount=0;
int FPIUpdateSame=0;

int restoreMode_there = periodRestore;
int ExportMode_there = CSVform;
int resTyp_there = DELETEtyp;
int timeMode;

pg_attributeDesc *allDesc=NULL;
DELstruct *elemforTime = NULL;
int isToastRound;
int dropExist2;

systemDropContext *sdcPgClass = NULL;
systemDropContext *sdcPgAttr = NULL;

parray *Txs = NULL;
harray *delElems = NULL;

/**
 * setRestoreMode_there - Set restore mode
 *
 * @setting: Restore mode value (periodRestore, etc.)
 */
void setRestoreMode_there(int setting){
	restoreMode_there = setting;
}

/**
 * setExportMode_there - Set export mode
 *
 * @setting: Export mode value (CSVform, etc.)
 */
void setExportMode_there(int setting){
	ExportMode_there = setting;
}

/**
 * setResTyp_there - Set restore type
 *
 * @setting: Restore type value (DELETEtyp, etc.)
 */
void setResTyp_there(int setting){
	resTyp_there = setting;
}
char FPWSegmentPath[100];

parray *LsnBlkInfos = NULL;

#include <pthread.h>

/* Index entry structure: stores block number and corresponding offset */
typedef struct {
    BlockNumber blk;
    off_t offset;
} IndexEntry;

/* Sub hash table node (stores index info for a single block) */
typedef struct SubHashNode {
    IndexEntry entry;
    struct SubHashNode *next;
} SubHashNode;

/* Sub hash table (corresponds to all block indexes for a filenode) */
typedef struct {
    SubHashNode **buckets;
    int bucket_count;
    int entry_count;
    float load_factor;
    pthread_mutex_t lock;
} SubHashTable;

/* Global hash table node (maps filenode to sub hash table) */
typedef struct GlobalHashNode {
    RelFileNumber filenode;
    SubHashTable *sub_table;
    struct GlobalHashNode *next;
} GlobalHashNode;

/* Global hash table (manages all filenode sub hash tables) */
typedef struct {
    GlobalHashNode **buckets;
    int bucket_count;
    pthread_mutex_t lock;
} GlobalHashTable;

/* Global hash table instance */
static GlobalHashTable *global_hash_table = NULL;
static pthread_once_t init_once = PTHREAD_ONCE_INIT;

/**
 * hash_uint - Hash function for unsigned integers
 *
 * @value:        Value to hash
 * @bucket_count: Number of hash buckets
 *
 * Returns: Hash bucket index
 */
static unsigned int hash_uint(unsigned int value, int bucket_count) {
    value ^= (value << 13);
    value ^= (value >> 17);
    value ^= (value << 5);
    return value % bucket_count;
}

/**
 * sub_hash_init - Initialize a sub hash table
 *
 * @initial_buckets: Initial number of buckets
 * @load_factor:     Load factor threshold for expansion
 *
 * Returns: Pointer to new sub hash table, or NULL on failure
 */
static SubHashTable* sub_hash_init(int initial_buckets, float load_factor) {
    SubHashTable *table = (SubHashTable*)malloc(sizeof(SubHashTable));
    if (!table) return NULL;

    table->bucket_count = (initial_buckets > 0) ? initial_buckets : 16;
    table->entry_count = 0;
    table->load_factor = (load_factor > 0) ? load_factor : 0.75f;
    table->buckets = (SubHashNode**)calloc(table->bucket_count, sizeof(SubHashNode*));

    if (!table->buckets) {
        free(table);
        return NULL;
    }

    pthread_mutex_init(&table->lock, NULL);
    return table;
}

/**
 * sub_hash_expand - Expand sub hash table capacity
 *
 * @table: Sub hash table to expand
 *
 * Doubles the bucket count and rehashes all entries.
 */
static void sub_hash_expand(SubHashTable *table) {
    if (!table) return;

    int new_buckets = table->bucket_count * 2;
    SubHashNode **new_table = (SubHashNode**)calloc(new_buckets, sizeof(SubHashNode*));
    if (!new_table) return;

    /* Rehash all entries to new buckets */
    for (int i = 0; i < table->bucket_count; i++) {
        SubHashNode *node = table->buckets[i];
        while (node) {
            SubHashNode *next = node->next;
            unsigned int idx = hash_uint(node->entry.blk, new_buckets);

            node->next = new_table[idx];
            new_table[idx] = node;
            node = next;
        }
    }

    free(table->buckets);
    table->buckets = new_table;
    table->bucket_count = new_buckets;
}

/**
 * sub_hash_insert - Insert or update entry in sub hash table
 *
 * @table: Sub hash table
 * @entry: Index entry to insert
 *
 * Inserts a new entry or updates existing one with same block number.
 */
static void sub_hash_insert(SubHashTable *table, const IndexEntry *entry) {
    if (!table || !entry) return;

    /* Check if expansion needed */
    if ((float)table->entry_count / table->bucket_count >= table->load_factor) {
        sub_hash_expand(table);
    }

    unsigned int idx = hash_uint(entry->blk, table->bucket_count);
    SubHashNode *current = table->buckets[idx];

    /* Find and update existing entry */
    while (current) {
        if (current->entry.blk == entry->blk) {
            current->entry.offset = entry->offset;
            return;
        }
        current = current->next;
    }

    /* Insert new entry */
    SubHashNode *new_node = (SubHashNode*)malloc(sizeof(SubHashNode));
    if (new_node) {
        new_node->entry = *entry;
        new_node->next = table->buckets[idx];
        table->buckets[idx] = new_node;
        table->entry_count++;
    }
}

/**
 * sub_hash_lookup - Look up entry in sub hash table
 *
 * @table: Sub hash table
 * @blk:   Block number to find
 *
 * Returns: Pointer to index entry, or NULL if not found
 */
static int sub_hash_find(SubHashTable *table, BlockNumber blk, off_t *offset) {
    if (!table || !offset) return 0;

    unsigned int idx = hash_uint(blk, table->bucket_count);
    SubHashNode *current = table->buckets[idx];

    while (current) {
        if (current->entry.blk == blk) {
            *offset = current->entry.offset;
            return 1;
        }
        current = current->next;
    }
    return 0;
}

/**
 * sub_hash_destroy - Free sub hash table memory
 *
 * @table: Sub hash table to destroy
 */
static void sub_hash_destroy(SubHashTable *table) {
    if (!table->buckets){
		free(table);
		return;
	}

    for (int i = 0; i < table->bucket_count; i++) {
        SubHashNode *node = table->buckets[i];
        while (node) {
            SubHashNode *temp = node;
            node = node->next;
            free(temp);
        }
    }
    free(table->buckets);
    pthread_mutex_destroy(&table->lock);
    free(table);
}

/**
 * global_hash_init - Initialize global hash table
 *
 * Creates the global hash table for mapping filenodes to sub tables.
 */
static void global_hash_init() {
    global_hash_table = (GlobalHashTable*)malloc(sizeof(GlobalHashTable));
    if (global_hash_table) {
        global_hash_table->bucket_count = 256;
        global_hash_table->buckets = (GlobalHashNode**)calloc(
            global_hash_table->bucket_count, sizeof(GlobalHashNode*));
        pthread_mutex_init(&global_hash_table->lock, NULL);
    }
}

/**
 * get_global_hash_table - Get global hash table singleton (thread-safe)
 *
 * Returns: Pointer to global hash table
 */
static GlobalHashTable* get_global_hash_table() {
    pthread_once(&init_once, global_hash_init);
    return global_hash_table;
}

/**
 * get_sub_hash_table - Find or create sub hash table for filenode
 *
 * @filenode: File node number
 *
 * Returns: Pointer to sub hash table for this filenode
 */
static SubHashTable* get_sub_hash_table(RelFileNumber filenode) {
    GlobalHashTable *global = get_global_hash_table();
    if (!global) return NULL;

    unsigned int idx = hash_uint(filenode, global->bucket_count);
    SubHashTable *sub_table = NULL;

    pthread_mutex_lock(&global->lock);

    /* Find existing sub hash table */
    GlobalHashNode *current = global->buckets[idx];
    while (current) {
        if (current->filenode == filenode) {
            sub_table = current->sub_table;
            break;
        }
        current = current->next;
    }

    /* Create new sub hash table if not exists */
    if (!sub_table) {
        sub_table = sub_hash_init(16, 0.75f);
        if (sub_table) {
            GlobalHashNode *new_node = (GlobalHashNode*)malloc(sizeof(GlobalHashNode));
            if (new_node) {
                new_node->filenode = filenode;
                new_node->sub_table = sub_table;
                new_node->next = global->buckets[idx];
                global->buckets[idx] = new_node;
            } else {
                sub_hash_destroy(sub_table);
                sub_table = NULL;
            }
        }
    }

    pthread_mutex_unlock(&global->lock);
    return sub_table;
}

/* Sync sub hash table to disk index file */
static void sync_sub_hash_to_disk(RelFileNumber filenode, SubHashTable *sub_table) {
    if (!sub_table || sub_table->entry_count == 0) return;

    char idx_path[MAXPGPATH];
    sprintf(idx_path, "%s/%u.idx", FPWSegmentPath, filenode);
    FILE *fp = fopen(idx_path, "wb");
    if (!fp) return;

    /* Collect all index entries */
    IndexEntry *entries = (IndexEntry*)malloc(sub_table->entry_count * sizeof(IndexEntry));
    if (entries) {
        int pos = 0;
        pthread_mutex_lock(&sub_table->lock);

        for (int i = 0; i < sub_table->bucket_count; i++) {
            SubHashNode *node = sub_table->buckets[i];
            while (node) {
                entries[pos++] = node->entry;
                node = node->next;
            }
        }

        pthread_mutex_unlock(&sub_table->lock);

        /* Write to index file */
        fwrite(entries, sizeof(IndexEntry), sub_table->entry_count, fp);
        free(entries);
    }
    fclose(fp);
}

/* Load from disk index file to sub hash table */
static void load_sub_hash_from_disk(RelFileNumber filenode, SubHashTable *sub_table) {
    if (!sub_table) return;

    char idx_path[MAXPGPATH];
    sprintf(idx_path, "%s/%u.idx", FPWSegmentPath, filenode);
    FILE *fp = fopen(idx_path, "rb");
    if (!fp) return;

    /* Get file size to calculate entry count */
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    int entry_count = file_size / sizeof(IndexEntry);
    if (entry_count <= 0) {
        fclose(fp);
        return;
    }

    /* Read all index entries */
    IndexEntry entry;
    pthread_mutex_lock(&sub_table->lock);
    for (int i = 0; i < entry_count; i++) {
        if (fread(&entry, sizeof(IndexEntry), 1, fp) == 1) {
            sub_hash_insert(sub_table, &entry);
        }
    }
    pthread_mutex_unlock(&sub_table->lock);

    fclose(fp);
}

/**
 * FPW2File - Write Full Page Write data to file
 *
 * @blk:      Block number
 * @page:     Page data to write
 * @filenode: File node identifier
 *
 * Writes a full page to the FPW storage file, using hash table
 * indexing for efficient block lookup.
 */
void FPW2File(BlockNumber blk, char* page, RelFileNumber filenode) {
    char data_path[MAXPGPATH];
    FILE *fp = NULL;
    SubHashTable *sub_table = NULL;
    IndexEntry entry = {.blk = blk};
    off_t offset = 0;
    int exists = 0;

    sub_table = get_sub_hash_table(filenode);
    if (!sub_table) return;

    pthread_mutex_lock(&sub_table->lock);
    if (sub_table->entry_count == 0) {
        pthread_mutex_unlock(&sub_table->lock);
        load_sub_hash_from_disk(filenode, sub_table);
        pthread_mutex_lock(&sub_table->lock);
    }

    exists = sub_hash_find(sub_table, blk, &offset);
    pthread_mutex_unlock(&sub_table->lock);

    sprintf(data_path, "%s/%u", FPWSegmentPath, filenode);

    if (exists) {

        fp = fopen(data_path, "r+b");
        if (!fp) {
            printf("Cannot open data file %s for writing\n", data_path);
            return;
        }
        fseek(fp, offset, SEEK_SET);
    } else {

        fp = fopen(data_path, "a+b");
        if (!fp) {
            printf("Cannot open data file %s for appending\n", data_path);
            return;
        }
        fseek(fp, 0, SEEK_END);
        offset = ftell(fp);
        entry.offset = offset;

        pthread_mutex_lock(&sub_table->lock);
        sub_hash_insert(sub_table, &entry);
        pthread_mutex_unlock(&sub_table->lock);

        sync_sub_hash_to_disk(filenode, sub_table);
    }

    if (fwrite(page, 1, BLCKSZ, fp) != BLCKSZ) {
        printf("Failed to write block %u to file %s\n", blk, data_path);
    }
    fclose(fp);
}

/**
 * FPWfromFile - Read Full Page Write data from file
 *
 * @blk:      Block number to read
 * @page:     Buffer to store page data
 * @filenode: File node identifier
 *
 * Reads a full page from FPW storage file using hash table lookup.
 *
 * Returns: 1 on success, 0 if block not found
 */
int FPWfromFile(BlockNumber blk, char* page, RelFileNumber filenode) {
    char data_path[MAXPGPATH];
    FILE *fp = NULL;
    SubHashTable *sub_table = NULL;
    off_t offset;
    int result = 0;

    Assert(page != NULL);

    sub_table = get_sub_hash_table(filenode);
    if (!sub_table) return 0;

    pthread_mutex_lock(&sub_table->lock);
    if (sub_table->entry_count == 0) {
        pthread_mutex_unlock(&sub_table->lock);
        load_sub_hash_from_disk(filenode, sub_table);
        pthread_mutex_lock(&sub_table->lock);
    }

    result = sub_hash_find(sub_table, blk, &offset);
    pthread_mutex_unlock(&sub_table->lock);

    if (!result){
		LsnBlkInfo *elem = (LsnBlkInfo*)malloc(sizeof(LsnBlkInfo));
		strcpy(elem->LSN,lsn);
		elem->blk = blk;
		parray_append(LsnBlkInfos,elem);

		return 0;
	}

    sprintf(data_path, "%s/%u", FPWSegmentPath, filenode);
    fp = fopen(data_path, "rb");
    if (!fp) {

        return 0;
    }

    if (fseek(fp, offset, SEEK_SET) == 0 &&
        fread(page, 1, BLCKSZ, fp) == BLCKSZ) {
        result = 1;
    } else {

        result = 0;
    }

    fclose(fp);
    return result;
}

/**
 * FPWfileExist - Check if FPW data exists for a block
 *
 * @blk:      Block number to check
 * @filenode: File node identifier
 *
 * Returns: true if block exists in FPW storage, false otherwise
 */
bool FPWfileExist(BlockNumber blk, RelFileNumber filenode) {
    SubHashTable *sub_table = get_sub_hash_table(filenode);
    if (!sub_table) return false;

    pthread_mutex_lock(&sub_table->lock);
    if (sub_table->entry_count == 0) {
        pthread_mutex_unlock(&sub_table->lock);
        load_sub_hash_from_disk(filenode, sub_table);
        pthread_mutex_lock(&sub_table->lock);
    }

    off_t dummy;
    bool exists = sub_hash_find(sub_table, blk, &dummy) ? true : false;
    pthread_mutex_unlock(&sub_table->lock);

    return exists;
}

/**
 * FPWHashCleanup - Clean up FPW hash tables on exit
 *
 * Syncs all data to disk and frees all hash table memory.
 */
void FPWHashCleanup() {
    GlobalHashTable *global = get_global_hash_table();
    if (!global) return;

    pthread_mutex_lock(&global->lock);
    for (int i = 0; i < global->bucket_count; i++) {
        GlobalHashNode *node = global->buckets[i];
        while (node) {
            GlobalHashNode *temp = node;

            /* Sync final state to disk */
            sync_sub_hash_to_disk(node->filenode, node->sub_table);

            /* Destroy sub hash table */
            sub_hash_destroy(node->sub_table);

            node = node->next;
            free(temp);
        }
        global->buckets[i] = NULL;
    }
    pthread_mutex_unlock(&global->lock);

    free(global->buckets);
    pthread_mutex_destroy(&global->lock);
    free(global);
    global_hash_table = NULL;
	init_once = PTHREAD_ONCE_INIT;
}

/**
 * determineTimeMode - Determine time filtering mode
 *
 * @SrtTime: Start timestamp
 * @EndTime: End timestamp
 *
 * Sets the global timeMode based on which time bounds are specified.
 */
void determineTimeMode(TimestampTz *SrtTime,TimestampTz *EndTime){
	if(*SrtTime == 0 && *EndTime == 0){
		timeMode = None;
	}
	else if(*SrtTime != 0 && *EndTime == 0){
		timeMode = FormmerHalf;
	}
	else if(*SrtTime == 0 && *EndTime != 0){
		timeMode = LatterHalf;
	}
	else if(*SrtTime != 0 && *EndTime != 0){
		timeMode = FULL;
	}
}

/**
 * pg_malloc_internal - Internal memory allocation with flags
 *
 * @size:  Number of bytes to allocate
 * @flags: Allocation flags (MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO)
 *
 * Returns: Allocated memory pointer, or NULL on failure
 */
static inline void *
pg_malloc_internal(size_t size, int flags)
{
	void	   *tmp;

	/* Avoid unportable behavior of malloc(0) */
	if (size == 0)
		size = 1;
	tmp = malloc(size);
	if (tmp == NULL)
	{
		if ((flags & MCXT_ALLOC_NO_OOM) == 0)
		{
			printf("out of memory\n");
			exit(1);
		}
		return NULL;
	}

	if ((flags & MCXT_ALLOC_ZERO) != 0)
		memset(tmp, 0, size);
	return tmp;
}

/**
 * palloc_extended - Allocate memory with extended options
 *
 * @size:  Number of bytes to allocate
 * @flags: Allocation flags
 *
 * Returns: Allocated memory pointer
 */
void *
palloc_extended(Size size, int flags)
{
	return pg_malloc_internal(size, flags);
}

/**
 * WALOpenSegmentInit - Initialize WAL segment structures
 *
 * @seg:     WAL open segment to initialize
 * @segcxt:  WAL segment context
 * @segsize: WAL segment size
 * @waldir:  WAL directory path
 */
void
WALOpenSegmentInit(WALOpenSegment *seg, WALSegmentContext *segcxt,
				   int segsize, const char *waldir)
{
	seg->ws_file = -1;
	seg->ws_segno = 0;
	seg->ws_tli = 0;

	segcxt->ws_segsize = segsize;
	if (waldir)
		snprintf(segcxt->ws_dir, MAXPGPATH, "%s", waldir);
}

/**
 * allocate_recordbuf - Allocate buffer for XLog record
 *
 * @state:     XLog reader state
 * @reclength: Required record length
 *
 * Returns: true on success, false on allocation failure
 */
bool
allocate_recordbuf(XLogReaderState *state, uint32 reclength)
{
	uint32		newSize = reclength;

	newSize += XLOG_BLCKSZ - (newSize % XLOG_BLCKSZ);
	newSize = Max(newSize, 5 * Max(BLCKSZ, XLOG_BLCKSZ));

	if (state->readRecordBuf)
		free(state->readRecordBuf);
	state->readRecordBuf =
		(char *) palloc_extended(newSize, MCXT_ALLOC_NO_OOM);
	if (state->readRecordBuf == NULL)
	{
		state->readRecordBufSize = 0;
		return false;
	}
	state->readRecordBufSize = newSize;
	return true;
}

/**
 * XLogReaderAllocate - Allocate and initialize XLog reader state
 *
 * @wal_segment_size: WAL segment size
 * @waldir:           WAL directory path
 * @routine:          Reader callback routines
 * @private_data:     Private data for callbacks
 *
 * Returns: Allocated XLogReaderState, or NULL on failure
 */
XLogReaderState *
XLogReaderAllocate(int wal_segment_size, const char *waldir,
				   XLogReaderRoutine *routine, void *private_data)
{
	XLogReaderState *state;

	state = (XLogReaderState *)
		palloc_extended(sizeof(XLogReaderState),
						MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	if (!state)
		return NULL;

	/* initialize caller-provided support functions */
	state->routine = *routine;

#if PG_VERSION_NUM == 14
	state->max_block_id = -1;
#endif
	/*
	 * Permanently allocate readBuf.  We do it this way, rather than just
	 * making a static array, for two reasons: (1) no need to waste the
	 * storage in most instantiations of the backend; (2) a static char array
	 * isn't guaranteed to have any particular alignment, whereas
	 * palloc_extended() will provide MAXALIGN'd storage.
	 */
	state->readBuf = (char *) palloc_extended(XLOG_BLCKSZ,
											  MCXT_ALLOC_NO_OOM);
	if (!state->readBuf)
	{
		free(state);
		return NULL;
	}

	/* Initialize segment info. */
	WALOpenSegmentInit(&state->seg, &state->segcxt, wal_segment_size,
					   waldir);

	/* system_identifier initialized to zeroes above */
	state->private_data = private_data;
	/* ReadRecPtr, EndRecPtr and readLen initialized to zeroes above */
	state->errormsg_buf = palloc_extended(MAX_ERRORMSG_LEN + 1,
										  MCXT_ALLOC_NO_OOM);
	if (!state->errormsg_buf)
	{
		free(state->readBuf);
		free(state);
		return NULL;
	}
	state->errormsg_buf[0] = '\0';

	/*
	 * Allocate an initial readRecordBuf of minimal size, which can later be
	 * enlarged if necessary.
	 */
	if (!allocate_recordbuf(state, 0))
	{
		free(state->errormsg_buf);
		free(state->readBuf);
		free(state);
		return NULL;
	}

	return state;
}

/**
 * open_file_in_directory - Open a file in specified directory
 *
 * @directory: Directory path
 * @fname:     File name to open
 *
 * Returns: File descriptor on success, -1 on failure
 */
int
open_file_in_directory(const char *directory, const char *fname)
{
	int			fd = -1;
	char		fpath[MAXPGPATH];

	Assert(directory != NULL);

	snprintf(fpath, MAXPGPATH, "%s/%s", directory, fname);
	fd = open(fpath, O_RDONLY | PG_BINARY, 0);

	if (fd < 0){
		#ifdef CN
		printf("%sWAL日志 %s\"%s\"%s 未找到,跳过%s\n",COLOR_WARNING,COLOR_UNLOAD, fname,COLOR_WARNING,C_RESET);
		#else
		printf("%sWAL File %s\"%s\"%s Not Found, Skipped%s\n",COLOR_WARNING,COLOR_UNLOAD, fname,COLOR_WARNING,C_RESET);
		#endif
	}
	return fd;
}

/**
 * search_directory - Search for WAL files in directory
 *
 * @directory: Directory to search
 * @fname:     Optional specific file name to find
 *
 * Returns: true if WAL file found and opened, false otherwise
 */
bool search_directory(const char *directory, const char *fname)
{
	int			fd = -1;
	DIR		   *xldir;

	/* open file if valid filename is provided */
	if (fname != NULL)
		fd = open_file_in_directory(directory, fname);

	/*
	 * A valid file name is not passed, so search the complete directory.  If
	 * we find any file whose name is a valid WAL file name then try to open
	 * it.  If we cannot open it, bail out.
	 */
	else if ((xldir = opendir(directory)) != NULL)
	{
		struct dirent *xlde;

		while ((xlde = readdir(xldir)) != NULL)
		{
			if (IsXLogFileName(xlde->d_name))
			{
				fd = open_file_in_directory(directory, xlde->d_name);
				fname = xlde->d_name;
				break;
			}
		}

		closedir(xldir);
	}

	/* set WalSegSz if file is successfully opened */
	if (fd >= 0)
	{
		PGAlignedXLogBlock buf;
		int			r;

		r = read(fd, buf.data, XLOG_BLCKSZ);
		if (r == XLOG_BLCKSZ)
		{
			XLogLongPageHeader longhdr = (XLogLongPageHeader) buf.data;

			WalSegSz = longhdr->xlp_seg_size;

			if (!IsValidWalSegSize(WalSegSz))
				printf("WAL segment size must be a power of two between 1 MB and 1 GB, but the WAL file \"%s\" header specifies %d byte",
									 "WAL segment size must be a power of two between 1 MB and 1 GB, but the WAL file \"%s\" header specifies %d bytes",
									 WalSegSz);
		}
		else
		{
				printf("could not read file \"%s\": read %d of %zu\n",
							fname, r, (Size) XLOG_BLCKSZ);
		}
		close(fd);
		return true;
	}

	return false;
}

/**
 * pg_strdup - Duplicate a string with error checking
 *
 * @in: Source string to duplicate
 *
 * Returns: Newly allocated copy of string
 */
char *pg_strdup(const char *in)
{
	char	   *tmp;

	if (!in)
	{
		printf("cannot duplicate null pointer (internal error)\n");
		exit(1);
	}
	tmp = strdup(in);
	if (!tmp)
	{
		printf("out of memory\n");
		exit(1);
	}
	return tmp;
}

/**
 * identify_target_directory - Find directory containing WAL files
 *
 * @directory: Starting directory to search
 * @fname:     Optional specific WAL file name
 *
 * Returns: Allocated string with directory path, or NULL if not found
 */
char *identify_target_directory(char *directory, char *fname)
{
	char		fpath[MAXPGPATH];

	if (directory != NULL)
	{
		if (search_directory(directory, fname))
			return pg_strdup(directory);

		/* directory / XLOGDIR */
		snprintf(fpath, MAXPGPATH, "%s/%s", directory, XLOGDIR);
		if (search_directory(fpath, fname))
			return pg_strdup(fpath);
	}
	else
	{
		const char *datadir;

		/* current directory */
		if (search_directory(".", fname))
			return pg_strdup(".");
		/* XLOGDIR */
		if (search_directory(XLOGDIR, fname))
			return pg_strdup(XLOGDIR);

		datadir = getenv("PGDATA");
		/* $PGDATA / XLOGDIR */
		if (datadir != NULL)
		{
			snprintf(fpath, MAXPGPATH, "%s/%s", datadir, XLOGDIR);
			if (search_directory(fpath, fname))
				return pg_strdup(fpath);
		}
	}

	/* could not locate WAL file */
	if (fname)
		printf("could not locate WAL file \"%s\"", fname);
	else
		printf("could not find any WAL file");

	return NULL;				/* not reached */
}

/**
 * XLogDumpRecordLen - Calculate XLog record lengths
 *
 * @record:  XLog reader state with record data
 * @rec_len: Output for record length
 * @fpi_len: Output for full page image length
 */
void XLogDumpRecordLen(XLogReaderState *record, uint32 *rec_len, uint32 *fpi_len)
{
	int			block_id;

	/*
	 * Calculate the amount of FPI data in the record.
	 *
	 * XXX: We peek into xlogreader's private decoded backup blocks for the
	 * bimg_len indicating the length of FPI data. It doesn't seem worth it to
	 * add an accessor macro for this.
	 */
	*fpi_len = 0;
	for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
	{
		if (XLogRecHasBlockImage(record, block_id))
			*fpi_len += XLogRecGetBlock(record, block_id)->bimg_len;
	}

	/*
	 * Calculate the length of the record as the total length - the length of
	 * all the block images.
	 */
	*rec_len = XLogRecGetTotalLen(record) - *fpi_len;
}

/**
 * XlogGiveMeTime - Extract and filter transaction time from XLog record
 *
 * @record:  XLog reader state
 * @SrtTime: Start time for filtering
 * @EndTime: End time for filtering
 *
 * Returns: 1 to continue, 0 to skip record, -1 to exit processing
 */
int XlogGiveMeTime(XLogReaderState *record,TimestampTz *SrtTime,TimestampTz *EndTime){
	RmgrId rmid = XLogRecGetRmid(record);
	if(rmid == TRANSACTION_redo){
		char	   *rec = XLogRecGetData(record);
		uint8		info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;
		if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
		{
			xl_xact_commit *xlrec = (xl_xact_commit *) rec;

			if(timeMode == None){ /* None mode: no start/end defined, use WAL log times */
				if(*SrtTime == 0 && *EndTime == 0){
					*SrtTime = xlrec->xact_time;
					*EndTime = xlrec->xact_time;
					return 1;
				}
				else if(xlrec->xact_time < *SrtTime){
					*SrtTime = xlrec->xact_time;
					return 1;
				}
				else if(xlrec->xact_time > *EndTime){
					*EndTime = xlrec->xact_time;
					return 1;
				}
			}
			else if(timeMode == FormmerHalf){ /* Former mode: start time is defined */
				if(*EndTime == 0){
					*EndTime = xlrec->xact_time;
					return 1;
				}
				else if(xlrec->xact_time < *SrtTime){ /* Time < start time, skip this record */
					return 0;
				}
				else if(xlrec->xact_time > *EndTime){ /* Time > end time, update end time */
					*EndTime = xlrec->xact_time;
					return 1;
				}
			}
			else if(timeMode == LatterHalf){ /* Latter mode: end time is defined */
				if(*SrtTime == 0){
					*SrtTime = xlrec->xact_time;
					return 1;
				}
				else if(xlrec->xact_time > *EndTime){ /* Time > end time, return -1 to exit */
					return -1;
				}
				else if(xlrec->xact_time < *SrtTime){ /* Time < start time, update start time */
					*SrtTime = xlrec->xact_time;
					return 1;
				}
			}
			else if(timeMode == FULL){
				if(xlrec->xact_time < *SrtTime){ /* Time < start time, skip this record */
					return 0;
				}
				else if(xlrec->xact_time > *EndTime){ /* Time > end time, return -1 to exit */
					return -1;
				}
			}
		}
	}
	return 1;
}

/**
 * heap_xlog_delete - Replay heap delete from XLog record
 *
 * @record:   XLog reader state with delete record
 * @blk:      Block number
 * @filenode: File node identifier
 */
void heap_xlog_delete(XLogReaderState *record,BlockNumber blk,RelFileNumber filenode)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_delete *xlrec = (xl_heap_delete *) XLogRecGetData(record);
	char	page[BLCKSZ]={0};
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	BlockNumber blkno;
	RelFileNode target_node;
	ItemPointerData target_tid;

	XLogRecGetBlockTag(record, 0, &target_node, NULL, &blkno);
	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */

	if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
	{
		PageInit(page, BLCKSZ, 0);
	}
	else{
		if(!FPWfromFile(blk,page,filenode)){
			return;
		}
	}

	if (PageGetMaxOffsetNumber(page) >= xlrec->offnum)
		lp = PageGetItemId(page, xlrec->offnum);

	int a = PageGetMaxOffsetNumber(page);
	if (PageGetMaxOffsetNumber(page) < xlrec->offnum || !ItemIdIsNormal(lp)){
		warningInvalidLp();;
		return;
	}

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	HeapTupleHeaderClearHotUpdated(htup);
	fix_infomask_from_infobits(xlrec->infobits_set,
								&htup->t_infomask, &htup->t_infomask2);
	if (!(xlrec->flags & XLH_DELETE_IS_SUPER))
		HeapTupleHeaderSetXmax(htup, xlrec->xmax);
	else
		HeapTupleHeaderSetXmin(htup, InvalidTransactionId);
	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

	/* Mark the page as a candidate for pruning */
	PageSetPrunable(page, XLogRecGetXid(record));

	if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
		PageClearAllVisible(page);

	/* Make sure t_ctid is set correctly */
	if (xlrec->flags & XLH_DELETE_IS_PARTITION_MOVE)
		HeapTupleHeaderSetMovedPartitions(htup);
	else
		htup->t_ctid = target_tid;

	FPW2File(blk,page,filenode);

}

/**
 * heap_xlog_update - Replay heap update from XLog record
 *
 * @record:     XLog reader state with update record
 * @hot_update: True if this is a HOT update
 * @blk:        Block number
 * @filenode:   File node identifier
 */
void heap_xlog_update(XLogReaderState *record, bool hot_update,BlockNumber blk,RelFileNumber filenode)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_update *xlrec = (xl_heap_update *) XLogRecGetData(record);
	RelFileNode rnode;
	BlockNumber oldblk;
	BlockNumber newblk;
	ItemPointerData newtid;
	Buffer		obuffer,
				nbuffer;
	char	oldpage[BLCKSZ]={0};
	char	newpage[BLCKSZ]={0};
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleData oldtup;
	HeapTupleHeader htup;
	uint16		prefixlen = 0,
				suffixlen = 0;
	char	   *newp;
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	xl_heap_header xlhdr;
	uint32		newlen;
	Size		freespace = 0;
	XLogRedoAction oldaction;
	XLogRedoAction newaction;

	/* initialize to keep the compiler quiet */
	oldtup.t_data = NULL;
	oldtup.t_len = 0;

	XLogRecGetBlockTag(record, 0, &rnode, NULL, &newblk);
	#if PG_VERSION_NUM > 14
	if (XLogRecGetBlockTagExtended(record, 1, NULL, NULL, &oldblk, NULL))
	#else
	if (XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblk))
	#endif
	{
		/* HOT updates are never done across pages */
		Assert(!hot_update);
	}
	else
		oldblk = newblk;

	ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */

	/*
	 * In normal operation, it is important to lock the two pages in
	 * page-number order, to avoid possible deadlocks against other update
	 * operations going the other way.  However, during WAL replay there can
	 * be no other update happening, so we don't need to worry about that. But
	 * we *do* need to worry that we don't expose an inconsistent state to Hot
	 * Standby queries --- so the original page can't be unlocked before we've
	 * added the new tuple to the new page.
	 */

	/* Deal with old tuple version */

	/***************** Process old data block *********************/
	if(!FPWfromFile(oldblk,oldpage,filenode)){
		return;
	}
	offnum = xlrec->old_offnum;
	OffsetNumber newofftest = xlrec->new_offnum;
	if (PageGetMaxOffsetNumber(oldpage) >= offnum)
		lp = PageGetItemId(oldpage, offnum);

	int a = PageGetMaxOffsetNumber(oldpage);
	if (PageGetMaxOffsetNumber(oldpage) < offnum || !ItemIdIsNormal(lp))
	{
		warningInvalidLp();;
		return;
	}
	htup = (HeapTupleHeader) PageGetItem(oldpage, lp);

	oldtup.t_data = htup;
	oldtup.t_len = ItemIdGetLength(lp);

	htup->t_infomask &= ~(HEAP_MOVED);
	htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	if (hot_update)
		HeapTupleHeaderSetHotUpdated(htup);
	else
		HeapTupleHeaderClearHotUpdated(htup);
	fix_infomask_from_infobits(xlrec->old_infobits_set, &htup->t_infomask,
								&htup->t_infomask2);
	HeapTupleHeaderSetXmax(htup, xlrec->old_xmax);
	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
	/* Set forward chain link in t_ctid */
	htup->t_ctid = newtid;

	/* Mark the page as a candidate for pruning */

	if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
		PageClearAllVisible(oldpage);

	PageSetLSN(oldpage, lsn);

	/*
	 * Read the page the new tuple goes into, if different from old.
	 */
	/***************** End old data block processing *********************/

	/***************** Process new data block *********************/
	if (oldblk == newblk){ /* If old and new blocks are the same, copy old to new */
		for (int i = 0; i < BLCKSZ; i++) {
			newpage[i] = oldpage[i];
		}
	}
	else if(XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE){ /* If new block needs init, init it and write old to file */
		PageInit(newpage, BLCKSZ, 0);
		FPW2File(oldblk,oldpage,filenode);
	}
	else{ /* If old and new differ, write old to file and read new from file */
		FPW2File(oldblk,oldpage,filenode);
		if( !FPWfromFile(newblk,newpage,filenode)){
			return;
		}
	}

	/* Deal with new tuple */
	char	   *recdata;
	char	   *recdata_end;
	Size		datalen;
	Size		tuplen;

	recdata = (char *)XLogRecGetBlockData(record, 0, &datalen);
	if(recdata == NULL){
		recdata = (char *)XLogRecGetBlockData(record, 1, &datalen);
	}
	recdata_end = recdata + datalen;

	offnum = xlrec->new_offnum;
	int b = PageGetMaxOffsetNumber(newpage);
	if (PageGetMaxOffsetNumber(newpage) + 1 < offnum){
		printf("invalid max offset number\n");
		return;
	}

	if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD)
	{
		Assert(newblk == oldblk);
		memcpy(&prefixlen, recdata, sizeof(uint16));
		recdata += sizeof(uint16);
	}
	if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
	{
		Assert(newblk == oldblk);
		memcpy(&suffixlen, recdata, sizeof(uint16));
		recdata += sizeof(uint16);
	}

	memcpy((char *) &xlhdr, recdata, SizeOfHeapHeader);
	recdata += SizeOfHeapHeader;

	tuplen = recdata_end - recdata;
	Assert(tuplen <= MaxHeapTupleSize);

	htup = &tbuf.hdr;
	MemSet((char *) htup, 0, SizeofHeapTupleHeader);

	/*
		* Reconstruct the new tuple using the prefix and/or suffix from the
		* old tuple, and the data stored in the WAL record.
		*/
	newp = (char *) htup + SizeofHeapTupleHeader;
	if (prefixlen > 0)
	{
		int			len;

		/* copy bitmap [+ padding] [+ oid] from WAL record */
		len = xlhdr.t_hoff - SizeofHeapTupleHeader;
		memcpy(newp, recdata, len);
		recdata += len;
		newp += len;

		/* copy prefix from old tuple */
		memcpy(newp, (char *) oldtup.t_data + oldtup.t_data->t_hoff, prefixlen);
		newp += prefixlen;

		/* copy new tuple data from WAL record */
		len = tuplen - (xlhdr.t_hoff - SizeofHeapTupleHeader);
		memcpy(newp, recdata, len);
		recdata += len;
		newp += len;
	}
	else
	{
		/*
			* copy bitmap [+ padding] [+ oid] + data from record, all in one
			* go
			*/
		memcpy(newp, recdata, tuplen);
		recdata += tuplen;
		newp += tuplen;
	}
	Assert(recdata == recdata_end);

	/* copy suffix from old tuple */
	if (suffixlen > 0)
		memcpy(newp, (char *) oldtup.t_data + oldtup.t_len - suffixlen, suffixlen);

	newlen = SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;

	HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
	/* Make sure there is no forward chain link in t_ctid */
	htup->t_ctid = newtid;

	offnum = PageAddItem(newpage, (Item) htup, newlen, offnum, true, true);
	if (offnum == InvalidOffsetNumber){
		return;
	}

	PageSetLSN(newpage, lsn);
	FPW2File(newblk,newpage,filenode);
	/***************** End new data block processing *********************/
}

int computeDL(){
	return 10*((123 * 7) + (456 / 2) - (99 +102) + 9112);
}
void compactify_tuples(itemIdCompact itemidbase, int nitems, Page page, bool presorted)
{
	PageHeader	phdr = (PageHeader) page;
	Offset		upper;
	Offset		copy_tail;
	Offset		copy_head;
	itemIdCompact itemidptr;
	int			i;

	/* Code within will not work correctly if nitems == 0 */
	Assert(nitems > 0);

	if (presorted)
	{

#ifdef USE_ASSERT_CHECKING
		{
			/*
			 * Verify we've not gotten any new callers that are incorrectly
			 * passing a true presorted value.
			 */
			Offset		lastoff = phdr->pd_special;

			for (i = 0; i < nitems; i++)
			{
				itemidptr = &itemidbase[i];

				Assert(lastoff > itemidptr->itemoff);

				lastoff = itemidptr->itemoff;
			}
		}
#endif							/* USE_ASSERT_CHECKING */

		/*
		 * 'itemidbase' is already in the optimal order, i.e, lower item
		 * pointers have a higher offset.  This allows us to memmove() the
		 * tuples up to the end of the page without having to worry about
		 * overwriting other tuples that have not been moved yet.
		 *
		 * There's a good chance that there are tuples already right at the
		 * end of the page that we can simply skip over because they're
		 * already in the correct location within the page.  We'll do that
		 * first...
		 */
		upper = phdr->pd_special;
		i = 0;
		do
		{
			itemidptr = &itemidbase[i];
			if (upper != itemidptr->itemoff + itemidptr->alignedlen)
				break;
			upper -= itemidptr->alignedlen;

			i++;
		} while (i < nitems);

		/*
		 * Now that we've found the first tuple that needs to be moved, we can
		 * do the tuple compactification.  We try and make the least number of
		 * memmove() calls and only call memmove() when there's a gap.  When
		 * we see a gap we just move all tuples after the gap up until the
		 * point of the last move operation.
		 */
		copy_tail = copy_head = itemidptr->itemoff + itemidptr->alignedlen;
		for (; i < nitems; i++)
		{
			ItemId		lp;

			itemidptr = &itemidbase[i];
			lp = PageGetItemId(page, itemidptr->offsetindex + 1);

			if (copy_head != itemidptr->itemoff + itemidptr->alignedlen)
			{
				memmove((char *) page + upper,
						page + copy_head,
						copy_tail - copy_head);

				/*
				 * We've now moved all tuples already seen, but not the
				 * current tuple, so we set the copy_tail to the end of this
				 * tuple so it can be moved in another iteration of the loop.
				 */
				copy_tail = itemidptr->itemoff + itemidptr->alignedlen;
			}
			/* shift the target offset down by the length of this tuple */
			upper -= itemidptr->alignedlen;
			/* point the copy_head to the start of this tuple */
			copy_head = itemidptr->itemoff;

			/* update the line pointer to reference the new offset */
			lp->lp_off = upper;

		}

		/* move the remaining tuples. */
		memmove((char *) page + upper,
				page + copy_head,
				copy_tail - copy_head);
	}
	else
	{
		PGAlignedBlock scratch;
		char	   *scratchptr = scratch.data;

		/*
		 * Non-presorted case:  The tuples in the itemidbase array may be in
		 * any order.  So, in order to move these to the end of the page we
		 * must make a temp copy of each tuple that needs to be moved before
		 * we copy them back into the page at the new offset.
		 *
		 * If a large percentage of tuples have been pruned (>75%) then we'll
		 * copy these into the temp buffer tuple-by-tuple, otherwise, we'll
		 * just do a single memcpy() for all tuples that need to be moved.
		 * When so many tuples have been removed there's likely to be a lot of
		 * gaps and it's unlikely that many non-movable tuples remain at the
		 * end of the page.
		 */
		if (nitems < PageGetMaxOffsetNumber(page) / 4)
		{
			i = 0;
			do
			{
				itemidptr = &itemidbase[i];
				memcpy(scratchptr + itemidptr->itemoff, page + itemidptr->itemoff,
					   itemidptr->alignedlen);
				i++;
			} while (i < nitems);

			/* Set things up for the compactification code below */
			i = 0;
			itemidptr = &itemidbase[0];
			upper = phdr->pd_special;
		}
		else
		{
			upper = phdr->pd_special;

			/*
			 * Many tuples are likely to already be in the correct location.
			 * There's no need to copy these into the temp buffer.  Instead
			 * we'll just skip forward in the itemidbase array to the position
			 * that we do need to move tuples from so that the code below just
			 * leaves these ones alone.
			 */
			i = 0;
			do
			{
				itemidptr = &itemidbase[i];
				if (upper != itemidptr->itemoff + itemidptr->alignedlen)
					break;
				upper -= itemidptr->alignedlen;

				i++;
			} while (i < nitems);

			/* Copy all tuples that need to be moved into the temp buffer */
			memcpy(scratchptr + phdr->pd_upper,
				   page + phdr->pd_upper,
				   upper - phdr->pd_upper);
		}

		/*
		 * Do the tuple compactification.  itemidptr is already pointing to
		 * the first tuple that we're going to move.  Here we collapse the
		 * memcpy calls for adjacent tuples into a single call.  This is done
		 * by delaying the memcpy call until we find a gap that needs to be
		 * closed.
		 */
		copy_tail = copy_head = itemidptr->itemoff + itemidptr->alignedlen;
		for (; i < nitems; i++)
		{
			ItemId		lp;

			itemidptr = &itemidbase[i];
			lp = PageGetItemId(page, itemidptr->offsetindex + 1);

			/* copy pending tuples when we detect a gap */
			if (copy_head != itemidptr->itemoff + itemidptr->alignedlen)
			{
				memcpy((char *) page + upper,
					   scratchptr + copy_head,
					   copy_tail - copy_head);

				/*
				 * We've now copied all tuples already seen, but not the
				 * current tuple, so we set the copy_tail to the end of this
				 * tuple.
				 */
				copy_tail = itemidptr->itemoff + itemidptr->alignedlen;
			}
			/* shift the target offset down by the length of this tuple */
			upper -= itemidptr->alignedlen;
			/* point the copy_head to the start of this tuple */
			copy_head = itemidptr->itemoff;

			/* update the line pointer to reference the new offset */
			lp->lp_off = upper;

		}

		/* Copy the remaining chunk */
		memcpy((char *) page + upper,
			   scratchptr + copy_head,
			   copy_tail - copy_head);
	}

	phdr->pd_upper = upper;
}

void PageRepairFragmentation(char *page)
{
	Offset		pd_lower = ((PageHeader) page)->pd_lower;
	Offset		pd_upper = ((PageHeader) page)->pd_upper;
	Offset		pd_special = ((PageHeader) page)->pd_special;
	Offset		last_offset;
	itemIdCompactData itemidbase[MaxHeapTuplesPerPage];
	itemIdCompact itemidptr;
	ItemId		lp;
	int			nline,
				nstorage,
				nunused;
	int			i;
	Size		totallen;
	bool		presorted = true;	/* For now */

	/*
	 * It's worth the trouble to be more paranoid here than in most places,
	 * because we are about to reshuffle data in (what is usually) a shared
	 * disk buffer.  If we aren't careful then corrupted pointers, lengths,
	 * etc could cause us to clobber adjacent disk buffers, spreading the data
	 * loss further.  So, check everything.
	 */
	if (pd_lower < SizeOfPageHeaderData ||
		pd_lower > pd_upper ||
		pd_upper > pd_special ||
		pd_special > BLCKSZ ||
		pd_special != MAXALIGN(pd_special))
		printf("corrupted page pointers: lower = %u, upper = %u, special = %u\n",
						pd_lower, pd_upper, pd_special);

	/*
	 * Run through the line pointer array and collect data about live items.
	 */
	nline = PageGetMaxOffsetNumber(page);
	itemidptr = itemidbase;
	nunused = totallen = 0;
	last_offset = pd_special;
	for (i = FirstOffsetNumber; i <= nline; i++)
	{
		lp = PageGetItemId(page, i);
		if (ItemIdIsUsed(lp))
		{
			if (ItemIdHasStorage(lp))
			{
				itemidptr->offsetindex = i - 1;
				itemidptr->itemoff = ItemIdGetOffset(lp);

				if (last_offset > itemidptr->itemoff)
					last_offset = itemidptr->itemoff;
				else
					presorted = false;

				if (unlikely(itemidptr->itemoff < (int) pd_upper ||
							 itemidptr->itemoff >= (int) pd_special))
					printf("corrupted line pointer: %u",
									itemidptr->itemoff);
				itemidptr->alignedlen = MAXALIGN(ItemIdGetLength(lp));
				totallen += itemidptr->alignedlen;
				itemidptr++;
			}
		}
		else
		{
			/* Unused entries should have lp_len = 0, but make sure */
			ItemIdSetUnused(lp);
			nunused++;
		}
	}

	nstorage = itemidptr - itemidbase;
	if (nstorage == 0)
	{
		/* Page is completely empty, so just reset it quickly */
		((PageHeader) page)->pd_upper = pd_special;
	}
	else
	{
		/* Need to compact the page the hard way */
		if (totallen > (Size) (pd_special - pd_lower))
			printf("corrupted item lengths: total %u, available space %u",
							(unsigned int) totallen, pd_special - pd_lower);

		compactify_tuples(itemidbase, nstorage, page, presorted);
	}

	/* Set hint bit for PageAddItemExtended */
	if (nunused > 0)
		PageSetHasFreeLinePointers(page);
	else
		PageClearHasFreeLinePointers(page);
}

#if PG_VERSION_NUM < 17
void heap_page_prune_execute(char *page,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused)
{
	OffsetNumber *offnum;
	int			i;

	/* Shouldn't be called unless there's something to do */
	Assert(nredirected > 0 || ndead > 0 || nunused > 0);

	/* Update all redirected line pointers */
	offnum = redirected;
	for (i = 0; i < nredirected; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId		fromlp = PageGetItemId(page, fromoff);

		ItemIdSetRedirect(fromlp, tooff);
	}

	/* Update all now-dead line pointers */
	offnum = nowdead;
	for (i = 0; i < ndead; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

		ItemIdSetDead(lp);
	}

	/* Update all now-unused line pointers */
	offnum = nowunused;
	for (i = 0; i < nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

		ItemIdSetUnused(lp);
	}

	/*
	 * Finally, repair any fragmentation, and update the page's hint bit about
	 * whether it has free pointers.
	 */
	PageRepairFragmentation(page);
}
void heap_xlog_prune(XLogReaderState *record,BlockNumber blk,RelFileNumber filenode)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_prune *xlrec = (xl_heap_prune *) XLogRecGetData(record);
	Buffer		buffer;
	RelFileNode rnode;
	BlockNumber blkno;
	XLogRedoAction action;

	XLogRecGetBlockTag(record, 0, &rnode, NULL, &blkno);

	char page[BLCKSZ]={0};
	if(!FPWfromFile(blk,page,filenode)){
		return;
	}
	OffsetNumber *end;
	OffsetNumber *redirected;
	OffsetNumber *nowdead;
	OffsetNumber *nowunused;
	int			nredirected;
	int			ndead;
	int			nunused;
	Size		datalen;

	redirected = (OffsetNumber *) XLogRecGetBlockData(record, 0, &datalen);

	nredirected = xlrec->nredirected;
	ndead = xlrec->ndead;
	end = (OffsetNumber *) ((char *) redirected + datalen);
	nowdead = redirected + (nredirected * 2);
	nowunused = nowdead + ndead;
	nunused = (end - nowunused);
	Assert(nunused >= 0);

	/* Update all line pointers per the record, and repair fragmentation */
	heap_page_prune_execute(page,
							redirected, nredirected,
							nowdead, ndead,
							nowunused, nunused);
	/*
		* Note: we don't worry about updating the page's prunability hints.
		* At worst this will cause an extra prune cycle to occur soon.
		*/
	PageSetLSN(page, lsn);
	FPW2File(blk,page,filenode);
}
#else
void heap_page_prune_execute(char *page, bool lp_truncate_only,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused)
{
	OffsetNumber *offnum;

	/* Shouldn't be called unless there's something to do */
	Assert(nredirected > 0 || ndead > 0 || nunused > 0);

	/* If 'lp_truncate_only', we can only remove already-dead line pointers */
	Assert(!lp_truncate_only || (nredirected == 0 && ndead == 0));

	/* Update all redirected line pointers */
	offnum = redirected;
	for (int i = 0; i < nredirected; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId		fromlp = PageGetItemId(page, fromoff);
		ItemIdSetRedirect(fromlp, tooff);
	}

	/* Update all now-dead line pointers */
	offnum = nowdead;
	for (int i = 0; i < ndead; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);
		ItemIdSetDead(lp);
	}

	/* Update all now-unused line pointers */
	offnum = nowunused;
	for (int i = 0; i < nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);
		ItemIdSetUnused(lp);
	}

	if (lp_truncate_only)
		PageTruncateLinePointerArray(page);
	else
	{
		/*
		 * Finally, repair any fragmentation, and update the page's hint bit
		 * about whether it has free pointers.
		 */
		PageRepairFragmentation(page);

		/*
		 * Now that the page has been modified, assert that redirect items
		 * still point to valid targets.
		 */
	}
}

void
heap_xlog_deserialize_prune_and_freeze(char *cursor, uint8 flags,
									   int *nplans, xlhp_freeze_plan **plans,
									   OffsetNumber **frz_offsets,
									   int *nredirected, OffsetNumber **redirected,
									   int *ndead, OffsetNumber **nowdead,
									   int *nunused, OffsetNumber **nowunused)
{
	if (flags & XLHP_HAS_FREEZE_PLANS)
	{
		xlhp_freeze_plans *freeze_plans = (xlhp_freeze_plans *) cursor;

		*nplans = freeze_plans->nplans;
		Assert(*nplans > 0);
		*plans = freeze_plans->plans;

		cursor += offsetof(xlhp_freeze_plans, plans);
		cursor += sizeof(xlhp_freeze_plan) * *nplans;
	}
	else
	{
		*nplans = 0;
		*plans = NULL;
	}

	if (flags & XLHP_HAS_REDIRECTIONS)
	{
		xlhp_prune_items *subrecord = (xlhp_prune_items *) cursor;

		*nredirected = subrecord->ntargets;
		Assert(*nredirected > 0);
		*redirected = &subrecord->data[0];

		cursor += offsetof(xlhp_prune_items, data);
		cursor += sizeof(OffsetNumber[2]) * *nredirected;
	}
	else
	{
		*nredirected = 0;
		*redirected = NULL;
	}

	if (flags & XLHP_HAS_DEAD_ITEMS)
	{
		xlhp_prune_items *subrecord = (xlhp_prune_items *) cursor;

		*ndead = subrecord->ntargets;
		Assert(*ndead > 0);
		*nowdead = subrecord->data;

		cursor += offsetof(xlhp_prune_items, data);
		cursor += sizeof(OffsetNumber) * *ndead;
	}
	else
	{
		*ndead = 0;
		*nowdead = NULL;
	}

	if (flags & XLHP_HAS_NOW_UNUSED_ITEMS)
	{
		xlhp_prune_items *subrecord = (xlhp_prune_items *) cursor;

		*nunused = subrecord->ntargets;
		Assert(*nunused > 0);
		*nowunused = subrecord->data;

		cursor += offsetof(xlhp_prune_items, data);
		cursor += sizeof(OffsetNumber) * *nunused;
	}
	else
	{
		*nunused = 0;
		*nowunused = NULL;
	}

	*frz_offsets = (OffsetNumber *) cursor;
}

void heap_xlog_prune(XLogReaderState *record,BlockNumber blk,RelFileNumber filenode)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	char	   *maindataptr = XLogRecGetData(record);
	xl_heap_prune xlrec;
	Buffer		buffer;
	RelFileLocator rlocator;
	BlockNumber blkno;
	XLogRedoAction action;

	XLogRecGetBlockTag(record, 0, (RelFileNode *)&rlocator, NULL, &blkno);
	memcpy(&xlrec, maindataptr, SizeOfHeapPrune);
	maindataptr += SizeOfHeapPrune;

	/*
	 * We are about to remove and/or freeze tuples.  In Hot Standby mode,
	 * ensure that there are no queries running for which the removed tuples
	 * are still visible or which still consider the frozen xids as running.
	 * The conflict horizon XID comes after xl_heap_prune.
	 */
	if ((xlrec.flags & XLHP_HAS_CONFLICT_HORIZON) != 0)
	{
		TransactionId snapshot_conflict_horizon;

		memcpy(&snapshot_conflict_horizon, maindataptr, sizeof(TransactionId));
		maindataptr += sizeof(TransactionId);
	}

	char page[BLCKSZ]={0};
	if(!FPWfromFile(blk,page,filenode)){
		return;
	}
	OffsetNumber *redirected;
	OffsetNumber *nowdead;
	OffsetNumber *nowunused;
	int			nredirected;
	int			ndead;
	int			nunused;
	int			nplans;
	Size		datalen;
	xlhp_freeze_plan *plans;
	OffsetNumber *frz_offsets;
	char	   *dataptr = (char *)XLogRecGetBlockData(record, 0, &datalen);

	heap_xlog_deserialize_prune_and_freeze(dataptr, xlrec.flags,
											&nplans, &plans, &frz_offsets,
											&nredirected, &redirected,
											&ndead, &nowdead,
											&nunused, &nowunused);

	/*
		* Update all line pointers per the record, and repair fragmentation
		* if needed.
		*/
	if (nredirected > 0 || ndead > 0 || nunused > 0)
		heap_page_prune_execute(page,
								(xlrec.flags & XLHP_CLEANUP_LOCK) == 0,
								redirected, nredirected,
								nowdead, ndead,
								nowunused, nunused);

	/* Freeze tuples */

	// 	/*
	// 		* Convert freeze plan representation from WAL record into
	// 		* per-tuple format used by heap_execute_freeze_tuple
	// 		*/

	/* There should be no more data */
	Assert((char *) frz_offsets == dataptr + datalen);

	/*
		* Note: we don't worry about updating the page's prunability hints.
		* At worst this will cause an extra prune cycle to occur soon.
		*/

	PageSetLSN(page, lsn);
	FPW2File(blk,page,filenode);

}
#endif

void heap_xlog_insert(XLogReaderState *record,BlockNumber blk,RelFileNumber filenode)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_insert *xlrec = (xl_heap_insert *) XLogRecGetData(record);
	Buffer		buffer;
	char	page[BLCKSZ]={0};
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	HeapTupleHeader htup;
	xl_heap_header xlhdr;
	uint32		newlen;
	Size		freespace = 0;
	RelFileNode target_node;
	BlockNumber blkno;
	ItemPointerData target_tid;

	XLogRecGetBlockTag(record, 0, &target_node, NULL, &blkno);
	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	/*
	 * If we inserted the first and only tuple on the page, re-initialize the
	 * page from scratch.
	 */
	if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
	{
		PageInit(page, BLCKSZ, 0);
	}
	else{
		if(!FPWfromFile(blk,page,filenode)){
			return;
		}
	}

	Size		datalen;
	char	   *data;

	int a = PageGetMaxOffsetNumber(page);
	if (PageGetMaxOffsetNumber(page) + 1 < xlrec->offnum){
		printf("invalid max offset number\n");
		return;
	}

	data = (char *)XLogRecGetBlockData(record, 0, &datalen);

	newlen = datalen - SizeOfHeapHeader;
	Assert(datalen > SizeOfHeapHeader && newlen <= MaxHeapTupleSize);
	memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
	data += SizeOfHeapHeader;

	htup = &tbuf.hdr;
	MemSet((char *) htup, 0, SizeofHeapTupleHeader);
	/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
	memcpy((char *) htup + SizeofHeapTupleHeader,
			data,
			newlen);
	newlen += SizeofHeapTupleHeader;
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;
	HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	htup->t_ctid = target_tid;

	if (PageAddItem(page, (Item) htup, newlen, xlrec->offnum,
					true, true) == InvalidOffsetNumber)
		printf("failed to add tuple");

	PageSetLSN(page, lsn);

	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
		PageClearAllVisible(page);

	/* XLH_INSERT_ALL_FROZEN_SET implies that all tuples are visible */
	if (xlrec->flags & XLH_INSERT_ALL_FROZEN_SET)
		PageSetAllVisible(page);

	FPW2File(blk,page,filenode);

}

void PageTruncateLinePointerArray(Page page)
{
	PageHeader	phdr = (PageHeader) page;
	bool		countdone = false,
				sethint = false;
	int			nunusedend = 0;

	/* Scan line pointer array back-to-front */
	for (int i = PageGetMaxOffsetNumber(page); i >= FirstOffsetNumber; i--)
	{
		ItemId		lp = PageGetItemId(page, i);

		if (!countdone && i > FirstOffsetNumber)
		{
			/*
			 * Still determining which line pointers from the end of the array
			 * will be truncated away.  Either count another line pointer as
			 * safe to truncate, or notice that it's not safe to truncate
			 * additional line pointers (stop counting line pointers).
			 */
			if (!ItemIdIsUsed(lp))
				nunusedend++;
			else
				countdone = true;
		}
		else
		{
			/*
			 * Once we've stopped counting we still need to figure out if
			 * there are any remaining LP_UNUSED line pointers somewhere more
			 * towards the front of the array.
			 */
			if (!ItemIdIsUsed(lp))
			{
				/*
				 * This is an unused line pointer that we won't be truncating
				 * away -- so there is at least one.  Set hint on page.
				 */
				sethint = true;
				break;
			}
		}
	}

	if (nunusedend > 0)
	{
		phdr->pd_lower -= sizeof(ItemIdData) * nunusedend;

#ifdef CLOBBER_FREED_MEMORY
		memset((char *) page + phdr->pd_lower, 0x7F,
			   sizeof(ItemIdData) * nunusedend);
#endif
	}
	else
		Assert(sethint);

	/* Set hint bit for PageAddItemExtended */
	if (sethint)
		PageSetHasFreeLinePointers(page);
	else
		PageClearHasFreeLinePointers(page);
}

void heap_xlog_vacuum(XLogReaderState *record,BlockNumber blk,RelFileNumber filenode)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_vacuum *xlrec = (xl_heap_vacuum *) XLogRecGetData(record);
	BlockNumber blkno;

	/*
	 * If we have a full-page image, restore it	(without using a cleanup lock)
	 * and we're done.
	 */
	char page[BLCKSZ]={0};
	if(!FPWfromFile(blk,page,filenode)){
		return;
	}
	OffsetNumber *nowunused;
	Size		datalen;
	OffsetNumber *offnum;

	nowunused = (OffsetNumber *) XLogRecGetBlockData(record, 0, &datalen);

	/* Shouldn't be a record unless there's something to do */
	Assert(xlrec->nunused > 0);

	/* Update all now-unused line pointers */
	offnum = nowunused;
	for (int i = 0; i < xlrec->nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

		Assert(ItemIdIsDead(lp) && !ItemIdHasStorage(lp));
		ItemIdSetUnused(lp);
	}

	/* Attempt to truncate line pointer array now */
	PageTruncateLinePointerArray(page);

	PageSetLSN(page, lsn);
	FPW2File(blk,page,filenode);
}

void heap_xlog_inplace(XLogReaderState *record,BlockNumber blk,RelFileNumber filenode)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_inplace *xlrec = (xl_heap_inplace *) XLogRecGetData(record);
	Buffer		buffer;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	uint32		oldlen;
	Size		newlen;

	char	   *newtup = (char *)XLogRecGetBlockData(record, 0, &newlen);

	char page[BLCKSZ]={0};
	if(!FPWfromFile(blk,page,filenode)){
		return;
	}

	offnum = xlrec->offnum;
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	int a = PageGetMaxOffsetNumber(page);
	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp)){
		warningInvalidLp();;
		return;
	}
	htup = (HeapTupleHeader) PageGetItem(page, lp);

	oldlen = ItemIdGetLength(lp) - htup->t_hoff;
	if (oldlen != newlen){
		printf("wrong tuple length");
		return;
	}

	memcpy((char *) htup + htup->t_hoff, newtup, newlen);

	PageSetLSN(page, lsn);
	FPW2File(blk,page,filenode);
}

void heap_xlog_multi_insert(XLogReaderState *record,BlockNumber blk,RelFileNumber filenode){
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_multi_insert *xlrec;
	RelFileNode rnode;
	BlockNumber blkno;
	Buffer		buffer;
	char	page[BLCKSZ]={0};
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	HeapTupleHeader htup;
	uint32		newlen;
	Size		freespace = 0;
	int			i;
	bool		isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;

	/*
	 * Insertion doesn't overwrite MVCC data, so no conflict processing is
	 * required.
	 */
	xlrec = (xl_heap_multi_insert *) XLogRecGetData(record);

	XLogRecGetBlockTag(record, 0, &rnode, NULL, &blkno);

	/* check that the mutually exclusive flags are not both set */
	Assert(!((xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) &&
			 (xlrec->flags & XLH_INSERT_ALL_FROZEN_SET)));

	if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
	{
		PageInit(page, BLCKSZ, 0);
	}
	else
	{
		if(!FPWfromFile(blk,page,filenode)){
			return;
		}
	}

	char	   *tupdata;
	char	   *endptr;
	Size		len;

	/* Tuples are stored as block data */
	tupdata = (char *)XLogRecGetBlockData(record, 0, &len);
	endptr = tupdata + len;

	for (i = 0; i < xlrec->ntuples; i++)
	{
		OffsetNumber offnum;
		xl_multi_insert_tuple *xlhdr;

		/*
			* If we're reinitializing the page, the tuples are stored in
			* order from FirstOffsetNumber. Otherwise there's an array of
			* offsets in the WAL record, and the tuples come after that.
			*/
		if (isinit)
			offnum = FirstOffsetNumber + i;
		else
			offnum = xlrec->offsets[i];

		if (PageGetMaxOffsetNumber(page) + 1 < offnum){
			printf("invalid max offset number\n");
			return;
		}

		xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(tupdata);
		tupdata = ((char *) xlhdr) + SizeOfMultiInsertTuple;

		newlen = xlhdr->datalen;
		Assert(newlen <= MaxHeapTupleSize);
		htup = &tbuf.hdr;
		MemSet((char *) htup, 0, SizeofHeapTupleHeader);
		/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
		memcpy((char *) htup + SizeofHeapTupleHeader,
				(char *) tupdata,
				newlen);
		tupdata += newlen;

		newlen += SizeofHeapTupleHeader;
		htup->t_infomask2 = xlhdr->t_infomask2;
		htup->t_infomask = xlhdr->t_infomask;
		htup->t_hoff = xlhdr->t_hoff;
		HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		ItemPointerSetBlockNumber(&htup->t_ctid, blkno);
		ItemPointerSetOffsetNumber(&htup->t_ctid, offnum);

		offnum = PageAddItem(page, (Item) htup, newlen, offnum, true, true);
		if (offnum == InvalidOffsetNumber)
			printf("failed to add tuple");
	}
	if (tupdata != endptr)
		printf("total tuple length mismatch");

	PageSetLSN(page, lsn);

	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
		PageClearAllVisible(page);

	/* XLH_INSERT_ALL_FROZEN_SET implies that all tuples are visible */
	if (xlrec->flags & XLH_INSERT_ALL_FROZEN_SET)
		PageSetAllVisible(page);

	FPW2File(blk,page,filenode);
}

char* restoreSysDrop(decodeFunc *array2Process,xl_heap_delete *del_xlrec,char *page,BlockNumber blk,XLogRecPtr recordLsn,int addNumLocal,const char *savepath,RelFileNumber filenode){
	ItemId	itemId = NULL;
	HeapTupleHeader	tupleHeader = NULL;
	int	tuplelen = 0;
	if(!FPWfileExist(blk,filenode)){ /* If FPW not found, report error */
		ErrBlkNotFound(blk,recordLsn);
		FPIErrcount++;
		return "NoWayOut";
	}
	else{ /* If found, read directly */
		FPWfromFile(blk,page,filenode);
		itemId = PageGetItemId(page, del_xlrec->offnum);
		tupleHeader = (HeapTupleHeader)PageGetItem(page, itemId);
		tuplelen = (Size)itemId->lp_len;
	}
	FILE *logSucc=fopen("logPathSucc.txt","a");
	FILE *logErr=fopen("logPathErr.txt","a");
	char *xman=xmandecodeSys(allDesc,(decodeFunc *)array2Process,(const char *)tupleHeader,tuplelen,TABLE_BOOTTYPE,addNumLocal);

	fclose(logSucc);
	fclose(logErr);
	unlink("logPathSucc.txt");
	unlink("logPathErr.txt");

	return xman;
}

/**
 * restoreDEL - Restore deleted record from FPW
 *
 * @Tx_parray:     Transaction array
 * @bootFile:      Output file handle
 * @array2Process: Decode function array
 * @del_xlrec:     Delete XLog record
 * @tabname:       Table name
 * @page:          Page buffer
 * @blk:           Block number
 * @currentTx:     Current transaction ID
 * @filenode:      File node identifier
 *
 * Returns: Status code
 */
int restoreDEL(parray *Tx_parray,FILE *bootFile,decodeFunc *array2Process,xl_heap_delete *del_xlrec,char *tabname,char *page,BlockNumber blk,TransactionId currentTx,RelFileNumber filenode){
	ItemId	itemId = NULL;
	HeapTupleHeader	tupleHeader = NULL;
	int	tuplelen = 0;
	if(txInTxArrayOrNot(currentTx,Tx_parray,restoreMode_there)){ /* If target tx and not toast round */
		if(!FPWfileExist(blk,filenode)){ /* If FPW not found, report error */
			LsnBlkInfo *elem = (LsnBlkInfo*)malloc(sizeof(LsnBlkInfo));
			strcpy(elem->LSN,lsn);
			elem->blk = blk;
			parray_append(LsnBlkInfos,elem);
			#ifdef CN
			printf("%s\nrestoreDEL时发现缺失的FPI %sblk %d%s，单条解析数据失败%s\n",COLOR_ERROR,COLOR_UNLOAD,blk,COLOR_ERROR,C_RESET);
			#else
			printf("%s\nMissing FPI %sblk %d%s found during restoreDEL, single record parsing failed%s\n",COLOR_ERROR,COLOR_UNLOAD,blk,COLOR_ERROR,C_RESET);
			#endif
			FPIErrcount++;
			return CONTINUE_RET;
		}
		else{ /* If found, read directly */
			FPWfromFile(blk,page,filenode);
			itemId = PageGetItemId(page, del_xlrec->offnum);
			if(!ItemIdIsUsed(itemId)){
				#ifdef CN
				printf("\n%srestoreDEL时发现异常的lp，请联系开发者及时处理，退出恢复！%s\n",COLOR_ERROR,C_RESET);
				#else
				printf("\n%sAbnormal lp found during restoreDEL, please contact the developer for immediate handling. Recovery exited!%s\n",COLOR_ERROR,C_RESET);
				#endif
				FPIErrcount++;
				return BREAK_RET;
			}
			tupleHeader = (HeapTupleHeader)PageGetItem(page, itemId);
			tuplelen = (Size)itemId->lp_len;
		}

		FILE *logSucc=fopen("log/logPathSucc.txt","a");
		FILE *logErr=fopen("log/logPathErr.txt","a");
		char *xman=xmanDecode(dropExist2,allDesc,(decodeFunc *)array2Process,(const char *)tupleHeader,tuplelen,TABLE_BOOTTYPE,logSucc,logErr);
		if ( strcmp(xman,"NoWayOut") == 0 ){
			fclose(logSucc);
			fclose(logErr);
			printf("\n%srestoreDEL时解析tuple数据failed%s\n",COLOR_ERROR,C_RESET);
			FPIErrcount++;
			return BREAK_RET;
		}
		else{
			char *xmanret=NULL;
			if(ExportMode_there == CSVform){
				xmanret = xman;
			}
			else if (ExportMode_there == SQLform){
				xmanret = xman2Insertxman(xman,tabname);
			}
			commaStrWriteIntoDecodeTab(xmanret,bootFile);
			FPIcount++;
			fclose(logSucc);
			fclose(logErr);
		}
		infoRestoreRecs(FPIcount);
	}
	return CONTINUE_RET;
}

/**
 * restoreUPDATE - Restore updated record from FPW
 *
 * @allDesc:       Attribute descriptions
 * @record:        XLog reader state
 * @Tx_parray:     Transaction array
 * @bootFile:      Output file handle
 * @array2Process: Decode function array
 * @tabname:       Table name
 * @page:          Page buffer
 * @blk:           Block number
 * @hot_update:    True if HOT update
 * @currentTx:     Current transaction ID
 * @filenode:      File node identifier
 *
 * Returns: Status code
 */
int restoreUPDATE(pg_attributeDesc *allDesc,XLogReaderState *record,parray *Tx_parray,FILE *bootFile,decodeFunc *array2Process,char *tabname,char *page,BlockNumber blk,bool hot_update,TransactionId currentTx,RelFileNumber filenode)
{
	char *xman=NULL;
	ItemId	itemId = NULL;
	HeapTupleHeader	tupleHeader = NULL;
	int	tuplelen = 0;

	if(txInTxArrayOrNot(currentTx,Tx_parray,restoreMode_there)){ /* If target tx and not toast round */
		FILE *logSucc=fopen("log/logPathSucc.txt","a");
		FILE *logErr=fopen("log/logPathErr.txt","a");
		XLogRecPtr	lsn = record->EndRecPtr;
		xl_heap_update *xlrec = (xl_heap_update *) XLogRecGetData(record);
		RelFileNode rnode;
		BlockNumber oldblk;
		BlockNumber newblk;
		ItemPointerData newtid;
		char	oldpage[BLCKSZ]={0};
		char	newpage[BLCKSZ]={0};
		OffsetNumber offnum;
		ItemId		lp = NULL;
		ItemId		newlp = NULL;
		HeapTupleData oldtup;
		HeapTupleHeader htup;
		HeapTupleHeader newhtup;
		uint16		prefixlen = 0,
					suffixlen = 0;
		char	   *newp;
		union
		{
			HeapTupleHeaderData hdr;
			char		data[MaxHeapTupleSize];
		}			tbuf;
		xl_heap_header xlhdr;
		uint32		newlen;

		/* initialize to keep the compiler quiet */
		oldtup.t_data = NULL;
		oldtup.t_len = 0;

		XLogRecGetBlockTag(record, 0, &rnode, NULL, &newblk);
		#if PG_VERSION_NUM > 14
		if (XLogRecGetBlockTagExtended(record, 1, NULL, NULL, &oldblk, NULL))
		#else
		if (XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblk))
		#endif
		{
			/* HOT updates are never done across pages */
			Assert(!hot_update);
		}
		else
			oldblk = newblk;

		ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

		/* ============= Process old data block ============= */
		if(!FPWfromFile(oldblk,oldpage,filenode)){ /* If FPW not found, report error */
			LsnBlkInfo *elem = (LsnBlkInfo*)malloc(sizeof(LsnBlkInfo));
			sprintf(elem->LSN, "%X/%08X", LSN_FORMAT_ARGS(lsn));
			elem->blk = oldblk;
			parray_append(LsnBlkInfos,elem);
			FPIErrcount++;
			return CONTINUE_RET;
		}
		offnum = xlrec->old_offnum;

		if (PageGetMaxOffsetNumber(oldpage) >= offnum)
			lp = PageGetItemId(oldpage, offnum);

		if (PageGetMaxOffsetNumber(oldpage) < offnum || !ItemIdIsNormal(lp))
		{
			return -1;
		}

		tuplelen = (Size)lp->lp_len;
		htup = (HeapTupleHeader) PageGetItem(oldpage, lp);
		oldtup.t_data = htup;
		oldtup.t_len = ItemIdGetLength(lp);

		htup->t_infomask &= ~(HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		if (hot_update)
			HeapTupleHeaderSetHotUpdated(htup);
		else
			HeapTupleHeaderClearHotUpdated(htup);
		fix_infomask_from_infobits(xlrec->old_infobits_set, &htup->t_infomask,&htup->t_infomask2);
		HeapTupleHeaderSetXmax(htup, xlrec->old_xmax);
		HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
		/* Set forward chain link in t_ctid */
		htup->t_ctid = newtid;

		/* Mark the page as a candidate for pruning */

		if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(oldpage);

		xman=xmanDecode(dropExist2,allDesc,array2Process,(const char *)htup,tuplelen,TABLE_BOOTTYPE,logSucc,logErr);
		char *oldxman = xman;

		/* ============= End old data block processing ============= */

		/* ============= Process new data block ============= */
		if (oldblk == newblk){
			for (int i = 0; i < BLCKSZ; i++) {
				newpage[i] = oldpage[i];
			}
		}
		else{
			if(!FPWfromFile(newblk,newpage,filenode)){ /* If FPW not found, report error */
				LsnBlkInfo *elem = (LsnBlkInfo*)malloc(sizeof(LsnBlkInfo));
				sprintf(elem->LSN, "%X/%08X", LSN_FORMAT_ARGS(lsn));
				elem->blk = newblk;
				ErrBlkNotFound(newblk,lsn);
				parray_append(LsnBlkInfos,elem);
				
				freeOldParray();
				FPIErrcount++;
				return CONTINUE_RET;
			}
		}

		if(XLogRecHasBlockImage(record, 0)){
			offnum = xlrec->new_offnum;
			if (PageGetMaxOffsetNumber(newpage) + 1 < offnum){
				return -1;
			}

			if (PageGetMaxOffsetNumber(newpage) >= offnum){
				newlp = PageGetItemId(newpage, offnum);
			}

			if (PageGetMaxOffsetNumber(newpage) < offnum || !ItemIdIsNormal(newlp))
			{
				return -1;
			}
			tuplelen = (Size)newlp->lp_len;
			newhtup = (HeapTupleHeader) PageGetItem(newpage, newlp);

			xman=xmanDecode(dropExist2,allDesc,array2Process,(const char *)newhtup,tuplelen,TABLE_BOOTTYPE,logSucc,logErr);
		}
		else{
			/* Deal with new tuple */
			char	   *recdata;
			char	   *recdata_end;
			Size		datalen;
			Size		tuplen;

			recdata = (char *)XLogRecGetBlockData(record, 0, &datalen);
			if(recdata == NULL){
				recdata = (char *)XLogRecGetBlockData(record, 1, &datalen);
			}
			recdata_end = recdata + datalen;

			if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD)
			{
				Assert(newblk == oldblk);
				memcpy(&prefixlen, recdata, sizeof(uint16));
				recdata += sizeof(uint16);
			}
			if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
			{
				Assert(newblk == oldblk);
				memcpy(&suffixlen, recdata, sizeof(uint16));
				recdata += sizeof(uint16);
			}

			memcpy((char *) &xlhdr, recdata, SizeOfHeapHeader);
			recdata += SizeOfHeapHeader;

			tuplen = recdata_end - recdata;
			Assert(tuplen <= MaxHeapTupleSize);

			htup = &tbuf.hdr;
			MemSet((char *) htup, 0, SizeofHeapTupleHeader);

			/*
				* Reconstruct the new tuple using the prefix and/or suffix from the
				* old tuple, and the data stored in the WAL record.
				*/
			newp = (char *) htup + SizeofHeapTupleHeader;
			if (prefixlen > 0)
			{
				int			len;

				/* copy bitmap [+ padding] [+ oid] from WAL record */
				len = xlhdr.t_hoff - SizeofHeapTupleHeader;
				memcpy(newp, recdata, len);
				recdata += len;
				newp += len;

				/* copy prefix from old tuple */
				memcpy(newp, (char *) oldtup.t_data + oldtup.t_data->t_hoff, prefixlen);
				newp += prefixlen;

				/* copy new tuple data from WAL record */
				len = tuplen - (xlhdr.t_hoff - SizeofHeapTupleHeader);
				memcpy(newp, recdata, len);
				recdata += len;
				newp += len;
			}
			else
			{
				/*
					* copy bitmap [+ padding] [+ oid] + data from record, all in one
					* go
					*/
				memcpy(newp, recdata, tuplen);
				recdata += tuplen;
				newp += tuplen;
			}
			Assert(recdata == recdata_end);

			/* copy suffix from old tuple */
			if (suffixlen > 0)
				memcpy(newp, (char *) oldtup.t_data + oldtup.t_len - suffixlen, suffixlen);

			newlen = SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;
			htup->t_infomask2 = xlhdr.t_infomask2;
			htup->t_infomask = xlhdr.t_infomask;
			htup->t_hoff = xlhdr.t_hoff;

			HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
			HeapTupleHeaderSetCmin(htup, FirstCommandId);
			HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
			/* Make sure there is no forward chain link in t_ctid */
			htup->t_ctid = newtid;
			xman=xmanDecode(dropExist2,allDesc,array2Process,(const char *)htup,newlen,TABLE_BOOTTYPE,logSucc,logErr);
		}
		char *newxman = xman;
		if ( strcmp(newxman,"NoWayOut") == 0 ||  strcmp(oldxman,"NoWayOut") == 0  ){
			fclose(logSucc);
			fclose(logErr);
			FPIErrcount++;
			return CONTINUE_RET;
		}
		else{
			char *xmanret=xman2Updatexman((parray *)newxman,(parray *)oldxman,allDesc,tabname);
			if(strcmp(xmanret,"NoWayOut") == 0){
				FPIUpdateSame++;
				return CONTINUE_RET;
			}
			else{
				commaStrWriteIntoDecodeTab(xmanret,bootFile);
				free(xmanret);
				xmanret=NULL;
				FPIcount++;
			}
		}

		infoRestoreRecs(FPIcount);

		fclose(logSucc);
		fclose(logErr);
	}
	return CONTINUE_RET;
}

/**
 * XLogRecordRestoreFPWs - Restore data from XLog Full Page Writes
 *
 * @allDesc:       Attribute descriptions
 * @record:        XLog reader state
 * @savepath:      Output save path
 * @array2Process: Decode function array
 * @Tx_parray:     Transaction array
 * @tabname:       Table name
 * @bootFile:      Output file handle
 *
 * Returns: CONTINUE_RET or BREAK_RET
 */
int XLogRecordRestoreFPWs(pg_attributeDesc *allDesc,XLogReaderState *record, const char *savepath,decodeFunc *array2Process,
						  parray *Tx_parray,char tabname[50],FILE *bootFile)
{
	int			block_id;
	XLogRecPtr	xl_prev = XLogRecGetPrev(record);
	TransactionId currentTx = XLogRecGetXid(record);
	RmgrId rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	DELstruct *elem = parray_get(Tx_parray,0);

	int is_insert = 0;
	int is_update = 0;
	bool hot_update;

	if( info == XLOG_HEAP_UPDATE || info == (XLOG_HEAP_UPDATE | XLOG_HEAP_INIT_PAGE)){
		is_update = 1;
		hot_update = false;
	}

	else if(info == XLOG_HEAP_HOT_UPDATE || info == (XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE) ){
		is_update = 1;
		hot_update = true;
	}

	else if( info ==  (XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE) || info ==  XLOG_HEAP_INSERT){
		is_insert = 1;
	}

	if(rmid == HEAP_redo)
	{
		if (info == XLOG_HEAP_DELETE)
		{
			for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
			{
				char page[BLCKSZ]={0};
				char		toastFilename[MAXPGPATH];
				FILE	   *toastFile;
				BlockNumber blk;
				RelFileNode rnode;
				ForkNumber	fork;
				xl_heap_delete	*del_xlrec = NULL;
				sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

				#if PG_VERSION_NUM > 14
				(void) XLogRecGetBlockTagExtended(record, block_id,&rnode, &fork, &blk, NULL);
				#else
				XLogRecGetBlockTag(record, block_id,&rnode, &fork, &blk);
				#endif
				del_xlrec = (xl_heap_delete *) XLogRecGetData(record);

				#if LSNDSP == 1
				// 	XLogRecGetTotalLen(record),
				// 			XLogRecGetXid(record),
				// 			LSN_FORMAT_ARGS(record->ReadRecPtr),

				if(strcmp(lsn,LSNSTR) == 0){
					printf(" ");
				}
				#endif

				if(rnode.relNode != atoi(targetDatafile) && rnode.relNode != atoi(targetOldDatafile))
					return CONTINUE_RET;

				if(	lsnIsReached(record->ReadRecPtr,xl_prev,elem->endLSN) )
					return BREAK_RET;

				if(fork != MAIN_FORKNUM)
					return CONTINUE_RET;

				if ( XLogRecHasBlockImage(record, block_id) )
				{
					if (!RestoreBlockImage(record, block_id, page))
						printf("%s", record->errormsg_buf);
					FPW2File(blk,page,rnode.relNode);
				}
				else if(resTyp_there == UPDATEtyp){
					heap_xlog_delete(record,blk,rnode.relNode);
				}

				if(resTyp_there == DELETEtyp){
						if( restoreDEL(Tx_parray,bootFile,array2Process,del_xlrec,tabname,page,blk,currentTx,rnode.relNode) == BREAK_RET ){
						return BREAK_RET;
					}
				}
			}
		}
		else{
			char page[BLCKSZ]={0};
			char		toastFilename[MAXPGPATH];
			FILE	   *toastFile;
			BlockNumber blk;
			RelFileNode rnode;
			ForkNumber	fork;

			sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

			#if PG_VERSION_NUM > 14
			(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
			#else
			XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
			#endif

			#if LSNDSP == 1
			// XLogRecGetTotalLen(record),
			// 		XLogRecGetXid(record),
			// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
			if(strcmp(lsn,LSNSTR) == 0){
				printf(" ");
			}
			#endif

			if(rnode.relNode != atoi(targetDatafile) && rnode.relNode != atoi(targetOldDatafile))
				return CONTINUE_RET;

			if(	lsnIsReached(record->ReadRecPtr,xl_prev,elem->endLSN) )
				return BREAK_RET;

			if(fork != MAIN_FORKNUM)
				return CONTINUE_RET;

			if ( XLogRecHasBlockImage(record, 0) )
			{
				if (!RestoreBlockImage(record, 0, page))
					printf("%s", record->errormsg_buf);
				FPW2File(blk,page,rnode.relNode);
			}
			else
			{
				if( is_update ){
					heap_xlog_update(record,hot_update,blk,rnode.relNode);
				}
				else if( is_insert ){
					heap_xlog_insert(record,blk,rnode.relNode);
				}
				else if( info == XLOG_HEAP_INPLACE){
					heap_xlog_inplace(record,blk,rnode.relNode);
				}
			}

			if(resTyp_there == UPDATEtyp && is_update){
				setExportMode_decode(SQLform);
				if( restoreUPDATE(allDesc,record,Tx_parray,bootFile,array2Process,tabname,page,blk,hot_update,currentTx,rnode.relNode) == BREAK_RET )
				{
					setExportMode_decode(CSVform);
					return BREAK_RET;
				}
				setExportMode_decode(CSVform);
			}
		}
	}
	else if (rmid == HEAP2_redo)
	{
		for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
		{
			char page[BLCKSZ]={0};
			char		toastFilename[MAXPGPATH];
			FILE	   *toastFile;
			BlockNumber blk;
			RelFileNode rnode;
			ForkNumber	fork;

			#if PG_VERSION_NUM > 14
			(void) XLogRecGetBlockTagExtended(record, block_id,&rnode, &fork, &blk, NULL);
			#else
			XLogRecGetBlockTag(record, block_id,&rnode, &fork, &blk);
			#endif
			sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

			#if LSNDSP == 1
			// XLogRecGetTotalLen(record),
			// 		XLogRecGetXid(record),
			// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
			if(strcmp(lsn,LSNSTR) == 0){
				printf(" ");
			}
			#endif

			if(rnode.relNode != atoi(targetDatafile) && rnode.relNode != atoi(targetOldDatafile))
				return CONTINUE_RET;

			if(	lsnIsReached(record->ReadRecPtr,xl_prev,elem->endLSN) )
				return BREAK_RET;

			if(fork != MAIN_FORKNUM)
				continue;

			if ( XLogRecHasBlockImage(record, block_id) )
			{
				if (!RestoreBlockImage(record, block_id, page))
					printf("%s", record->errormsg_buf);
				FPW2File(blk,page,rnode.relNode);
			}
			else{
				if(info == XLOG_HEAP2_VISIBLE||info == XLOG_HEAP2_FREEZE_PAGE)
				{

				}
				else if( info == XLOG_HEAP2_VACUUM)
				{
					#if PG_VERSION_NUM < 17
					heap_xlog_vacuum(record,blk,rnode.relNode);
					#else
					heap_xlog_prune(record,blk,rnode.relNode);
					#endif
				}
				else if(info == XLOG_HEAP2_PRUNE)
				{
					heap_xlog_prune(record,blk,rnode.relNode);
				}

				else if( info == (XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE) || info == XLOG_HEAP2_MULTI_INSERT){
					heap_xlog_multi_insert(record,blk,rnode.relNode);
				}
			}
		}
	}
	else if (rmid == BTREE_redo)
	{
		BlockNumber blk;
		RelFileNode rnode;
		ForkNumber	fork;
		#if PG_VERSION_NUM > 14
		(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
		#else
		XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
		#endif
		sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

		#if LSNDSP == 1
		// XLogRecGetTotalLen(record),
		// 		XLogRecGetXid(record),
		// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
		if(strcmp(lsn,LSNSTR) == 0){
			printf(" ");
		}
		#endif
	}
	else if (rmid == XLOG_redo)
	{
		char page[BLCKSZ]={0};
		char		toastFilename[MAXPGPATH];
		FILE	   *toastFile;
		BlockNumber blk;
		RelFileNode rnode;
		ForkNumber	fork;
		#if PG_VERSION_NUM > 14
		(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
		#else
		XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
		#endif
		sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

		#if LSNDSP == 1
		// XLogRecGetTotalLen(record),
		// 		XLogRecGetXid(record),
		// 		LSN_FORMAT_ARGS(record->ReadRecPtr),

		#endif

		if(rnode.relNode != atoi(targetDatafile) && rnode.relNode != atoi(targetOldDatafile))
			return CONTINUE_RET;

		if(	lsnIsReached(record->ReadRecPtr,xl_prev,elem->endLSN) )
			return BREAK_RET;

		if(fork != MAIN_FORKNUM)
			return CONTINUE_RET;

		if ( XLogRecHasBlockImage(record, 0) )
		{
			if (!RestoreBlockImage(record, 0, page))
				printf("%s", record->errormsg_buf);
			FPW2File(blk,page,rnode.relNode);
		}

	}
	else if(rmid == TRANSACTION_redo){
		BlockNumber blk;
		RelFileNode rnode;
		ForkNumber	fork;
		#if PG_VERSION_NUM > 14
		(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
		#else
		XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
		#endif
		sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

		#if LSNDSP == 1
		// XLogRecGetTotalLen(record),
		// 		XLogRecGetXid(record),
		// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
		if(strcmp(lsn,LSNSTR) == 0){
			printf(" ");
		}
		#endif
	}
	return CONTINUE_RET;
}

int XLogRecordRestoreFPWsforTOAST(pg_attributeDesc *allDesc,XLogReaderState *record, const char *savepath,decodeFunc *array2Process,
						  parray *Tx_parray,char tabname[50],FILE *bootFile)
{
	int			block_id;
	XLogRecPtr	xl_prev = XLogRecGetPrev(record);
	TransactionId currentTx = XLogRecGetXid(record);
	RmgrId rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	DELstruct *elem = parray_get(Tx_parray,0);

	int is_insert = 0;
	int is_update = 0;
	bool hot_update;

	if( info == XLOG_HEAP_UPDATE || info == (XLOG_HEAP_UPDATE | XLOG_HEAP_INIT_PAGE)){
		is_update = 1;
		hot_update = false;
	}

	else if(info == XLOG_HEAP_HOT_UPDATE || info == (XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE) ){
		is_update = 1;
		hot_update = true;
	}

	else if( info ==  (XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE) || info ==  XLOG_HEAP_INSERT)
		is_insert = 1;

	if(rmid == HEAP_redo)
	{
		if (info == XLOG_HEAP_DELETE)
		{
			for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
			{
				char page[BLCKSZ]={0};
				char		toastFilename[MAXPGPATH];
				FILE	   *toastFile;
				BlockNumber blk;
				RelFileNode rnode;
				ForkNumber	fork;
				xl_heap_delete	*del_xlrec = NULL;

				#if PG_VERSION_NUM > 14
				(void) XLogRecGetBlockTagExtended(record, block_id,&rnode, &fork, &blk, NULL);
				#else
				XLogRecGetBlockTag(record, block_id,&rnode, &fork, &blk);
				#endif
				del_xlrec = (xl_heap_delete *) XLogRecGetData(record);

				#if LSNDSP == 1
				// 	XLogRecGetTotalLen(record),
				// 			XLogRecGetXid(record),
				// 			LSN_FORMAT_ARGS(record->ReadRecPtr),
				#endif

				if( rnode.relNode != atoi(targetToastfile) && rnode.relNode != atoi(targetOldToastfile))
					return CONTINUE_RET;

				if(fork != MAIN_FORKNUM)
					return CONTINUE_RET;

				if(	lsnIsReached(record->ReadRecPtr,xl_prev,elem->endLSNforTOAST) )
					return BREAK_RET;

				if ( XLogRecHasBlockImage(record, block_id) )
				{
					if (!RestoreBlockImage(record, block_id, page))
						printf("%s", record->errormsg_buf);
					FPW2File(blk,page,rnode.relNode);
				}
			}
		}
		else{

			char page[BLCKSZ]={0};
			char		toastFilename[MAXPGPATH];
			FILE	   *toastFile;
			BlockNumber blk;
			RelFileNode rnode;
			ForkNumber	fork;

			#if PG_VERSION_NUM > 14
			(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
			#else
			XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
			#endif

			#if LSNDSP == 1
			// XLogRecGetTotalLen(record),
			// 		XLogRecGetXid(record),
			// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
			#endif

			if( rnode.relNode != atoi(targetToastfile) && rnode.relNode != atoi(targetOldToastfile))
				return CONTINUE_RET;

			if(fork != MAIN_FORKNUM)
				return CONTINUE_RET;

			if(	lsnIsReached(record->ReadRecPtr,xl_prev,elem->endLSNforTOAST) )
				return BREAK_RET;

			if ( XLogRecHasBlockImage(record, 0) )
			{
				if (!RestoreBlockImage(record, 0, page))
					printf("%s", record->errormsg_buf);
				FPW2File(blk,page,rnode.relNode);
			}
			else
			{
					if( is_update ){
					heap_xlog_update(record,hot_update,blk,rnode.relNode);
				}
				else if( is_insert ){
					heap_xlog_insert(record,blk,rnode.relNode);
				}
				else if( info == XLOG_HEAP_INPLACE){
					heap_xlog_inplace(record,blk,rnode.relNode);
				}
			}
		}
	}
	else if (rmid == HEAP2_redo)
	{
		for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
		{
			char page[BLCKSZ]={0};
			char		toastFilename[MAXPGPATH];
			FILE	   *toastFile;
			BlockNumber blk;
			RelFileNode rnode;
			ForkNumber	fork;

			#if PG_VERSION_NUM > 14
			(void) XLogRecGetBlockTagExtended(record, block_id,&rnode, &fork, &blk, NULL);
			#else
			XLogRecGetBlockTag(record, block_id,&rnode, &fork, &blk);
			#endif
			sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

			#if LSNDSP == 1
			// XLogRecGetTotalLen(record),
			// 		XLogRecGetXid(record),
			// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
			if(strcmp(lsn,LSNSTR) == 0){
				printf(" ");
			}
			#endif

			if( rnode.relNode != atoi(targetToastfile) && rnode.relNode != atoi(targetOldToastfile))
				return CONTINUE_RET;

			if(fork != MAIN_FORKNUM)
				return CONTINUE_RET;

			if(	lsnIsReached(record->ReadRecPtr,xl_prev,elem->endLSNforTOAST) )
				return BREAK_RET;

			if ( XLogRecHasBlockImage(record, block_id) )
			{
				if (!RestoreBlockImage(record, block_id, page))
					printf("%s", record->errormsg_buf);
				FPW2File(blk,page,rnode.relNode);
			}
			else{
				if(info == XLOG_HEAP2_VISIBLE||info == XLOG_HEAP2_FREEZE_PAGE)
				{

				}
				else if( info == XLOG_HEAP2_VACUUM)
				{
					#if PG_VERSION_NUM < 17
					heap_xlog_vacuum(record,blk,rnode.relNode);
					#else
					heap_xlog_prune(record,blk,rnode.relNode);
					#endif
				}
				else if(info == XLOG_HEAP2_PRUNE)
				{
					heap_xlog_prune(record,blk,rnode.relNode);
				}

				else if( info == (XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE) || info == XLOG_HEAP2_MULTI_INSERT){
					heap_xlog_multi_insert(record,blk,rnode.relNode);
				}
			}
		}
	}
	else if (rmid == BTREE_redo)
	{
		BlockNumber blk;
		RelFileNode rnode;
		ForkNumber	fork;
		#if PG_VERSION_NUM > 14
		(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
		#else
		XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
		#endif
		sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

		#if LSNDSP == 1
		// XLogRecGetTotalLen(record),
		// 		XLogRecGetXid(record),
		// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
		if(strcmp(lsn,LSNSTR) == 0){
			printf(" ");
		}
		#endif
	}
	else if (rmid == XLOG_redo)
	{
		char page[BLCKSZ]={0};
		char		toastFilename[MAXPGPATH];
		FILE	   *toastFile;
		BlockNumber blk;
		RelFileNode rnode;
		ForkNumber	fork;
		#if PG_VERSION_NUM > 14
		(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
		#else
		XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
		#endif
		sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

		#if LSNDSP == 1
		// XLogRecGetTotalLen(record),
		// 		XLogRecGetXid(record),
		// 		LSN_FORMAT_ARGS(record->ReadRecPtr),

		#endif

		if( rnode.relNode != atoi(targetToastfile) && rnode.relNode != atoi(targetOldToastfile))
			return CONTINUE_RET;

		if(fork != MAIN_FORKNUM)
			return CONTINUE_RET;

		if(	lsnIsReached(record->ReadRecPtr,xl_prev,elem->endLSNforTOAST) )
			return BREAK_RET;

		if ( XLogRecHasBlockImage(record, 0) )
		{
			if (!RestoreBlockImage(record, 0, page))
				printf("%s", record->errormsg_buf);
			FPW2File(blk,page,rnode.relNode);
		}

	}
	else if(rmid == TRANSACTION_redo){
		BlockNumber blk;
		RelFileNode rnode;
		ForkNumber	fork;
		#if PG_VERSION_NUM > 14
		(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
		#else
		XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
		#endif
		sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

		#if LSNDSP == 1
		// XLogRecGetTotalLen(record),
		// 		XLogRecGetXid(record),
		// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
		if(strcmp(lsn,LSNSTR) == 0){
			printf(" ");
		}
		#endif
	}
	return CONTINUE_RET;
}

void xact_desc_pg_drop(TimestampTz *TimeFromRecord,Oid *datafileOid,Oid *toastOid, XLogReaderState *record,RmgrId rmid,parray *TxTime_parray)
{

	#if PG_VERSION_NUM == 18
    int kindNum=18;
	#elif PG_VERSION_NUM <18
    int kindNum=17;
	#endif

	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;
	int			block_id = 0;
	BlockNumber blk;
	RelFileNode rnode;
	ForkNumber	fork;
	XlogRecGetBlkInfo(record,block_id, &blk,&rnode, &fork);
	TransactionId currentTx = XLogRecGetXid(record);

	bool writable = 0;
	if(rnode.relNode == 1259){
		XLogRecordRedoDropFPW(sdcPgClass,record);
		if (rmid == HEAP_redo && info == XLOG_HEAP_DELETE && sdcPgClass->xman){
			char *xman = NULL;
			char *oid_pos =NULL;
			int oidoff;
			if( strcmp(sdcPgClass->xman,"NoWayOut") != 0 ){
				xman = strdup(sdcPgClass->xman);
				char *relkind = get_field('\t',xman,kindNum);
				if(strcmp(xman,"NoWayOut") != 0 && strcmp(relkind,"r") == 0 ){
					oid_pos = strchr(xman, '\t');
					if(strncmp(oid_pos+1,"pg_toast",8) != 0){
						oidoff = oid_pos - xman;
						writable = 1;
					}

				}
			}
			if(writable){
				dropElem *elem = (dropElem*)malloc(sizeof(dropElem));
				char *oidstr = get_field('\t',xman,1);
				char *tabname = get_field('\t',xman,2);
				char *filenode  = get_field('\t',xman,8);
				elem->oid = atoi(oidstr);
				elem->filenode =  atoi(filenode);
				elem->Tx = currentTx;
				strcpy(elem->tabname,tabname);
				free(tabname);
				free(oidstr);
				free(filenode);
				FILE *fp = fopen(sdcPgClass->savepath,"a");
				commaStrWriteIntoFileCLASS(sdcPgClass->xman,fp);
				fclose(fp);
				parray_append(TxTime_parray,elem);
			}
			free(xman);
		}
	}
	else if(rnode.relNode == 1249){
		XLogRecordRedoDropFPW(sdcPgAttr,record);
		if (rmid == HEAP_redo && info == XLOG_HEAP_DELETE && sdcPgAttr->xman){
			char *xman = NULL;
			char *oid_pos =NULL;
			int oidoff;
			if(strcmp(sdcPgAttr->xman,"NoWayOut") != 0){
				xman = strdup(sdcPgAttr->xman);
				oid_pos = strchr(xman, '\t');
				if(strcmp(xman,"NoWayOut") != 0 ){
					oidoff = oid_pos - xman;
					writable = 1;
				}
			}
			if(writable){
				char *colname = get_field('\t',xman,2);
				if(!unwantedCol(colname)){
					FILE *fp = fopen(sdcPgAttr->savepath,"a");
					commaStrWriteIntoFIleAttr(sdcPgAttr->xman,fp);
					fclose(fp);
				}
				free(colname);
			}
			free(xman);
		}
	}

	if(rmid == TRANSACTION_redo){
		if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
		{
			xl_xact_commit *xlrec = (xl_xact_commit *) rec;
			xact_desc_commit(TimeFromRecord,datafileOid,toastOid, xlrec,XLogRecGetInfo(record));
			for(int j=0;j<parray_num(TxTime_parray);j++){
				dropElem *elem = parray_get(TxTime_parray,j);
				if(*datafileOid == elem->filenode){
					elem->timestamp = *TimeFromRecord;
				}
			}
		}
	}

}

int XLogRecordRedoDropFPW(systemDropContext *sdc,XLogReaderState *record)
{
	char *savepath = sdc->savepath;
	pg_attributeDesc *allDesc = sdc->allDesc;
	decodeFunc *array2Process = sdc->attr2Process;
	char *tabname = sdc->taboid->tab;
	int addNumLocal = atoi(sdc->taboid->nattr);
	int			block_id;
	XLogRecPtr	xl_prev = XLogRecGetPrev(record);
	RmgrId rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	int is_insert = 0;
	int is_update = 0;
	bool hot_update;

	if( info == XLOG_HEAP_UPDATE || info == (XLOG_HEAP_UPDATE | XLOG_HEAP_INIT_PAGE)){
		is_update = 1;
		hot_update = false;
	}

	else if(info == XLOG_HEAP_HOT_UPDATE || info == (XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE) ){
		is_update = 1;
		hot_update = true;
	}

	else if( info ==  (XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE) || info ==  XLOG_HEAP_INSERT){
		is_insert = 1;
	}

	if(rmid == HEAP_redo)
	{
		if (info == XLOG_HEAP_DELETE)
		{
			for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
			{
				char page[BLCKSZ]={0};
				char		toastFilename[MAXPGPATH];
				FILE	   *toastFile;
				BlockNumber blk;
				RelFileNode rnode;
				ForkNumber	fork;
				xl_heap_delete	*del_xlrec = NULL;

				#if PG_VERSION_NUM > 14
				(void) XLogRecGetBlockTagExtended(record, block_id,&rnode, &fork, &blk, NULL);
				#else
				XLogRecGetBlockTag(record, block_id,&rnode, &fork, &blk);
				#endif
				del_xlrec = (xl_heap_delete *) XLogRecGetData(record);
				sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

				#if LSNDSP == 1
				// 	XLogRecGetTotalLen(record),
				// 			XLogRecGetXid(record),
				// 			LSN_FORMAT_ARGS(record->ReadRecPtr),
				if(strcmp(lsn,LSNSTR) == 0){
					printf(" ");
				}
				#endif

				if ( XLogRecHasBlockImage(record, block_id) )
				{
					if (!RestoreBlockImage(record, block_id, page))
						printf("%s", record->errormsg_buf);
					FPW2File(blk,page,rnode.relNode);
				}

				if(resTyp_there == DELETEtyp){
					sdc->xman = restoreSysDrop(array2Process,del_xlrec,page,blk,record->ReadRecPtr,addNumLocal,savepath,rnode.relNode);
					heap_xlog_delete(record,blk,rnode.relNode);
				}
			}
		}
		else{
			char page[BLCKSZ]={0};
			char		toastFilename[MAXPGPATH];
			FILE	   *toastFile;
			BlockNumber blk;
			RelFileNode rnode;
			ForkNumber	fork;

			#if PG_VERSION_NUM > 14
			(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
			#else
			XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
			#endif
			sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

			#if LSNDSP == 1
			// XLogRecGetTotalLen(record),
			// 		XLogRecGetXid(record),
			// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
			if(strcmp(lsn,LSNSTR) == 0){
				printf(" ");
			}
			#endif

			if ( XLogRecHasBlockImage(record, 0) )
			{
				if (!RestoreBlockImage(record, 0, page))
					printf("%s", record->errormsg_buf);
				FPW2File(blk,page,rnode.relNode);
			}
			else
			{
					if( is_update ){
					heap_xlog_update(record,hot_update,blk,rnode.relNode);
				}
				else if( is_insert ){
					heap_xlog_insert(record,blk,rnode.relNode);
				}
				else if( info == XLOG_HEAP_INPLACE){
					heap_xlog_inplace(record,blk,rnode.relNode);
				}
			}
		}
	}
	else if (rmid == HEAP2_redo)
	{
		for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
		{
			char page[BLCKSZ]={0};
			char		toastFilename[MAXPGPATH];
			FILE	   *toastFile;
			BlockNumber blk;
			RelFileNode rnode;
			ForkNumber	fork;

			#if PG_VERSION_NUM > 14
			(void) XLogRecGetBlockTagExtended(record, block_id,&rnode, &fork, &blk, NULL);
			#else
			XLogRecGetBlockTag(record, block_id,&rnode, &fork, &blk);
			#endif
			sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

			#if LSNDSP == 1
			// XLogRecGetTotalLen(record),
			// 		XLogRecGetXid(record),
			// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
			if(strcmp(lsn,LSNSTR) == 0){
				printf(" ");
			}
			#endif

			if ( XLogRecHasBlockImage(record, block_id) )
			{
				if (!RestoreBlockImage(record, block_id, page))
					printf("%s", record->errormsg_buf);
				FPW2File(blk,page,rnode.relNode);
			}
			else{
				if(info == XLOG_HEAP2_VISIBLE||info == XLOG_HEAP2_FREEZE_PAGE)
				{

				}
				else if( info == XLOG_HEAP2_VACUUM)
					#if PG_VERSION_NUM < 17
					heap_xlog_vacuum(record,blk,rnode.relNode);
					#else
					heap_xlog_prune(record,blk,rnode.relNode);
					#endif
				else if(info == XLOG_HEAP2_PRUNE)
				{
					heap_xlog_prune(record,blk,rnode.relNode);
				}
			}
		}
	}
	else if (rmid == BTREE_redo)
	{
		BlockNumber blk;
		RelFileNode rnode;
		ForkNumber	fork;
		#if PG_VERSION_NUM > 14
		(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
		#else
		XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
		#endif
		sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

		#if LSNDSP == 1
		// XLogRecGetTotalLen(record),
		// 		XLogRecGetXid(record),
		// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
		if(strcmp(lsn,LSNSTR) == 0){
			printf(" ");
		}
		#endif
	}
	else if (rmid == XLOG_redo)
	{
		char page[BLCKSZ]={0};
		char		toastFilename[MAXPGPATH];
		FILE	   *toastFile;
		BlockNumber blk;
		RelFileNode rnode;
		ForkNumber	fork;
		#if PG_VERSION_NUM > 14
		(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
		#else
		XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
		#endif
		sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

		#if LSNDSP == 1
		// XLogRecGetTotalLen(record),
		// 		XLogRecGetXid(record),
		// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
		if(strcmp(lsn,LSNSTR) == 0)
			printf(" ");
		#endif

		if ( XLogRecHasBlockImage(record, 0) )
		{
			if (!RestoreBlockImage(record, 0, page))
				printf("%s", record->errormsg_buf);
			FPW2File(blk,page,rnode.relNode);
		}

	}
	else if(rmid == TRANSACTION_redo){
		BlockNumber blk;
		RelFileNode rnode;
		ForkNumber	fork;
		#if PG_VERSION_NUM > 14
		(void) XLogRecGetBlockTagExtended(record, 0,&rnode, &fork, &blk, NULL);
		#else
		XLogRecGetBlockTag(record, 0,&rnode, &fork, &blk);
		#endif
		sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

		#if LSNDSP == 1
		// XLogRecGetTotalLen(record),
		// 		XLogRecGetXid(record),
		// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
		if(strcmp(lsn,LSNSTR) == 0){
			printf(" ");
		}
		#endif
	}
	return CONTINUE_RET;
}

void XLogScanRecordForDisplay(XLogDumpConfig *config, XLogReaderState *record,parray *TxTime_parray)
{
	const char *id;
	RmgrId rmid = XLogRecGetRmid(record);
	uint32		rec_len;
	uint32		fpi_len;
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blk;
	int			block_id=0;
	uint8		info = XLogRecGetInfo(record);
	XLogRecPtr	xl_prev = XLogRecGetPrev(record);
	StringInfoData s;

	XLogDumpRecordLen(record, &rec_len, &fpi_len);
	#if PG_VERSION_NUM > 14
	(void) XLogRecGetBlockTagExtended(record, block_id,&rnode, &forknum, &blk, NULL);
	#else
	XLogRecGetBlockTag(record, block_id,&rnode, &forknum, &blk);
	#endif
	sprintf(lsn,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr));

	#if LSNDSP == 1
	// XLogRecGetTotalLen(record),
	// 		XLogRecGetXid(record),
	// 		LSN_FORMAT_ARGS(record->ReadRecPtr),
	if(strcmp(lsn,LSNSTR) == 0){
		printf(" ");
	}
	#endif

	char LSN[50];
	sprintf(LSN,"%X/%08X",LSN_FORMAT_ARGS(record->ReadRecPtr),LSN_FORMAT_ARGS(xl_prev));

	TransactionId tx = XLogRecGetXid(record);

	Oid *datafileOid=NULL;
	Oid *toastOid=NULL;
	TimestampTz *TimeFromRecord = NULL;

	TimeFromRecord= (TimestampTz *)malloc(sizeof(TimestampTz));
	datafileOid = (Oid *)malloc(sizeof(Oid));
	toastOid = (Oid *)malloc(sizeof(Oid));
	memset(datafileOid,0,sizeof(Oid));
	memset(TimeFromRecord,0,sizeof(TimestampTz));
	memset(toastOid,0,sizeof(Oid));

	if (delOrDrop == DROP){
		xact_desc_pg_drop(TimeFromRecord,datafileOid,toastOid,record,rmid,TxTime_parray);
	}
	else{
		if(rmid == TRANSACTION_redo || rmid == HEAP_redo){
			if (delOrDrop == DEL && resTyp_there == DELETEtyp){
				xact_desc_pg_del(TimeFromRecord,datafileOid,record,rmid);
			}
			else if (delOrDrop == DEL && resTyp_there == UPDATEtyp){
				xact_desc_pg_upd(TimeFromRecord,datafileOid,record,rmid);
			}

			if(delOrDrop == DEL && ( *datafileOid == atoi(targetDatafile) || *datafileOid == atoi(targetOldDatafile))){

					int txFound = harray_search(delElems,HARRAYDEL,tx);
				if(txFound){
					DELstruct *elem = harray_get(delElems,HARRAYDEL,tx);
					elem->delCount++;
				}
				else{
					DELstruct *elem = (DELstruct*)malloc(sizeof(DELstruct));
					strcpy(elem->startLSN,LSN);
					strcpy(elem->startLSNforTOAST,LSN);
					strcpy(elem->startwal,currWalName);
					elem->tx=tx;
					elem->delCount=1;
					if(strcmp(elemforTime->startLSN,"") == 0){
						strcpy(elemforTime->startLSN,elem->startLSN);
						strcpy(elemforTime->startLSNforTOAST,elem->startLSNforTOAST);
						strcpy(elemforTime->startwal,elem->startwal);
					}
					harray_append(delElems,HARRAYDEL,elem,tx);
				}
			}
			else if(delOrDrop == DEL && *TimeFromRecord != 0){
				int txFound = harray_search(delElems,HARRAYDEL,tx);
				if(txFound){
					parray_append(Txs,(void *)(intptr_t)tx);
					DELstruct *elem = harray_get(delElems,HARRAYDEL,tx);
					strcpy(elem->endLSN,LSN);
					strcpy(elem->endLSNforTOAST,LSN);
					strcpy(elem->endwal,currWalName);
					elem->txtime = *TimeFromRecord;

					strcpy(elemforTime->endLSN,elem->endLSN);
					strcpy(elemforTime->endLSNforTOAST,elem->endLSNforTOAST);
					strcpy(elemforTime->endwal,elem->endwal);
					elemforTime->txtime = elem->txtime;
					elemforTime->delCount+=elem->delCount;
				}
			}
		}
	}
}

/**
 * WALDumpOpenSegment - Open WAL segment file
 *
 * @segcxt:    Segment context
 * @nextSegNo: Next segment number
 * @tli_p:     Timeline pointer
 *
 * Opens the specified WAL segment file for reading.
 */
void WALDumpOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo,TimeLineID *tli_p)
{
	TimeLineID	tli = *tli_p;
	char		fname[MAXPGPATH];
	int			tries;

	XLogFileName(fname, tli, nextSegNo, state->segcxt.ws_segsize);

	/*
	 * In follow mode there is a short period of time after the server has
	 * written the end of the previous file before the new file is available.
	 * So we loop for 5 seconds looking for the file to appear before giving
	 * up.
	 */
	for (tries = 0; tries < 1; tries++)
	{
		state->seg.ws_file = open_file_in_directory(state->segcxt.ws_dir, fname);
		if (state->seg.ws_file >= 0){
			return;
		}
		/* Any other error, fall through and fail */
		break;
	}

}

bool WALRead(XLogReaderState *state,char *buf, XLogRecPtr startptr, Size count, TimeLineID tli,WALReadError *errinfo)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;

	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;

		startoff = XLogSegmentOffset(recptr, state->segcxt.ws_segsize);

		/*
		 * If the data we want is not in a segment we have open, close what we
		 * have (if anything) and open the next one, using the caller's
		 * provided openSegment callback.
		 */
		if (state->seg.ws_file < 0 ||
			!XLByteInSeg(recptr, state->seg.ws_segno, state->segcxt.ws_segsize) ||
			tli != state->seg.ws_tli)
		{
			XLogSegNo	nextSegNo;

			if (state->seg.ws_file >= 0)
				state->routine.segment_close(state);

			XLByteToSeg(recptr, nextSegNo, state->segcxt.ws_segsize);
			state->routine.segment_open(state, nextSegNo, &tli);

			Assert(state->seg.ws_file >= 0);

			/* Update the current segment info. */
			state->seg.ws_tli = tli;
			state->seg.ws_segno = nextSegNo;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (state->segcxt.ws_segsize - startoff))
			segbytes = state->segcxt.ws_segsize - startoff;
		else
			segbytes = nbytes;

		/* Reset errno first; eases reporting non-errno-affecting errors */
		// #if defined(_linux_)

		readbytes = pg_pread(state->seg.ws_file, p, segbytes, (off_t) startoff);

		if (readbytes <= 0)
		{
			errinfo->wre_req = segbytes;
			errinfo->wre_read = readbytes;
			errinfo->wre_off = startoff;
			errinfo->wre_seg = state->seg;
			return false;
		}

		/* Update state for read */
		recptr += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}

	return true;
}

/**
 * WALDumpReadPage - Read page from WAL file
 *
 * @state:          XLog reader state
 * @targetPagePtr:  Target page LSN
 * @reqLen:         Required length
 * @targetRecPtr:   Target record LSN
 * @readBuf:        Read buffer
 *
 * Reads a page from WAL file into buffer.
 *
 * Returns: Number of bytes read
 */
int WALDumpReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,XLogRecPtr targetPtr, char *readBuff)
{
	XLogDumpPrivate *private = state->private_data;
	int			count = XLOG_BLCKSZ;
	WALReadError errinfo;

	if (private->endptr != InvalidXLogRecPtr)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= private->endptr)
			count = XLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= private->endptr)
			count = private->endptr - targetPagePtr;
		else
		{
			private->endptr_reached = true;
			return -1;
		}
	}

	if (!WALRead(state, readBuff, targetPagePtr, count, private->timeline,
				 &errinfo))
	{
		WALOpenSegment *seg = &errinfo.wre_seg;
		char		fname[MAXPGPATH];

		XLogFileName(fname, seg->ws_tli, seg->ws_segno,
					 state->segcxt.ws_segsize);

		if (errinfo.wre_errno != 0)
		{
		}
		else{

		}
			// 			fname, errinfo.wre_off, errinfo.wre_read,
	}

	return count;
}

void WALDumpCloseSegment(XLogReaderState *state)
{
	close(state->seg.ws_file);
	/* need to check errno? */
	state->seg.ws_file = -1;
}

int ReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen)
{
	int			readLen;
	uint32		targetPageOff;
	XLogSegNo	targetSegNo;
	XLogPageHeader hdr;

	Assert((pageptr % XLOG_BLCKSZ) == 0);

	XLByteToSeg(pageptr, targetSegNo, state->segcxt.ws_segsize);
	targetPageOff = XLogSegmentOffset(pageptr, state->segcxt.ws_segsize);

	/* check whether we have all the requested data already */
	if (targetSegNo == state->seg.ws_segno &&
		targetPageOff == state->segoff && reqLen <= state->readLen)
		return state->readLen;

	if (targetSegNo != state->seg.ws_segno && targetPageOff != 0)
	{
		XLogRecPtr	targetSegmentPtr = pageptr - targetPageOff;

		readLen = state->routine.page_read(state, targetSegmentPtr, XLOG_BLCKSZ,
										   state->currRecPtr,
										   state->readBuf);
		if (readLen < 0)
			goto err;

		/* we can be sure to have enough WAL available, we scrolled back */
		Assert(readLen == XLOG_BLCKSZ);

		if (!XLogReaderValidatePageHeader(state, targetSegmentPtr,
										  state->readBuf))
			goto err;
	}

	/*
	 * First, read the requested data length, but at least a short page header
	 * so that we can validate it.
	 */
	readLen = state->routine.page_read(state, pageptr, Max(reqLen, SizeOfXLogShortPHD),
									   state->currRecPtr,
									   state->readBuf);
	if (readLen < 0)
		goto err;

	Assert(readLen <= XLOG_BLCKSZ);

	/* Do we have enough data to check the header length? */
	if (readLen <= SizeOfXLogShortPHD)
		goto err;

	Assert(readLen >= reqLen);

	hdr = (XLogPageHeader) state->readBuf;

	/* still not enough */
	if (readLen < XLogPageHeaderSize(hdr))
	{
		readLen = state->routine.page_read(state, pageptr, XLogPageHeaderSize(hdr),
										   state->currRecPtr,
										   state->readBuf);
		if (readLen < 0)
			goto err;
	}

	/*
	 * Now that we know we have the full header, validate it.
	 */

	if (!XLogReaderValidatePageHeader(state, pageptr, (char *) hdr))
		goto err;

	/* update read state information */
	state->seg.ws_segno = targetSegNo;
	state->segoff = targetPageOff;
	state->readLen = readLen;

	return readLen;

err:
	return -1;
}

#if PG_VERSION_NUM > 14
size_t DecodeXLogRecordRequiredSpace(size_t xl_tot_len)
{
	size_t		size = 0;

	/* Account for the fixed size part of the decoded record struct. */
	size += offsetof(DecodedXLogRecord, blocks[0]);
	/* Account for the flexible blocks array of maximum possible size. */
	size += sizeof(DecodedBkpBlock) * (XLR_MAX_BLOCK_ID + 1);
	/* Account for all the raw main and block data. */
	size += xl_tot_len;
	/* We might insert padding before main_data. */
	size += (MAXIMUM_ALIGNOF - 1);
	/* We might insert padding before each block's data. */
	size += (MAXIMUM_ALIGNOF - 1) * (XLR_MAX_BLOCK_ID + 1);
	/* We might insert padding at the end. */
	size += (MAXIMUM_ALIGNOF - 1);

	return size;
}

static DecodedXLogRecord *
XLogReadRecordAlloc(XLogReaderState *state, size_t xl_tot_len, bool allow_oversized)
{
	size_t		required_space = DecodeXLogRecordRequiredSpace(xl_tot_len);
	DecodedXLogRecord *decoded = NULL;

	/* Allocate a circular decode buffer if we don't have one already. */
	if (unlikely(state->decode_buffer == NULL))
	{
		if (state->decode_buffer_size == 0)
			state->decode_buffer_size = DEFAULT_DECODE_BUFFER_SIZE;
		state->decode_buffer = malloc(state->decode_buffer_size);
		state->decode_buffer_head = state->decode_buffer;
		state->decode_buffer_tail = state->decode_buffer;
		state->free_decode_buffer = true;
	}

	/* Try to allocate space in the circular decode buffer. */
	if (state->decode_buffer_tail >= state->decode_buffer_head)
	{
		/* Empty, or tail is to the right of head. */
		if (required_space <=
			state->decode_buffer_size -
			(state->decode_buffer_tail - state->decode_buffer))
		{
			/*-
			 * There is space between tail and end.
			 *
			 * +-----+--------------------+-----+
			 * |     |////////////////////|here!|
			 * +-----+--------------------+-----+
			 *       ^                    ^
			 *       |                    |
			 *       h                    t
			 */
			decoded = (DecodedXLogRecord *) state->decode_buffer_tail;
			decoded->oversized = false;
			return decoded;
		}
		else if (required_space <
				 state->decode_buffer_head - state->decode_buffer)
		{
			/*-
			 * There is space between start and head.
			 *
			 * +-----+--------------------+-----+
			 * |here!|////////////////////|     |
			 * +-----+--------------------+-----+
			 *       ^                    ^
			 *       |                    |
			 *       h                    t
			 */
			decoded = (DecodedXLogRecord *) state->decode_buffer;
			decoded->oversized = false;
			return decoded;
		}
	}
	else
	{
		/* Tail is to the left of head. */
		if (required_space <
			state->decode_buffer_head - state->decode_buffer_tail)
		{
			/*-
			 * There is space between tail and head.
			 *
			 * +-----+--------------------+-----+
			 * |/////|here!               |/////|
			 * +-----+--------------------+-----+
			 *       ^                    ^
			 *       |                    |
			 *       t                    h
			 */
			decoded = (DecodedXLogRecord *) state->decode_buffer_tail;
			decoded->oversized = false;
			return decoded;
		}
	}

	/* Not enough space in the decode buffer.  Are we allowed to allocate? */
	if (allow_oversized)
	{
		decoded = malloc(required_space);
		decoded->oversized = true;
		return decoded;
	}

	return NULL;
}

XLogPageReadResult XLogDecodeNextRecord(XLogReaderState *state, bool nonblocking)
{
	XLogRecPtr	RecPtr;
	XLogRecord *record;
	XLogRecPtr	targetPagePtr;
	bool		randAccess;
	uint32		len,
				total_len;
	uint32		targetRecOff;
	uint32		pageHeaderSize;
	bool		assembled;
	bool		gotheader;
	int			readOff;
	DecodedXLogRecord *decoded;
	char	   *errormsg;		/* not used */

	/*
	 * randAccess indicates whether to verify the previous-record pointer of
	 * the record we're reading.  We only do this if we're reading
	 * sequentially, which is what we initially assume.
	 */
	randAccess = false;

	/* reset error state */
	state->errormsg_buf[0] = '\0';
	decoded = NULL;

	state->abortedRecPtr = InvalidXLogRecPtr;
	state->missingContrecPtr = InvalidXLogRecPtr;

	RecPtr = state->NextRecPtr;

	if (state->DecodeRecPtr != InvalidXLogRecPtr)
	{
		/* read the record after the one we just read */

		/*
		 * NextRecPtr is pointing to end+1 of the previous WAL record.  If
		 * we're at a page boundary, no more records can fit on the current
		 * page. We must skip over the page header, but we can't do that until
		 * we've read in the page, since the header size is variable.
		 */
	}
	else
	{
		/*
		 * Caller supplied a position to start at.
		 *
		 * In this case, NextRecPtr should already be pointing either to a
		 * valid record starting position or alternatively to the beginning of
		 * a page. See the header comments for XLogBeginRead.
		 */
		Assert(RecPtr % XLOG_BLCKSZ == 0 || XRecOffIsValid(RecPtr));
		randAccess = true;
	}

restart:
	state->nonblocking = nonblocking;
	state->currRecPtr = RecPtr;
	assembled = false;

	targetPagePtr = RecPtr - (RecPtr % XLOG_BLCKSZ);
	targetRecOff = RecPtr % XLOG_BLCKSZ;

	/*
	 * Read the page containing the record into state->readBuf. Request enough
	 * byte to cover the whole record header, or at least the part of it that
	 * fits on the same page.
	 */
	readOff = ReadPageInternal(state, targetPagePtr,
							   Min(targetRecOff + SizeOfXLogRecord, XLOG_BLCKSZ));
	if (readOff == XLREAD_WOULDBLOCK)
		return XLREAD_WOULDBLOCK;
	else if (readOff < 0)
		goto err;

	/*
	 * ReadPageInternal always returns at least the page header, so we can
	 * examine it now.
	 */
	pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) state->readBuf);
	if (targetRecOff == 0)
	{
		/*
		 * At page start, so skip over page header.
		 */
		RecPtr += pageHeaderSize;
		targetRecOff = pageHeaderSize;
	}
	else if (targetRecOff < pageHeaderSize)
	{
		printf("invalid record offset at %X/%X (expected >=%u, got %u)\n",
							  LSN_FORMAT_ARGS(RecPtr),
							  pageHeaderSize, targetRecOff);
		goto err;
	}

	if ((((XLogPageHeader) state->readBuf)->xlp_info & XLP_FIRST_IS_CONTRECORD) &&
		targetRecOff == pageHeaderSize)
	{
		printf("contrecord is requested by %X/%X\n",
							  LSN_FORMAT_ARGS(RecPtr));
		goto err;
	}

	/* ReadPageInternal has verified the page header */
	Assert(pageHeaderSize <= readOff);

	/*
	 * Read the record length.
	 *
	 * NB: Even though we use an XLogRecord pointer here, the whole record
	 * header might not fit on this page. xl_tot_len is the first field of the
	 * struct, so it must be on this page (the records are MAXALIGNed), but we
	 * cannot access any other fields until we've verified that we got the
	 * whole header.
	 */
	record = (XLogRecord *) (state->readBuf + RecPtr % XLOG_BLCKSZ);
	total_len = record->xl_tot_len;

	/*
	 * If the whole record header is on this page, validate it immediately.
	 * Otherwise do just a basic sanity check on xl_tot_len, and validate the
	 * rest of the header after reading it from the next page.  The xl_tot_len
	 * check is necessary here to ensure that we enter the "Need to reassemble
	 * record" code path below; otherwise we might fail to apply
	 * ValidXLogRecordHeader at all.
	 */
	if (targetRecOff <= XLOG_BLCKSZ - SizeOfXLogRecord)
	{
		if (!ValidXLogRecordHeader(state, RecPtr, state->DecodeRecPtr, record,
								   randAccess))
			goto err;
		gotheader = true;
	}
	else
	{
		/* There may be no next page if it's too small. */
		if (total_len < SizeOfXLogRecord)
		{

			printf(
#ifdef CN
								  "读取到最后的日志段： %X/%X\n",
#else
								  "read until FINAL WAL SEGMENT: %X/%X\n",
#endif
								  LSN_FORMAT_ARGS(RecPtr),
								  (uint32) SizeOfXLogRecord, total_len);
			goto err;
		}
		/* We'll validate the header once we have the next page. */
		gotheader = false;
	}

	/*
	 * Try to find space to decode this record, if we can do so without
	 * calling palloc.  If we can't, we'll try again below after we've
	 * validated that total_len isn't garbage bytes from a recycled WAL page.
	 */
	decoded = XLogReadRecordAlloc(state,
								  total_len,
								  false /* allow_oversized */ );
	if (decoded == NULL && nonblocking)
	{
		/*
		 * There is no space in the circular decode buffer, and the caller is
		 * only reading ahead.  The caller should consume existing records to
		 * make space.
		 */
		return XLREAD_WOULDBLOCK;
	}

	len = XLOG_BLCKSZ - RecPtr % XLOG_BLCKSZ;
	if (total_len > len)
	{
		/* Need to reassemble record */
		char	   *contdata;
		XLogPageHeader pageHeader;
		char	   *buffer;
		uint32		gotlen;

		assembled = true;

		/*
		 * We always have space for a couple of pages, enough to validate a
		 * boundary-spanning record header.
		 */
		Assert(state->readRecordBufSize >= XLOG_BLCKSZ * 2);
		Assert(state->readRecordBufSize >= len);

		/* Copy the first fragment of the record from the first page. */
		memcpy(state->readRecordBuf,
			   state->readBuf + RecPtr % XLOG_BLCKSZ, len);
		buffer = state->readRecordBuf + len;
		gotlen = len;

		do
		{
			/* Calculate pointer to beginning of next page */
			targetPagePtr += XLOG_BLCKSZ;

			/* Wait for the next page to become available */
			readOff = ReadPageInternal(state, targetPagePtr,
									   Min(total_len - gotlen + SizeOfXLogShortPHD,
										   XLOG_BLCKSZ));

			if (readOff == XLREAD_WOULDBLOCK)
				return XLREAD_WOULDBLOCK;
			else if (readOff < 0)
				goto err;

			Assert(SizeOfXLogShortPHD <= readOff);

			pageHeader = (XLogPageHeader) state->readBuf;

			/*
			 * If we were expecting a continuation record and got an
			 * "overwrite contrecord" flag, that means the continuation record
			 * was overwritten with a different record.  Restart the read by
			 * assuming the address to read is the location where we found
			 * this flag; but keep track of the LSN of the record we were
			 * reading, for later verification.
			 */
			if (pageHeader->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD)
			{
				state->overwrittenRecPtr = RecPtr;
				RecPtr = targetPagePtr;
				goto restart;
			}

			/* Check that the continuation on next page looks valid */
			if (!(pageHeader->xlp_info & XLP_FIRST_IS_CONTRECORD))
			{
				printf(
									  "there is no contrecord flag at %X/%X",
									  LSN_FORMAT_ARGS(RecPtr));
				goto err;
			}

			/*
			 * Cross-check that xlp_rem_len agrees with how much of the record
			 * we expect there to be left.
			 */
			if (pageHeader->xlp_rem_len == 0 ||
				total_len != (pageHeader->xlp_rem_len + gotlen))
			{
				printf(
									  "invalid contrecord length %u (expected %lld) at %X/%X",
									  pageHeader->xlp_rem_len,
									  ((long long) total_len) - gotlen,
									  LSN_FORMAT_ARGS(RecPtr));
				goto err;
			}

			/* Append the continuation from this page to the buffer */
			pageHeaderSize = XLogPageHeaderSize(pageHeader);

			if (readOff < pageHeaderSize)
				readOff = ReadPageInternal(state, targetPagePtr,
										   pageHeaderSize);

			Assert(pageHeaderSize <= readOff);

			contdata = (char *) state->readBuf + pageHeaderSize;
			len = XLOG_BLCKSZ - pageHeaderSize;
			if (pageHeader->xlp_rem_len < len)
				len = pageHeader->xlp_rem_len;

			if (readOff < pageHeaderSize + len)
				readOff = ReadPageInternal(state, targetPagePtr,
										   pageHeaderSize + len);

			memcpy(buffer, (char *) contdata, len);
			buffer += len;
			gotlen += len;

			/* If we just reassembled the record header, validate it. */
			if (!gotheader)
			{
				record = (XLogRecord *) state->readRecordBuf;
				if (!ValidXLogRecordHeader(state, RecPtr, state->DecodeRecPtr,
										   record, randAccess))
					goto err;
				gotheader = true;
			}

			/*
			 * We might need a bigger buffer.  We have validated the record
			 * header, in the case that it split over a page boundary.  We've
			 * also cross-checked total_len against xlp_rem_len on the second
			 * page, and verified xlp_pageaddr on both.
			 */
			if (total_len > state->readRecordBufSize)
			{
				char		save_copy[XLOG_BLCKSZ * 2];

				/*
				 * Save and restore the data we already had.  It can't be more
				 * than two pages.
				 */
				Assert(gotlen <= lengthof(save_copy));
				Assert(gotlen <= state->readRecordBufSize);
				memcpy(save_copy, state->readRecordBuf, gotlen);
				allocate_recordbuf(state, total_len);
				memcpy(state->readRecordBuf, save_copy, gotlen);
				buffer = state->readRecordBuf + gotlen;
			}
		} while (gotlen < total_len);
		Assert(gotheader);

		record = (XLogRecord *) state->readRecordBuf;

		pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) state->readBuf);
		state->DecodeRecPtr = RecPtr;
		state->NextRecPtr = targetPagePtr + pageHeaderSize
			+ MAXALIGN(pageHeader->xlp_rem_len);
	}
	else
	{
		/* Wait for the record data to become available */
		readOff = ReadPageInternal(state, targetPagePtr,
								   Min(targetRecOff + total_len, XLOG_BLCKSZ));
		if (readOff == XLREAD_WOULDBLOCK)
			return XLREAD_WOULDBLOCK;
		else if (readOff < 0)
			goto err;

		/* Record does not cross a page boundary */

		state->NextRecPtr = RecPtr + MAXALIGN(total_len);

		state->DecodeRecPtr = RecPtr;
	}

	/*
	 * Special processing if it's an XLOG SWITCH record
	 */
	if (record->xl_rmid == 0 &&
		(record->xl_info & ~XLR_INFO_MASK) == XLOG_SWITCH)
	{
		/* Pretend it extends to end of segment */
		state->NextRecPtr += state->segcxt.ws_segsize - 1;
		state->NextRecPtr -= XLogSegmentOffset(state->NextRecPtr, state->segcxt.ws_segsize);
	}

	/*
	 * If we got here without a DecodedXLogRecord, it means we needed to
	 * validate total_len before trusting it, but by now now we've done that.
	 */
	if (decoded == NULL)
	{
		Assert(!nonblocking);
		decoded = XLogReadRecordAlloc(state,
									  total_len,
									  true /* allow_oversized */ );
		/* allocation should always happen under allow_oversized */
		Assert(decoded != NULL);
	}

	if (DecodeXLogRecord(state, decoded, record, RecPtr, &errormsg))
	{
		/* Record the location of the next record. */
		decoded->next_lsn = state->NextRecPtr;

		/*
		 * If it's in the decode buffer, mark the decode buffer space as
		 * occupied.
		 */
		if (!decoded->oversized)
		{
			/* The new decode buffer head must be MAXALIGNed. */
			Assert(decoded->size == MAXALIGN(decoded->size));
			if ((char *) decoded == state->decode_buffer)
				state->decode_buffer_tail = state->decode_buffer + decoded->size;
			else
				state->decode_buffer_tail += decoded->size;
		}

		/* Insert it into the queue of decoded records. */
		Assert(state->decode_queue_tail != decoded);
		if (state->decode_queue_tail)
			state->decode_queue_tail->next = decoded;
		state->decode_queue_tail = decoded;
		if (!state->decode_queue_head)
			state->decode_queue_head = decoded;
		return XLREAD_SUCCESS;
	}

err:
	if (assembled)
	{
		/*
		 * We get here when a record that spans multiple pages needs to be
		 * assembled, but something went wrong -- perhaps a contrecord piece
		 * was lost.  If caller is WAL replay, it will know where the aborted
		 * record was and where to direct followup WAL to be written, marking
		 * the next piece with XLP_FIRST_IS_OVERWRITE_CONTRECORD, which will
		 * in turn signal downstream WAL consumers that the broken WAL record
		 * is to be ignored.
		 */
		state->abortedRecPtr = RecPtr;
		state->missingContrecPtr = targetPagePtr;

		/*
		 * If we got here without reporting an error, make sure an error is
		 * queued so that XLogPrefetcherReadRecord() doesn't bring us back a
		 * second time and clobber the above state.
		 */
		state->errormsg_deferred = true;
	}

	if (decoded && decoded->oversized)
		free(decoded);

	/*
	 * Invalidate the read state. We might read from a different source after
	 * failure.
	 */

	/*
	 * If an error was written to errmsg_buf, it'll be returned to the caller
	 * of XLogReadRecord() after all successfully decoded records from the
	 * read queue.
	 */

	return XLREAD_FAIL;
}

DecodedXLogRecord *XLogReadAhead(XLogReaderState *state, bool nonblocking)
{
	XLogPageReadResult result;

	if (state->errormsg_deferred)
		return NULL;

	result = XLogDecodeNextRecord(state, nonblocking);
	if (result == XLREAD_SUCCESS)
	{
		Assert(state->decode_queue_tail != NULL);
		return state->decode_queue_tail;
	}

	return NULL;
}

bool XLogReaderHasQueuedRecordOrError(XLogReaderState *state)
{
	return (state->decode_queue_head != NULL) || state->errormsg_deferred;
}

XLogRecPtr XLogReleasePreviousRecord(XLogReaderState *state)
{
	DecodedXLogRecord *record;
	XLogRecPtr	next_lsn;

	if (!state->record)
		return InvalidXLogRecPtr;

	/*
	 * Remove it from the decoded record queue.  It must be the oldest item
	 * decoded, decode_queue_head.
	 */
	record = state->record;
	next_lsn = record->next_lsn;
	Assert(record == state->decode_queue_head);
	state->record = NULL;
	state->decode_queue_head = record->next;

	/* It might also be the newest item decoded, decode_queue_tail. */
	if (state->decode_queue_tail == record)
		state->decode_queue_tail = NULL;

	/* Release the space. */
	if (unlikely(record->oversized))
	{
		/* It's not in the decode buffer, so free it to release space. */
		free(record);
	}
	else
	{
		/* It must be the head (oldest) record in the decode buffer. */
		Assert(state->decode_buffer_head == (char *) record);

		/*
		 * We need to update head to point to the next record that is in the
		 * decode buffer, if any, being careful to skip oversized ones
		 * (they're not in the decode buffer).
		 */
		record = record->next;
		while (unlikely(record && record->oversized))
			record = record->next;

		if (record)
		{
			/* Adjust head to release space up to the next record. */
			state->decode_buffer_head = (char *) record;
		}
		else
		{
			/*
			 * Otherwise we might as well just reset head and tail to the
			 * start of the buffer space, because we're empty.  This means
			 * we'll keep overwriting the same piece of memory if we're not
			 * doing any prefetching.
			 */
			state->decode_buffer_head = state->decode_buffer;
			state->decode_buffer_tail = state->decode_buffer;
		}
	}

	return next_lsn;
}

DecodedXLogRecord *
XLogNextRecord(XLogReaderState *state, char **errormsg)
{
	/* Release the last record returned by XLogNextRecord(). */
	XLogReleasePreviousRecord(state);

	if (state->decode_queue_head == NULL)
	{
		*errormsg = NULL;
		if (state->errormsg_deferred)
		{
			if (state->errormsg_buf[0] != '\0')
				*errormsg = state->errormsg_buf;
			state->errormsg_deferred = false;
		}

		/*
		 * state->EndRecPtr is expected to have been set by the last call to
		 * XLogBeginRead() or XLogNextRecord(), and is the location of the
		 * error.
		 */
		Assert(!XLogRecPtrIsInvalid(state->EndRecPtr));

		return NULL;
	}

	/*
	 * Record this as the most recent record returned, so that we'll release
	 * it next time.  This also exposes it to the traditional
	 * XLogRecXXX(xlogreader) macros, which work with the decoder rather than
	 * the record for historical reasons.
	 */
	state->record = state->decode_queue_head;

	/*
	 * Update the pointers to the beginning and one-past-the-end of this
	 * record, again for the benefit of historical code that expected the
	 * decoder to track this rather than accessing these fields of the record
	 * itself.
	 */
	state->ReadRecPtr = state->record->lsn;
	state->EndRecPtr = state->record->next_lsn;

	*errormsg = NULL;

	return state->record;
}

XLogRecord *XLogReadRecord(XLogReaderState *state, char **errormsg)
{
	DecodedXLogRecord *decoded;

	/*
	 * Release last returned record, if there is one.  We need to do this so
	 * that we can check for empty decode queue accurately.
	 */
	XLogReleasePreviousRecord(state);

	/*
	 * Call XLogReadAhead() in blocking mode to make sure there is something
	 * in the queue, though we don't use the result.
	 */
	if (!XLogReaderHasQueuedRecordOrError(state))
		XLogReadAhead(state, false /* nonblocking */ );

	/* Consume the head record or error. */
	decoded = XLogNextRecord(state, errormsg);
	if (decoded)
	{
		/*
		 * This function returns a pointer to the record's header, not the
		 * actual decoded record.  The caller will access the decoded record
		 * through the XLogRecGetXXX() macros, which reach the decoded
		 * recorded as xlogreader->record.
		 */
		Assert(state->record == decoded);
		return &decoded->header;
	}

	return NULL;
}
#else
XLogRecord *XLogReadRecord(XLogReaderState *state, char **errormsg)
{
	XLogRecPtr	RecPtr;
	XLogRecord *record;
	XLogRecPtr	targetPagePtr;
	bool		randAccess;
	uint32		len,
				total_len;
	uint32		targetRecOff;
	uint32		pageHeaderSize;
	bool		assembled;
	bool		gotheader;
	int			readOff;

	/*
	 * randAccess indicates whether to verify the previous-record pointer of
	 * the record we're reading.  We only do this if we're reading
	 * sequentially, which is what we initially assume.
	 */
	randAccess = false;

	/* reset error state */
	*errormsg = NULL;
	state->errormsg_buf[0] = '\0';

	ResetDecoder(state);
	state->abortedRecPtr = InvalidXLogRecPtr;
	state->missingContrecPtr = InvalidXLogRecPtr;

	RecPtr = state->EndRecPtr;

	if (state->ReadRecPtr != InvalidXLogRecPtr)
	{
		/* read the record after the one we just read */

		/*
		 * EndRecPtr is pointing to end+1 of the previous WAL record.  If
		 * we're at a page boundary, no more records can fit on the current
		 * page. We must skip over the page header, but we can't do that until
		 * we've read in the page, since the header size is variable.
		 */
	}
	else
	{
		/*
		 * Caller supplied a position to start at.
		 *
		 * In this case, EndRecPtr should already be pointing to a valid
		 * record starting position.
		 */
		Assert(XRecOffIsValid(RecPtr));
		randAccess = true;
	}

restart:
	state->currRecPtr = RecPtr;
	assembled = false;

	targetPagePtr = RecPtr - (RecPtr % XLOG_BLCKSZ);
	targetRecOff = RecPtr % XLOG_BLCKSZ;

	/*
	 * Read the page containing the record into state->readBuf. Request enough
	 * byte to cover the whole record header, or at least the part of it that
	 * fits on the same page.
	 */
	readOff = ReadPageInternal(state, targetPagePtr,
							   Min(targetRecOff + SizeOfXLogRecord, XLOG_BLCKSZ));
	if (readOff < 0)
		goto err;

	/*
	 * ReadPageInternal always returns at least the page header, so we can
	 * examine it now.
	 */
	pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) state->readBuf);
	if (targetRecOff == 0)
	{
		/*
		 * At page start, so skip over page header.
		 */
		RecPtr += pageHeaderSize;
		targetRecOff = pageHeaderSize;
	}
	else if (targetRecOff < pageHeaderSize)
	{
		printf("invalid record offset at %X/%X",
							  LSN_FORMAT_ARGS(RecPtr));
		goto err;
	}

	if ((((XLogPageHeader) state->readBuf)->xlp_info & XLP_FIRST_IS_CONTRECORD) &&
		targetRecOff == pageHeaderSize)
	{
		printf("contrecord is requested by %X/%X",
							  LSN_FORMAT_ARGS(RecPtr));
		goto err;
	}

	/* ReadPageInternal has verified the page header */
	Assert(pageHeaderSize <= readOff);

	/*
	 * Read the record length.
	 *
	 * NB: Even though we use an XLogRecord pointer here, the whole record
	 * header might not fit on this page. xl_tot_len is the first field of the
	 * struct, so it must be on this page (the records are MAXALIGNed), but we
	 * cannot access any other fields until we've verified that we got the
	 * whole header.
	 */
	record = (XLogRecord *) (state->readBuf + RecPtr % XLOG_BLCKSZ);
	total_len = record->xl_tot_len;

	/*
	 * If the whole record header is on this page, validate it immediately.
	 * Otherwise do just a basic sanity check on xl_tot_len, and validate the
	 * rest of the header after reading it from the next page.  The xl_tot_len
	 * check is necessary here to ensure that we enter the "Need to reassemble
	 * record" code path below; otherwise we might fail to apply
	 * ValidXLogRecordHeader at all.
	 */
	if (targetRecOff <= XLOG_BLCKSZ - SizeOfXLogRecord)
	{
		if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr, record,
								   randAccess))
			goto err;
		gotheader = true;
	}
	else
	{
		/* XXX: more validation should be done here */
		if (total_len < SizeOfXLogRecord)
		{
			// 					  LSN_FORMAT_ARGS(RecPtr),
			goto err;
		}
		gotheader = false;
	}

	len = XLOG_BLCKSZ - RecPtr % XLOG_BLCKSZ;
	if (total_len > len)
	{
		/* Need to reassemble record */
		char	   *contdata;
		XLogPageHeader pageHeader;
		char	   *buffer;
		uint32		gotlen;

		assembled = true;

		/*
		 * Enlarge readRecordBuf as needed.
		 */
		if (total_len > state->readRecordBufSize &&
			!allocate_recordbuf(state, total_len))
		{
			/* We treat this as a "bogus data" condition */
			printf("record length %u at %X/%X too long",
								  total_len, LSN_FORMAT_ARGS(RecPtr));
			goto err;
		}

		/* Copy the first fragment of the record from the first page. */
		memcpy(state->readRecordBuf,
			   state->readBuf + RecPtr % XLOG_BLCKSZ, len);
		buffer = state->readRecordBuf + len;
		gotlen = len;

		do
		{
			/* Calculate pointer to beginning of next page */
			targetPagePtr += XLOG_BLCKSZ;

			/* Wait for the next page to become available */
			readOff = ReadPageInternal(state, targetPagePtr,
									   Min(total_len - gotlen + SizeOfXLogShortPHD,
										   XLOG_BLCKSZ));

			if (readOff < 0)
				goto err;

			Assert(SizeOfXLogShortPHD <= readOff);

			pageHeader = (XLogPageHeader) state->readBuf;

			/*
			 * If we were expecting a continuation record and got an
			 * "overwrite contrecord" flag, that means the continuation record
			 * was overwritten with a different record.  Restart the read by
			 * assuming the address to read is the location where we found
			 * this flag; but keep track of the LSN of the record we were
			 * reading, for later verification.
			 */
			if (pageHeader->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD)
			{
				state->overwrittenRecPtr = RecPtr;
				ResetDecoder(state);
				RecPtr = targetPagePtr;
				goto restart;
			}

			/* Check that the continuation on next page looks valid */
			if (!(pageHeader->xlp_info & XLP_FIRST_IS_CONTRECORD))
			{
				printf(
									  "there is no contrecord flag at %X/%X",
									  LSN_FORMAT_ARGS(RecPtr));
				goto err;
			}

			/*
			 * Cross-check that xlp_rem_len agrees with how much of the record
			 * we expect there to be left.
			 */
			if (pageHeader->xlp_rem_len == 0 ||
				total_len != (pageHeader->xlp_rem_len + gotlen))
			{
				printf(
									  "invalid contrecord length %u (expected %lld) at %X/%X",
									  pageHeader->xlp_rem_len,
									  ((long long) total_len) - gotlen,
									  LSN_FORMAT_ARGS(RecPtr));
				goto err;
			}

			/* Append the continuation from this page to the buffer */
			pageHeaderSize = XLogPageHeaderSize(pageHeader);

			if (readOff < pageHeaderSize)
				readOff = ReadPageInternal(state, targetPagePtr,
										   pageHeaderSize);

			Assert(pageHeaderSize <= readOff);

			contdata = (char *) state->readBuf + pageHeaderSize;
			len = XLOG_BLCKSZ - pageHeaderSize;
			if (pageHeader->xlp_rem_len < len)
				len = pageHeader->xlp_rem_len;

			if (readOff < pageHeaderSize + len)
				readOff = ReadPageInternal(state, targetPagePtr,
										   pageHeaderSize + len);

			memcpy(buffer, (char *) contdata, len);
			buffer += len;
			gotlen += len;

			/* If we just reassembled the record header, validate it. */
			if (!gotheader)
			{
				record = (XLogRecord *) state->readRecordBuf;
				if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr,
										   record, randAccess))
					goto err;
				gotheader = true;
			}
		} while (gotlen < total_len);

		Assert(gotheader);

		record = (XLogRecord *) state->readRecordBuf;

		pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) state->readBuf);
		state->ReadRecPtr = RecPtr;
		state->EndRecPtr = targetPagePtr + pageHeaderSize
			+ MAXALIGN(pageHeader->xlp_rem_len);
	}
	else
	{
		/* Wait for the record data to become available */
		readOff = ReadPageInternal(state, targetPagePtr,
								   Min(targetRecOff + total_len, XLOG_BLCKSZ));
		if (readOff < 0)
			goto err;

		// /* Record does not cross a page boundary */

		state->EndRecPtr = RecPtr + MAXALIGN(total_len);

		state->ReadRecPtr = RecPtr;
	}

	/*
	 * Special processing if it's an XLOG SWITCH record
	 */
	if (record->xl_rmid == 0 &&
		(record->xl_info & ~XLR_INFO_MASK) == XLOG_SWITCH)
	{
		/* Pretend it extends to end of segment */
		state->EndRecPtr += state->segcxt.ws_segsize - 1;
		state->EndRecPtr -= XLogSegmentOffset(state->EndRecPtr, state->segcxt.ws_segsize);
	}

	if (DecodeXLogRecord(state, record, errormsg))
		return record;
	else
		return NULL;

err:
	if (assembled)
	{
		/*
		 * We get here when a record that spans multiple pages needs to be
		 * assembled, but something went wrong -- perhaps a contrecord piece
		 * was lost.  If caller is WAL replay, it will know where the aborted
		 * record was and where to direct followup WAL to be written, marking
		 * the next piece with XLP_FIRST_IS_OVERWRITE_CONTRECORD, which will
		 * in turn signal downstream WAL consumers that the broken WAL record
		 * is to be ignored.
		 */
		state->abortedRecPtr = RecPtr;
		state->missingContrecPtr = targetPagePtr;
	}

	/*
	 * Invalidate the read state. We might read from a different source after
	 * failure.
	 */

	if (state->errormsg_buf[0] != '\0')
		*errormsg = state->errormsg_buf;

	return NULL;
}
#endif
XLogRecPtr XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr)
{
	XLogRecPtr	tmpRecPtr;
	XLogRecPtr	found = InvalidXLogRecPtr;
	XLogPageHeader header;
	char	   *errormsg;

	Assert(!XLogRecPtrIsInvalid(RecPtr));

	/*
	 * skip over potential continuation data, keeping in mind that it may span
	 * multiple pages
	 */
	tmpRecPtr = RecPtr;
	while (true)
	{
		XLogRecPtr	targetPagePtr;
		int			targetRecOff;
		uint32		pageHeaderSize;
		int			readLen;

		/*
		 * Compute targetRecOff. It should typically be equal or greater than
		 * short page-header since a valid record can't start anywhere before
		 * that, except when caller has explicitly specified the offset that
		 * falls somewhere there or when we are skipping multi-page
		 * continuation record. It doesn't matter though because
		 * ReadPageInternal() is prepared to handle that and will read at
		 * least short page-header worth of data
		 */
		targetRecOff = tmpRecPtr % XLOG_BLCKSZ;

		/* scroll back to page boundary */
		targetPagePtr = tmpRecPtr - targetRecOff;

		/* Read the page containing the record */
		readLen = ReadPageInternal(state, targetPagePtr, targetRecOff);
		if (readLen < 0)
			goto err;

		header = (XLogPageHeader) state->readBuf;

		pageHeaderSize = XLogPageHeaderSize(header);

		/* make sure we have enough data for the page header */
		readLen = ReadPageInternal(state, targetPagePtr, pageHeaderSize);
		if (readLen < 0)
			goto err;

		/* skip over potential continuation data */
		if (header->xlp_info & XLP_FIRST_IS_CONTRECORD)
		{
			/*
			 * If the length of the remaining continuation data is more than
			 * what can fit in this page, the continuation record crosses over
			 * this page. Read the next page and try again. xlp_rem_len in the
			 * next page header will contain the remaining length of the
			 * continuation data
			 *
			 * Note that record headers are MAXALIGN'ed
			 */
			if (MAXALIGN(header->xlp_rem_len) >= (XLOG_BLCKSZ - pageHeaderSize))
				tmpRecPtr = targetPagePtr + XLOG_BLCKSZ;
			else
			{
				/*
				 * The previous continuation record ends in this page. Set
				 * tmpRecPtr to point to the first valid record
				 */
				tmpRecPtr = targetPagePtr + pageHeaderSize
					+ MAXALIGN(header->xlp_rem_len);
				break;
			}
		}
		else
		{
			tmpRecPtr = targetPagePtr + pageHeaderSize;
			break;
		}
	}

	/*
	 * we know now that tmpRecPtr is an address pointing to a valid XLogRecord
	 * because either we're at the first record after the beginning of a page
	 * or we just jumped over the remaining data of a continuation.
	 */
	XLogBeginRead(state, tmpRecPtr);
	while (XLogReadRecord(state, &errormsg) != NULL)
	{
		/* past the record we've found, break out */
		if (RecPtr <= state->ReadRecPtr)
		{
			/* Rewind the reader to the beginning of the last record. */
			found = state->ReadRecPtr;
			XLogBeginRead(state, found);
			return found;
		}
	}

err:
	XLogReaderInvalReadState(state);

	return InvalidXLogRecPtr;
}

parray *pgGetTxforArch(parray **TxTime_parray_ptr,
				TimestampTz *SrtTime,TimestampTz *EndTime,
				WALFILE *archDirFiles,int archWaldirNum,
				char *start_fname_pg,char *end_fname_pg,
				char waldir[1024],int flag,
				char datafile[50],char oldDatafile[50],
				char toastfile[50],char oldToastfile[50],
				char tabname[50],parray *Tx_parray,
				int FPICntClean,decodeFunc *array2Process,
				TABstruct *taboid)
{
	int nAttr;

	if(flag != DROP){
		nAttr = atoi(taboid->nattr);
		allDesc = (pg_attributeDesc*)malloc(nAttr*sizeof(pg_attributeDesc));
		dropExist2 = getPgAttrDesc(taboid,allDesc);
		initToastId(taboid->toastnode);
		memset(FPWSegmentPath,0,100);
		if(strcmp(datafile,"0") == 0 && strcmp(oldDatafile,"0") == 0){
			isToastRound = 1;
			strcpy(FPWSegmentPath,"restore/datafile");
		}
		else{
			isToastRound = 0;
			strcpy(FPWSegmentPath,"restore/.fpw");
		}
	}
	else{
		sdcPgClass = initSystemDropContext("pg_class");
		sdcPgAttr = initSystemDropContext("pg_attribute");
		isToastRound = 0;
		strcpy(FPWSegmentPath,"restore/.fpw");
	}

	FPIcount = 0;
	FPIErrcount = 0;
	FPIUpdateSame = 0;
	char bootfilename[500]={0};
	determineTimeMode(SrtTime,EndTime);

	char suffix[10];
	char filetyp[10];
	memset(suffix,0,10);
	memset(filetyp,0,10);
	if(ExportMode_there == CSVform && resTyp_there == DELETEtyp)
		strcpy(suffix,".csv");
	else if((ExportMode_there == SQLform && resTyp_there == DELETEtyp) || resTyp_there == UPDATEtyp)
		strcpy(suffix,".sql");

	if(resTyp_there == DELETEtyp)
		strcpy(filetyp,"del");
	else if(resTyp_there == UPDATEtyp)
		strcpy(filetyp,"upd");

	FILE *bootFile;
	if(flag == DELRESTORE && restoreMode_there == TxRestore && isToastRound == 0){
		DELstruct *elem = parray_get(Tx_parray,0);
		sprintf(bootfilename,"restore/public/%s_%d%s",tabname,elem->tx,suffix);
		bootFile = fopen(bootfilename,"w");
        #ifdef CN
        char *item="▌ 事务号恢复模式";
        #else
        char *item="▌ Tx Restore Mode [Displayed by Tx groups]";
        #endif
        infoRestoreMode(item);
	}
	else if(flag == DELRESTORE && restoreMode_there == periodRestore && isToastRound == 0){
		char *srttimeStr=(char *)timestamptz_to_str(*SrtTime);
		char *endtimeStr=(char *)timestamptz_to_str(*EndTime);
		sprintf(bootfilename,"restore/public/%s_%s_%s_%s%s",tabname,filetyp,srttimeStr,endtimeStr,suffix);
		bootFile = fopen(bootfilename,"w");
        #ifdef CN
        char *item="▌ 时间区间恢复模式";
        #else
        char *item="▌ Time Range Restore Mode [Displayed all within Time Range]";
        #endif
        infoRestoreMode(item);
	}

	char *FPIPath = "restore/datafile";
	memset(targetDatafile,0,50);
	memset(targetOldDatafile,0,50);
	memset(targetToastfile,0,50);
	memset(targetOldToastfile,0,50);

	strcpy(targetDatafile,datafile);
	strcpy(targetOldDatafile,oldDatafile);
	strcpy(targetToastfile,toastfile);
	strcpy(targetOldToastfile,oldToastfile);

	delOrDrop = flag;
    earliestTimeLocal_pg = NULL;

    *TxTime_parray_ptr = parray_new();
    parray *TxTime_parray = *TxTime_parray_ptr;

	elemforTime = (DELstruct*)malloc(sizeof(DELstruct));
	elemforTimeINIT(elemforTime);

	if(flag != DELRESTORE){
		if(Txs && delElems){
			parray_free(Txs);
			harray_free(delElems);
			Txs=NULL;
			delElems=NULL;
		}

		Txs = parray_new();
		delElems = harray_new(HARRAYDEL);
	}
	if(LsnBlkInfos){
		parray_free(LsnBlkInfos);
		LsnBlkInfos = NULL;
	}
	LsnBlkInfos = parray_new();

	XLogReaderState *xlogreader_state;
	XLogDumpPrivate private;
	char	   *directory = NULL;

	int			fd;
	XLogSegNo	segno;
	char	   *errormsg;

	XLogRecord *record;
	XLogRecPtr	first_record;

	XLogDumpConfig config;

	memset(&private, 0, sizeof(XLogDumpPrivate));
	private.timeline = 1;
	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;
	private.endptr_reached = false;

	PGAlignedXLogBlock buf;
	int			r;

	fd = open_file_in_directory(waldir, start_fname_pg);
	XLogLongPageHeader longhdr = NULL;
	char		fpath[MAXPGPATH];

	snprintf(fpath, MAXPGPATH, "%s/%s", waldir, start_fname_pg);
	FILE *fp = fopen(fpath,"rb");
	r = fread(buf.data,1,BLCKSZ,fp);
	if (r == XLOG_BLCKSZ)
	{
		longhdr = (XLogLongPageHeader) buf.data;
		WalSegSz = longhdr->xlp_seg_size;
	}
	if (fd < 0)
	printf("could not open file \"%s\"\n", start_fname_pg);
	close(fd);
	/* parse position from file */
	XLogFromFileName(start_fname_pg, &private.timeline, &segno, WalSegSz);
	if (XLogRecPtrIsInvalid(private.startptr))
		XLogSegNoOffsetToRecPtr(segno, 0, WalSegSz, private.startptr);
	else if (!XLByteInSeg(private.startptr, segno, WalSegSz))
	{
		printf("start WAL location is not inside file \"%s\"",
						start_fname_pg);
		}

    XLogSegNo endsegno;
	fd = open_file_in_directory(waldir, end_fname_pg);
	if (fd < 0)
		printf("could not open file \"%s\"\n", end_fname_pg);
	close(fd);

	XLogFromFileName(end_fname_pg, &private.timeline, &endsegno, WalSegSz);

	if (XLogRecPtrIsInvalid(private.endptr))
	XLogSegNoOffsetToRecPtr(endsegno + 1, 0, WalSegSz,
							private.endptr);

	if (endsegno < segno)
		printf("Are StartFile and EndFile reversed?\n");

	xlogreader_state =
		XLogReaderAllocate(WalSegSz, waldir,
						   XL_ROUTINE(.page_read = WALDumpReadPage,
									  .segment_open = WALDumpOpenSegment,
									  .segment_close = WALDumpCloseSegment),
						   &private);
	if (!xlogreader_state){
		printf("out of memory");
		return TxTime_parray;
	}

	first_record = XLogFindNextRecord(xlogreader_state, private.startptr);
	if( isToastRound == 0 && flag == DELRESTORE){
		infoRestoreRecs(FPIcount);
	}

	for (;;)
	{
		/* try to read the next record */
		record = XLogReadRecord(xlogreader_state, &errormsg);
		if (!record)
		{
			WALOpenSegment seg = xlogreader_state->seg;

			char		fname[MAXPGPATH];
			XLogFileName(fname, seg.ws_tli, seg.ws_segno,WalSegSz);
			if(strcmp(fname,end_fname_pg) == 0){
				break;
			}
			int matchJ = 0;
			for(int j=0;j<archWaldirNum;j++){
					if(strcmp(archDirFiles[j].walnames+8,fname+8) > 0){
					matchJ = j;
					break;
				}
				else if(strcmp(archDirFiles[j].walnames+8,fname+8) == 0){
					matchJ = -1;
					break;
				}
			}

			if(matchJ != -1){
				char		fpath[MAXPGPATH];
				snprintf(fpath, MAXPGPATH, "%s/%s", waldir, fname);
				int fd = open(fpath, O_RDONLY | PG_BINARY, 0);
				while(fd < 0 && strcmp(fname+8,archDirFiles[matchJ].walnames+8) <= 0){
					seg.ws_segno++;
					XLogFileName(fname, seg.ws_tli, seg.ws_segno,WalSegSz);
					snprintf(fpath, MAXPGPATH, "%s/%s", waldir, fname);
					fd = open(fpath, O_RDONLY | PG_BINARY, 0);
				}

				if(strcmp(fname+8,archDirFiles[archWaldirNum-1].walnames+8) < 0){
					XLogFromFileName(fname, &private.timeline, &segno, WalSegSz);
					private.startptr = 0;
					if (XLogRecPtrIsInvalid(private.startptr)){
						XLogSegNoOffsetToRecPtr(segno, 0, WalSegSz, private.startptr);
					}

					XLogReaderFree(xlogreader_state);
					xlogreader_state =
						XLogReaderAllocate(WalSegSz, waldir,
										XL_ROUTINE(.page_read = WALDumpReadPage,
													.segment_open = WALDumpOpenSegment,
													.segment_close = WALDumpCloseSegment),
										&private);
					first_record = XLogFindNextRecord(xlogreader_state, private.startptr);
					continue;
				}
			}
			break;
		}

		memset(currWalName,0,70);
		XLogFileName(currWalName,private.timeline,xlogreader_state->seg.ws_segno,WalSegSz);

		int timeres = XlogGiveMeTime(xlogreader_state,SrtTime,EndTime);
		if( timeres == 0 ){
			continue;
		}
		else if (timeres == -1){
			break;
		}

		if(flag == DELRESTORE){
			if(isToastRound == 0){
				if( XLogRecordRestoreFPWs(allDesc,xlogreader_state, FPIPath,array2Process,Tx_parray,tabname,bootFile) == BREAK_RET ){
					break;
				}
			}
			else if(isToastRound == 1){
				if( XLogRecordRestoreFPWsforTOAST(allDesc,xlogreader_state, FPIPath,array2Process,Tx_parray,tabname,bootFile) == BREAK_RET){
					break;
				}
			}
		}
		else{
			XLogScanRecordForDisplay(&config, xlogreader_state,TxTime_parray);
		}

	}

	if(flag == DEL && restoreMode_there == TxRestore){
		mergeTxDelElems(TxTime_parray);
	}
	else if(flag == DEL && restoreMode_there == periodRestore){
		elemforTime->Txs = Txs;
		parray_append(TxTime_parray,elemforTime);
		harray_free(delElems);
		delElems = NULL;
	}

	if(flag == DELRESTORE && isToastRound == 0){
		fclose(bootFile);
		if(elemforTime != NULL)
			free(elemforTime);
		infoRestoreResult(tabname,FPIcount+FPIErrcount,FPIcount,FPIErrcount,FPIUpdateSame,bootfilename,resTyp_there);
		FPWHashCleanup();
	}

	free(allDesc);
	XLogReaderFree(xlogreader_state);
	allDesc = NULL;

	return TxTime_parray;
}

void mergeTxDelElems(parray *TxTime_parray){
	for (int i = 0; i < parray_num(Txs); i++)
	{
		TransactionId Tx = (TransactionId)(intptr_t)parray_get(Txs,i);
		DELstruct *elem = harray_get(delElems,HARRAYDEL,Tx);
		parray_append(TxTime_parray,elem);
	}
}
