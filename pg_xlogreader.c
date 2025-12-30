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
#define _GNU_SOURCE 
#include "pg_xlogreader.h"
#include <stdarg.h>
#include <time.h>
#include <stdint.h>
#include <stdatomic.h>

/**
 * XLogReaderValidatePageHeader - Validate WAL page header
 *
 * @state:  XLog reader state
 * @recptr: Record pointer to validate
 * @phdr:   Page header data
 *
 * Validates the WAL page header, checking magic number, info bits,
 * system identifier, segment size, and page address consistency.
 *
 * Returns: true if page header is valid, false otherwise
 */
bool
XLogReaderValidatePageHeader(XLogReaderState *state, XLogRecPtr recptr,
							 char *phdr)
{
	XLogRecPtr	recaddr;
	XLogSegNo	segno;
	int32		offset;
	XLogPageHeader hdr = (XLogPageHeader) phdr;

	Assert((recptr % XLOG_BLCKSZ) == 0);

	XLByteToSeg(recptr, segno, state->segcxt.ws_segsize);
	offset = XLogSegmentOffset(recptr, state->segcxt.ws_segsize);

	XLogSegNoOffsetToRecPtr(segno, offset, state->segcxt.ws_segsize, recaddr);

	// 						  "invalid magic number %04X in log segment %s, offset %u",
	// 						  hdr->xlp_magic,
	// 						  fname,

	if ((hdr->xlp_info & ~XLP_ALL_FLAGS) != 0)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->seg.ws_tli, segno, state->segcxt.ws_segsize);

		// 					  "invalid info bits %04X in log segment %s, offset %u",
		// 					  hdr->xlp_info,
		// 					  fname,
		return false;
	}

	if (hdr->xlp_info & XLP_LONG_HEADER)
	{
		XLogLongPageHeader longhdr = (XLogLongPageHeader) hdr;

		if (state->system_identifier &&
			longhdr->xlp_sysid != state->system_identifier)
		{
			printf(
								  "WAL file is from different database system: WAL file database system identifier is %llu, pg_control database system identifier is %llu",
								  (unsigned long long) longhdr->xlp_sysid,
								  (unsigned long long) state->system_identifier);
			return false;
		}
		else if (longhdr->xlp_seg_size != state->segcxt.ws_segsize)
		{
			printf(
								  "WAL file is from different database system: incorrect segment size in page header");
			return false;
		}
		else if (longhdr->xlp_xlog_blcksz != XLOG_BLCKSZ)
		{
			printf(
								  "WAL file is from different database system: incorrect XLOG_BLCKSZ in page header");
			return false;
		}
	}
	else if (offset == 0)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->seg.ws_tli, segno, state->segcxt.ws_segsize);

		/* hmm, first page of file doesn't have a long header? */
		// 					  "invalid info bits %04X in log segment %s, offset %u",
		// 					  hdr->xlp_info,
		// 					  fname,
		return false;
	}

	/*
	 * Check that the address on the page agrees with what we expected. This
	 * check typically fails when an old WAL segment is recycled, and hasn't
	 * yet been overwritten with new data yet.
	 */
	if (hdr->xlp_pageaddr != recaddr)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->seg.ws_tli, segno, state->segcxt.ws_segsize);

		printf(
							  "unexpected pageaddr %X/%X in log segment %s, offset %u",
							  LSN_FORMAT_ARGS(hdr->xlp_pageaddr),
							  fname,
							  offset);
		return false;
	}

	/*
	 * Since child timelines are always assigned a TLI greater than their
	 * immediate parent's TLI, we should never see TLI go backwards across
	 * successive pages of a consistent WAL sequence.
	 *
	 * Sometimes we re-read a segment that's already been (partially) read. So
	 * we only verify TLIs for pages that are later than the last remembered
	 * LSN.
	 */
	if (recptr > state->latestPagePtr)
	{
		if (hdr->xlp_tli < state->latestPageTLI)
		{
			char		fname[MAXFNAMELEN];

			XLogFileName(fname, state->seg.ws_tli, segno, state->segcxt.ws_segsize);

			printf(
								  "out-of-sequence timeline ID %u (after %u) in log segment %s, offset %u",
								  hdr->xlp_tli,
								  state->latestPageTLI,
								  fname,
								  offset);
			return false;
		}
	}
	state->latestPagePtr = recptr;
	state->latestPageTLI = hdr->xlp_tli;

	return true;
}

/**
 * ValidXLogRecordHeader - Validate XLog record header
 *
 * @state:      XLog reader state
 * @RecPtr:     Current record pointer
 * @PrevRecPtr: Previous record pointer
 * @record:     XLog record to validate
 * @randAccess: Whether this is random access (vs sequential)
 *
 * Validates the XLog record header by checking total length, resource
 * manager ID, and previous link consistency.
 *
 * Returns: true if record header is valid, false otherwise
 */
bool ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr,
					  XLogRecPtr PrevRecPtr, XLogRecord *record,
					  bool randAccess)
{
	if (record->xl_tot_len < SizeOfXLogRecord)
	{
		printf("read until FINAL WAL SEGMENT: %X/%X\n",
							  LSN_FORMAT_ARGS(RecPtr),
							  (uint32) SizeOfXLogRecord, record->xl_tot_len);
		return false;
	}
	// 						  "invalid resource manager ID %u at %X/%X",
	if (randAccess)
	{
		/*
		 * We can't exactly verify the prev-link, but surely it should be less
		 * than the record's own address.
		 */
		if (!(record->xl_prev < RecPtr))
		{
			printf(
								  "record with incorrect prev-link %X/%X at %X/%X",
								  LSN_FORMAT_ARGS(record->xl_prev),
								  LSN_FORMAT_ARGS(RecPtr));
			return false;
		}
	}
	else
	{
		/*
		 * Record's prev-link should exactly match our previous location. This
		 * check guards against torn WAL pages where a stale but valid-looking
		 * WAL record starts on a sector boundary.
		 */
		if (record->xl_prev != PrevRecPtr)
		{
			printf(
								  "record with incorrect prev-link %X/%X at %X/%X",
								  LSN_FORMAT_ARGS(record->xl_prev),
								  LSN_FORMAT_ARGS(RecPtr));
			return false;
		}
	}

	return true;
}

#if PG_VERSION_NUM > 14
/**
 * ResetDecoder - Reset the XLog decoder state (PG > 14)
 *
 * @state: XLog reader state to reset
 *
 * Resets the decoded record queue, decode buffer, and error state.
 * Frees any oversized records in the queue.
 */
void ResetDecoder(XLogReaderState *state)
{
	DecodedXLogRecord *r;

	/* Reset the decoded record queue, freeing any oversized records. */
	while ((r = state->decode_queue_head) != NULL)
	{
		state->decode_queue_head = r->next;
		if (r->oversized)
			free(r);
	}
	state->decode_queue_tail = NULL;
	state->decode_queue_head = NULL;
	state->record = NULL;

	/* Reset the decode buffer to empty. */
	state->decode_buffer_tail = state->decode_buffer;
	state->decode_buffer_head = state->decode_buffer;

	/* Clear error state. */
	state->errormsg_buf[0] = '\0';
	state->errormsg_deferred = false;
}

/**
 * XLogRecGetBlockData - Get block data from XLog record (PG > 14)
 *
 * @record:   XLog reader state containing the record
 * @block_id: Block ID to retrieve data for
 * @len:      Output parameter for data length (may be NULL)
 *
 * Returns: Pointer to block data as uintptr_t, or 0 if not available
 */
uintptr_t XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len)
{
	DecodedBkpBlock *bkpb;

	if (block_id > record->record->max_block_id ||
		!record->record->blocks[block_id].in_use)
		return 0;

	bkpb = &record->record->blocks[block_id];

	if (!bkpb->has_data)
	{
		if (len)
			*len = 0;
		return 0;
	}
	else
	{
		if (len)
			*len = bkpb->data_len;
		return (uintptr_t)bkpb->data;
	}
}

/**
 * DecodeXLogRecord - Decode an XLog record (PG > 14)
 *
 * @state:    XLog reader state
 * @decoded:  Output decoded record structure
 * @record:   Raw XLog record to decode
 * @lsn:      Log sequence number
 * @errormsg: Output error message on failure
 *
 * Parses the XLog record header and data fragments, copying block
 * images and data to properly aligned locations.
 *
 * Returns: true on success, false on error
 */
bool DecodeXLogRecord(XLogReaderState *state,
				 DecodedXLogRecord *decoded,
				 XLogRecord *record,
				 XLogRecPtr lsn,
				 char **errormsg)
{
	/*
	 * read next _size bytes from record buffer, but check for overrun first.
	 */
#define COPY_HEADER_FIELD(_dst, _size)			\
	do {										\
		if (remaining < _size)					\
			goto shortdata_err;					\
		memcpy(_dst, ptr, _size);				\
		ptr += _size;							\
		remaining -= _size;						\
	} while(0)

	char	   *ptr;
	char	   *out;
	uint32		remaining;
	uint32		datatotal;
	RelFileNode *rnode = NULL;
	uint8		block_id;

	decoded->header = *record;
	decoded->lsn = lsn;
	decoded->next = NULL;
	decoded->record_origin = InvalidRepOriginId;
	decoded->toplevel_xid = InvalidTransactionId;
	decoded->main_data = NULL;
	decoded->main_data_len = 0;
	decoded->max_block_id = -1;
	ptr = (char *) record;
	ptr += SizeOfXLogRecord;
	remaining = record->xl_tot_len - SizeOfXLogRecord;

	/* Decode the headers */
	datatotal = 0;
	while (remaining > datatotal)
	{
		COPY_HEADER_FIELD(&block_id, sizeof(uint8));

		if (block_id == XLR_BLOCK_ID_DATA_SHORT)
		{
			/* XLogRecordDataHeaderShort */
			uint8		main_data_len;

			COPY_HEADER_FIELD(&main_data_len, sizeof(uint8));

			decoded->main_data_len = main_data_len;
			datatotal += main_data_len;
			break;				/* by convention, the main data fragment is
								 * always last */
		}
		else if (block_id == XLR_BLOCK_ID_DATA_LONG)
		{
			/* XLogRecordDataHeaderLong */
			uint32		main_data_len;

			COPY_HEADER_FIELD(&main_data_len, sizeof(uint32));
			decoded->main_data_len = main_data_len;
			datatotal += main_data_len;
			break;				/* by convention, the main data fragment is
								 * always last */
		}
		else if (block_id == XLR_BLOCK_ID_ORIGIN)
		{
			COPY_HEADER_FIELD(&decoded->record_origin, sizeof(RepOriginId));
		}
		else if (block_id == XLR_BLOCK_ID_TOPLEVEL_XID)
		{
			COPY_HEADER_FIELD(&decoded->toplevel_xid, sizeof(TransactionId));
		}
		else if (block_id <= XLR_MAX_BLOCK_ID)
		{
			/* XLogRecordBlockHeader */
			DecodedBkpBlock *blk;
			uint8		fork_flags;

			/* mark any intervening block IDs as not in use */
			for (int i = decoded->max_block_id + 1; i < block_id; ++i)
				decoded->blocks[i].in_use = false;

			if (block_id <= decoded->max_block_id)
			{
				printf(
									  "out-of-order block_id %u at %X/%X",
									  block_id,
									  LSN_FORMAT_ARGS(state->ReadRecPtr));
				goto err;
			}
			decoded->max_block_id = block_id;

			blk = &decoded->blocks[block_id];
			blk->in_use = true;
			blk->apply_image = false;

			COPY_HEADER_FIELD(&fork_flags, sizeof(uint8));
			blk->forknum = fork_flags & BKPBLOCK_FORK_MASK;
			blk->flags = fork_flags;
			blk->has_image = ((fork_flags & BKPBLOCK_HAS_IMAGE) != 0);
			blk->has_data = ((fork_flags & BKPBLOCK_HAS_DATA) != 0);

			blk->prefetch_buffer = InvalidBuffer;

			COPY_HEADER_FIELD(&blk->data_len, sizeof(uint16));
			/* cross-check that the HAS_DATA flag is set iff data_length > 0 */
			if (blk->has_data && blk->data_len == 0)
			{
				printf(
									  "BKPBLOCK_HAS_DATA set, but no data included at %X/%X",
									  LSN_FORMAT_ARGS(state->ReadRecPtr));
				goto err;
			}
			if (!blk->has_data && blk->data_len != 0)
			{
				printf(
									  "BKPBLOCK_HAS_DATA not set, but data length is %u at %X/%X",
									  (unsigned int) blk->data_len,
									  LSN_FORMAT_ARGS(state->ReadRecPtr));
				goto err;
			}
			datatotal += blk->data_len;

			if (blk->has_image)
			{
				COPY_HEADER_FIELD(&blk->bimg_len, sizeof(uint16));
				COPY_HEADER_FIELD(&blk->hole_offset, sizeof(uint16));
				COPY_HEADER_FIELD(&blk->bimg_info, sizeof(uint8));

				blk->apply_image = ((blk->bimg_info & BKPIMAGE_APPLY) != 0);

				if (BKPIMAGE_COMPRESSED(blk->bimg_info))
				{
					if (blk->bimg_info & BKPIMAGE_HAS_HOLE)
						COPY_HEADER_FIELD(&blk->hole_length, sizeof(uint16));
					else
						blk->hole_length = 0;
				}
				else
					blk->hole_length = BLCKSZ - blk->bimg_len;
				datatotal += blk->bimg_len;

				/*
				 * cross-check that hole_offset > 0, hole_length > 0 and
				 * bimg_len < BLCKSZ if the HAS_HOLE flag is set.
				 */
				if ((blk->bimg_info & BKPIMAGE_HAS_HOLE) &&
					(blk->hole_offset == 0 ||
					 blk->hole_length == 0 ||
					 blk->bimg_len == BLCKSZ))
				{
					printf(
										  "BKPIMAGE_HAS_HOLE set, but hole offset %u length %u block image length %u at %X/%X",
										  (unsigned int) blk->hole_offset,
										  (unsigned int) blk->hole_length,
										  (unsigned int) blk->bimg_len,
										  LSN_FORMAT_ARGS(state->ReadRecPtr));
					goto err;
				}

				/*
				 * cross-check that hole_offset == 0 and hole_length == 0 if
				 * the HAS_HOLE flag is not set.
				 */
				if (!(blk->bimg_info & BKPIMAGE_HAS_HOLE) &&
					(blk->hole_offset != 0 || blk->hole_length != 0))
				{
					printf(
										  "BKPIMAGE_HAS_HOLE not set, but hole offset %u length %u at %X/%X",
										  (unsigned int) blk->hole_offset,
										  (unsigned int) blk->hole_length,
										  LSN_FORMAT_ARGS(state->ReadRecPtr));
					goto err;
				}

				/*
				 * Cross-check that bimg_len < BLCKSZ if it is compressed.
				 */
				if (BKPIMAGE_COMPRESSED(blk->bimg_info) &&
					blk->bimg_len == BLCKSZ)
				{
					printf(
										  "BKPIMAGE_COMPRESSED set, but block image length %u at %X/%X",
										  (unsigned int) blk->bimg_len,
										  LSN_FORMAT_ARGS(state->ReadRecPtr));
					goto err;
				}

				/*
				 * cross-check that bimg_len = BLCKSZ if neither HAS_HOLE is
				 * set nor COMPRESSED().
				 */
				if (!(blk->bimg_info & BKPIMAGE_HAS_HOLE) &&
					!BKPIMAGE_COMPRESSED(blk->bimg_info) &&
					blk->bimg_len != BLCKSZ)
				{
					printf(
										  "neither BKPIMAGE_HAS_HOLE nor BKPIMAGE_COMPRESSED set, but block image length is %u at %X/%X",
										  (unsigned int) blk->data_len,
										  LSN_FORMAT_ARGS(state->ReadRecPtr));
					goto err;
				}
			}
			if (!(fork_flags & BKPBLOCK_SAME_REL))
			{
				COPY_HEADER_FIELD(&blk->rnode, sizeof(RelFileNode));
				rnode = &blk->rnode;
			}
			else
			{
				if (rnode == NULL)
				{
					printf(
										  "BKPBLOCK_SAME_REL set but no previous rel at %X/%X",
										  LSN_FORMAT_ARGS(state->ReadRecPtr));
					goto err;
				}

				blk->rnode = *rnode;
			}
			COPY_HEADER_FIELD(&blk->blkno, sizeof(BlockNumber));
		}
		else
		{
			printf(
								  "invalid block_id %u at %X/%X",
								  block_id, LSN_FORMAT_ARGS(state->ReadRecPtr));
			goto err;
		}
	}

	if (remaining != datatotal)
		goto shortdata_err;

	/*
	 * Ok, we've parsed the fragment headers, and verified that the total
	 * length of the payload in the fragments is equal to the amount of data
	 * left.  Copy the data of each fragment to contiguous space after the
	 * blocks array, inserting alignment padding before the data fragments so
	 * they can be cast to struct pointers by REDO routines.
	 */
	out = ((char *) decoded) +
		offsetof(DecodedXLogRecord, blocks) +
		sizeof(decoded->blocks[0]) * (decoded->max_block_id + 1);

	/* block data first */
	for (block_id = 0; block_id <= decoded->max_block_id; block_id++)
	{
		DecodedBkpBlock *blk = &decoded->blocks[block_id];

		if (!blk->in_use)
			continue;

		Assert(blk->has_image || !blk->apply_image);

		if (blk->has_image)
		{
			/* no need to align image */
			blk->bkp_image = out;
			memcpy(out, ptr, blk->bimg_len);
			ptr += blk->bimg_len;
			out += blk->bimg_len;
		}
		if (blk->has_data)
		{
			out = (char *) MAXALIGN(out);
			blk->data = out;
			memcpy(blk->data, ptr, blk->data_len);
			ptr += blk->data_len;
			out += blk->data_len;
		}
	}

	/* and finally, the main data */
	if (decoded->main_data_len > 0)
	{
		out = (char *) MAXALIGN(out);
		decoded->main_data = out;
		memcpy(decoded->main_data, ptr, decoded->main_data_len);
		ptr += decoded->main_data_len;
		out += decoded->main_data_len;
	}

	/* Report the actual size we used. */
	decoded->size = MAXALIGN(out - (char *) decoded);
	Assert(DecodeXLogRecordRequiredSpace(record->xl_tot_len) >=
		   decoded->size);

	return true;

shortdata_err:
	printf(
						  "record with invalid length at %X/%X",
						  LSN_FORMAT_ARGS(state->ReadRecPtr));
err:
	*errormsg = state->errormsg_buf;

	return false;
}

#else
/**
 * ResetDecoder - Reset the XLog decoder state (PG <= 14)
 *
 * @state: XLog reader state to reset
 *
 * Resets the decoded record and clears all block states.
 */
void ResetDecoder(XLogReaderState *state)
{
	int			block_id;

	state->decoded_record = NULL;

	state->main_data_len = 0;

	for (block_id = 0; block_id <= state->max_block_id; block_id++)
	{
		state->blocks[block_id].in_use = false;
		state->blocks[block_id].has_image = false;
		state->blocks[block_id].has_data = false;
		state->blocks[block_id].apply_image = false;
	}
	state->max_block_id = -1;
}
/**
 * XLogRecGetBlockData - Get block data from XLog record (PG <= 14)
 *
 * @record:   XLog reader state containing the record
 * @block_id: Block ID to retrieve data for
 * @len:      Output parameter for data length (may be NULL)
 *
 * Returns: Pointer to block data as uintptr_t, or NULL if not available
 */
uintptr_t XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len)
{
	DecodedBkpBlock *bkpb;

	if (!record->blocks[block_id].in_use)
		return NULL;

	bkpb = &record->blocks[block_id];

	if (!bkpb->has_data)
	{
		if (len)
			*len = 0;
		return NULL;
	}
	else
	{
		if (len)
			*len = bkpb->data_len;
		return (uintptr_t)bkpb->data;
	}
}

/**
 * DecodeXLogRecord - Decode an XLog record (PG <= 14)
 *
 * @state:    XLog reader state
 * @record:   Raw XLog record to decode
 * @errormsg: Output error message on failure
 *
 * Parses the XLog record header and data fragments, allocating
 * and copying block data and main data to properly aligned buffers.
 *
 * Returns: true on success, false on error
 */
bool
DecodeXLogRecord(XLogReaderState *state, XLogRecord *record, char **errormsg)
{
	/*
	 * read next _size bytes from record buffer, but check for overrun first.
	 */
#define COPY_HEADER_FIELD(_dst, _size)			\
	do {										\
		if (remaining < _size)					\
			goto shortdata_err;					\
		memcpy(_dst, ptr, _size);				\
		ptr += _size;							\
		remaining -= _size;						\
	} while(0)

	char	   *ptr;
	uint32		remaining;
	uint32		datatotal;
	RelFileNode *rnode = NULL;
	uint8		block_id;

	ResetDecoder(state);

	state->decoded_record = record;
	state->record_origin = InvalidRepOriginId;
	state->toplevel_xid = InvalidTransactionId;

	ptr = (char *) record;
	ptr += SizeOfXLogRecord;
	remaining = record->xl_tot_len - SizeOfXLogRecord;

	/* Decode the headers */
	datatotal = 0;
	while (remaining > datatotal)
	{
		COPY_HEADER_FIELD(&block_id, sizeof(uint8));

		if (block_id == XLR_BLOCK_ID_DATA_SHORT)
		{
			/* XLogRecordDataHeaderShort */
			uint8		main_data_len;

			COPY_HEADER_FIELD(&main_data_len, sizeof(uint8));

			state->main_data_len = main_data_len;
			datatotal += main_data_len;
			break;				/* by convention, the main data fragment is
								 * always last */
		}
		else if (block_id == XLR_BLOCK_ID_DATA_LONG)
		{
			/* XLogRecordDataHeaderLong */
			uint32		main_data_len;

			COPY_HEADER_FIELD(&main_data_len, sizeof(uint32));
			state->main_data_len = main_data_len;
			datatotal += main_data_len;
			break;				/* by convention, the main data fragment is
								 * always last */
		}
		else if (block_id == XLR_BLOCK_ID_ORIGIN)
		{
			COPY_HEADER_FIELD(&state->record_origin, sizeof(RepOriginId));
		}
		else if (block_id == XLR_BLOCK_ID_TOPLEVEL_XID)
		{
			COPY_HEADER_FIELD(&state->toplevel_xid, sizeof(TransactionId));
		}
		else if (block_id <= XLR_MAX_BLOCK_ID)
		{
			/* XLogRecordBlockHeader */
			DecodedBkpBlock *blk;
			uint8		fork_flags;

			if (block_id <= state->max_block_id)
			{
				printf(
									  "out-of-order block_id %u at %X/%X",
									  block_id,
									  LSN_FORMAT_ARGS(state->ReadRecPtr));
				goto err;
			}
			state->max_block_id = block_id;

			blk = &state->blocks[block_id];
			blk->in_use = true;
			blk->apply_image = false;

			COPY_HEADER_FIELD(&fork_flags, sizeof(uint8));
			blk->forknum = fork_flags & BKPBLOCK_FORK_MASK;
			blk->flags = fork_flags;
			blk->has_image = ((fork_flags & BKPBLOCK_HAS_IMAGE) != 0);
			blk->has_data = ((fork_flags & BKPBLOCK_HAS_DATA) != 0);

			COPY_HEADER_FIELD(&blk->data_len, sizeof(uint16));
			/* cross-check that the HAS_DATA flag is set iff data_length > 0 */
			if (blk->has_data && blk->data_len == 0)
			{
				printf(
									  "BKPBLOCK_HAS_DATA set, but no data included at %X/%X",
									  LSN_FORMAT_ARGS(state->ReadRecPtr));
				goto err;
			}
			if (!blk->has_data && blk->data_len != 0)
			{
				printf(
									  "BKPBLOCK_HAS_DATA not set, but data length is %u at %X/%X",
									  (unsigned int) blk->data_len,
									  LSN_FORMAT_ARGS(state->ReadRecPtr));
				goto err;
			}
			datatotal += blk->data_len;

			if (blk->has_image)
			{
				COPY_HEADER_FIELD(&blk->bimg_len, sizeof(uint16));
				COPY_HEADER_FIELD(&blk->hole_offset, sizeof(uint16));
				COPY_HEADER_FIELD(&blk->bimg_info, sizeof(uint8));

				blk->apply_image = ((blk->bimg_info & BKPIMAGE_APPLY) != 0);

				if (blk->bimg_info & BKPIMAGE_IS_COMPRESSED)
				{
					if (blk->bimg_info & BKPIMAGE_HAS_HOLE)
						COPY_HEADER_FIELD(&blk->hole_length, sizeof(uint16));
					else
						blk->hole_length = 0;
				}
				else
					blk->hole_length = BLCKSZ - blk->bimg_len;
				datatotal += blk->bimg_len;

				/*
				 * cross-check that hole_offset > 0, hole_length > 0 and
				 * bimg_len < BLCKSZ if the HAS_HOLE flag is set.
				 */
				if ((blk->bimg_info & BKPIMAGE_HAS_HOLE) &&
					(blk->hole_offset == 0 ||
					 blk->hole_length == 0 ||
					 blk->bimg_len == BLCKSZ))
				{
					printf(
										  "BKPIMAGE_HAS_HOLE set, but hole offset %u length %u block image length %u at %X/%X",
										  (unsigned int) blk->hole_offset,
										  (unsigned int) blk->hole_length,
										  (unsigned int) blk->bimg_len,
										  LSN_FORMAT_ARGS(state->ReadRecPtr));
					goto err;
				}

				/*
				 * cross-check that hole_offset == 0 and hole_length == 0 if
				 * the HAS_HOLE flag is not set.
				 */
				if (!(blk->bimg_info & BKPIMAGE_HAS_HOLE) &&
					(blk->hole_offset != 0 || blk->hole_length != 0))
				{
					printf(
										  "BKPIMAGE_HAS_HOLE not set, but hole offset %u length %u at %X/%X",
										  (unsigned int) blk->hole_offset,
										  (unsigned int) blk->hole_length,
										  LSN_FORMAT_ARGS(state->ReadRecPtr));
					goto err;
				}

				/*
				 * cross-check that bimg_len < BLCKSZ if the IS_COMPRESSED
				 * flag is set.
				 */
				if ((blk->bimg_info & BKPIMAGE_IS_COMPRESSED) &&
					blk->bimg_len == BLCKSZ)
				{
					printf(
										  "BKPIMAGE_IS_COMPRESSED set, but block image length %u at %X/%X",
										  (unsigned int) blk->bimg_len,
										  LSN_FORMAT_ARGS(state->ReadRecPtr));
					goto err;
				}

				/*
				 * cross-check that bimg_len = BLCKSZ if neither HAS_HOLE nor
				 * IS_COMPRESSED flag is set.
				 */
				if (!(blk->bimg_info & BKPIMAGE_HAS_HOLE) &&
					!(blk->bimg_info & BKPIMAGE_IS_COMPRESSED) &&
					blk->bimg_len != BLCKSZ)
				{
					printf(
										  "neither BKPIMAGE_HAS_HOLE nor BKPIMAGE_IS_COMPRESSED set, but block image length is %u at %X/%X",
										  (unsigned int) blk->data_len,
										  LSN_FORMAT_ARGS(state->ReadRecPtr));
					goto err;
				}
			}
			if (!(fork_flags & BKPBLOCK_SAME_REL))
			{
				COPY_HEADER_FIELD(&blk->rnode, sizeof(RelFileNode));
				rnode = &blk->rnode;
			}
			else
			{
				if (rnode == NULL)
				{
					printf(
										  "BKPBLOCK_SAME_REL set but no previous rel at %X/%X",
										  LSN_FORMAT_ARGS(state->ReadRecPtr));
					goto err;
				}

				blk->rnode = *rnode;
			}
			COPY_HEADER_FIELD(&blk->blkno, sizeof(BlockNumber));
		}
		else
		{
			printf(
								  "invalid block_id %u at %X/%X",
								  block_id, LSN_FORMAT_ARGS(state->ReadRecPtr));
			goto err;
		}
	}

	if (remaining != datatotal)
		goto shortdata_err;

	/*
	 * Ok, we've parsed the fragment headers, and verified that the total
	 * length of the payload in the fragments is equal to the amount of data
	 * left. Copy the data of each fragment to a separate buffer.
	 *
	 * We could just set up pointers into readRecordBuf, but we want to align
	 * the data for the convenience of the callers. Backup images are not
	 * copied, however; they don't need alignment.
	 */

	/* block data first */
	for (block_id = 0; block_id <= state->max_block_id; block_id++)
	{
		DecodedBkpBlock *blk = &state->blocks[block_id];

		if (!blk->in_use)
			continue;

		Assert(blk->has_image || !blk->apply_image);

		if (blk->has_image)
		{
			blk->bkp_image = ptr;
			ptr += blk->bimg_len;
		}
		if (blk->has_data)
		{
			if (!blk->data || blk->data_len > blk->data_bufsz)
			{
				if (blk->data)
					free(blk->data);

				/*
				 * Force the initial request to be BLCKSZ so that we don't
				 * waste time with lots of trips through this stanza as a
				 * result of WAL compression.
				 */
				blk->data_bufsz = MAXALIGN(Max(blk->data_len, BLCKSZ));
				blk->data = malloc(blk->data_bufsz);
			}
			memcpy(blk->data, ptr, blk->data_len);
			ptr += blk->data_len;
		}
	}

	/* and finally, the main data */
	if (state->main_data_len > 0)
	{
		if (!state->main_data || state->main_data_len > state->main_data_bufsz)
		{
			if (state->main_data)
				free(state->main_data);

			/*
			 * main_data_bufsz must be MAXALIGN'ed.  In many xlog record
			 * types, we omit trailing struct padding on-disk to save a few
			 * bytes; but compilers may generate accesses to the xlog struct
			 * that assume that padding bytes are present.  If the palloc
			 * request is not large enough to include such padding bytes then
			 * we'll get valgrind complaints due to otherwise-harmless fetches
			 * of the padding bytes.
			 *
			 * In addition, force the initial request to be reasonably large
			 * so that we don't waste time with lots of trips through this
			 * stanza.  BLCKSZ / 2 seems like a good compromise choice.
			 */
			state->main_data_bufsz = MAXALIGN(Max(state->main_data_len,
												  BLCKSZ / 2));
			state->main_data = malloc(state->main_data_bufsz);
		}
		memcpy(state->main_data, ptr, state->main_data_len);
		ptr += state->main_data_len;
	}

	return true;

shortdata_err:
	printf(
						  "record with invalid length at %X/%X",
						  LSN_FORMAT_ARGS(state->ReadRecPtr));
err:
	*errormsg = state->errormsg_buf;

	return false;
}
#endif

/**
 * XLogReaderInvalReadState - Invalidate reader state
 *
 * @state: XLog reader state to invalidate
 *
 * Resets the segment number, offset, and read length to indicate
 * no valid data is loaded.
 */
void
XLogReaderInvalReadState(XLogReaderState *state)
{
	state->seg.ws_segno = 0;
	state->segoff = 0;
	state->readLen = 0;
}

/**
 * pg_pread - Positioned read from file descriptor
 *
 * @fd:     File descriptor
 * @buf:    Buffer to read into
 * @size:   Number of bytes to read
 * @offset: File offset to read from
 *
 * Seeks to the specified offset and reads data.
 *
 * Returns: Number of bytes read, or -1 on error
 */
size_t
pg_pread(int fd, void *buf, size_t size, off_t offset)
{
	if (lseek(fd, offset, SEEK_SET) < 0)
		return -1;
	return read(fd, buf, size);
}

/**
 * pg_pread_win - Positioned read from FILE pointer (Windows compatible)
 *
 * @fp:     FILE pointer
 * @buf:    Buffer to read into
 * @size:   Number of bytes to read
 * @offset: File offset to read from
 *
 * Seeks to the specified offset and reads data using fread.
 *
 * Returns: Number of bytes read, or -1 on error
 */
size_t
pg_pread_win(FILE *fp, void *buf, size_t size, off_t offset)
{
	if (fseek(fp, offset, SEEK_SET) < 0)
		return -1;

	return fread(buf,1, size, fp);
}

#if PG_VERSION_NUM > 14
/**
 * XLogReaderFree - Free XLog reader state (PG > 14)
 *
 * @state: XLog reader state to free
 *
 * Closes the segment file if open, frees the decode buffer,
 * error message buffer, record buffer, read buffer, and state itself.
 */
void
XLogReaderFree(XLogReaderState *state)
{
	if (state->seg.ws_file != -1)
		state->routine.segment_close(state);

	if (state->decode_buffer && state->free_decode_buffer)
		free(state->decode_buffer);

	free(state->errormsg_buf);
	if (state->readRecordBuf)
		free(state->readRecordBuf);
	free(state->readBuf);
	free(state);
}
/**
 * RestoreBlockImage - Restore a full-page image from backup block (PG > 14)
 *
 * @record:   XLog reader state containing the record
 * @block_id: Block ID to restore
 * @page:     Output buffer for restored page (BLCKSZ bytes)
 *
 * Decompresses the backup block image if necessary (PGLZ, LZ4, or ZSTD)
 * and reconstructs the full page including any hole.
 *
 * Returns: true if image was restored, false on error
 */
bool RestoreBlockImage(XLogReaderState *record, uint8 block_id, char *page)
{
	DecodedBkpBlock *bkpb;
	char	   *ptr;
	PGAlignedBlock tmp;

	if (block_id > record->record->max_block_id ||
		!record->record->blocks[block_id].in_use)
	{
		printf(
							  "could not restore image at %X/%X with invalid block %d specified",
							  LSN_FORMAT_ARGS(record->ReadRecPtr),
							  block_id);
		return false;
	}
	if (!record->record->blocks[block_id].has_image)
	{
		printf("could not restore image at %X/%X with invalid state, block %d",
							  LSN_FORMAT_ARGS(record->ReadRecPtr),
							  block_id);
		return false;
	}

	bkpb = &record->record->blocks[block_id];
	ptr = bkpb->bkp_image;

	if (BKPIMAGE_COMPRESSED(bkpb->bimg_info))
	{
		/* If a backup block image is compressed, decompress it */
		bool		decomp_success = true;

		if ((bkpb->bimg_info & BKPIMAGE_COMPRESS_PGLZ) != 0)
		{
			if (pglz_decompress(ptr, bkpb->bimg_len, tmp.data,
								BLCKSZ - bkpb->hole_length, true) < 0)
				decomp_success = false;
		}
		else if ((bkpb->bimg_info & BKPIMAGE_COMPRESS_LZ4) != 0)
		{
			if (LZ4_decompress_safe(ptr, tmp.data,
									bkpb->bimg_len, BLCKSZ - bkpb->hole_length) <= 0)
				decomp_success = false;
		}
		else if ((bkpb->bimg_info & BKPIMAGE_COMPRESS_ZSTD) != 0)
		{
#ifdef USE_ZSTD
			size_t		decomp_result = ZSTD_decompress(tmp.data,
														BLCKSZ - bkpb->hole_length,
														ptr, bkpb->bimg_len);

			if (ZSTD_isError(decomp_result))
				decomp_success = false;
#else
			printf("could not restore image at %X/%X compressed with %s not supported by build, block %d",
								  LSN_FORMAT_ARGS(record->ReadRecPtr),
								  "zstd",
								  block_id);
			return false;
#endif
		}
		else
		{
			printf("could not restore image at %X/%X compressed with unknown method, block %d",
								  LSN_FORMAT_ARGS(record->ReadRecPtr),
								  block_id);
			return false;
		}

		if (!decomp_success)
		{
			printf("could not decompress image at %X/%X, block %d",
								  LSN_FORMAT_ARGS(record->ReadRecPtr),
								  block_id);
			return false;
		}

		ptr = tmp.data;
	}

	/* generate page, taking into account hole if necessary */
	if (bkpb->hole_length == 0)
	{
		memcpy(page, ptr, BLCKSZ);
	}
	else
	{
		memcpy(page, ptr, bkpb->hole_offset);
		/* must zero-fill the hole */
		memset(page + bkpb->hole_offset, 0, bkpb->hole_length);
		memcpy(page + (bkpb->hole_offset + bkpb->hole_length),
			   ptr + bkpb->hole_offset,
			   BLCKSZ - (bkpb->hole_offset + bkpb->hole_length));
	}

	return true;
}

/**
 * XLogBeginRead - Initialize reader to start reading at given position (PG > 14)
 *
 * @state:  XLog reader state
 * @RecPtr: Record pointer to start reading from
 *
 * Resets the decoder and sets up the reader state to begin
 * reading from the specified position.
 */
void
XLogBeginRead(XLogReaderState *state, XLogRecPtr RecPtr)
{
	Assert(!XLogRecPtrIsInvalid(RecPtr));

	ResetDecoder(state);

	/* Begin at the passed-in record pointer. */
	state->EndRecPtr = RecPtr;
	state->NextRecPtr = RecPtr;
	state->ReadRecPtr = InvalidXLogRecPtr;
	state->DecodeRecPtr = InvalidXLogRecPtr;
}

/**
 * XLogRecGetBlockTagExtended - Get extended block tag info (PG > 14)
 *
 * @record:          XLog reader state
 * @block_id:        Block ID to query
 * @rnode:           Output RelFileNode (may be NULL)
 * @forknum:         Output fork number (may be NULL)
 * @blknum:          Output block number (may be NULL)
 * @prefetch_buffer: Output prefetch buffer (may be NULL)
 *
 * Returns: true if block reference exists, false otherwise
 */
bool
XLogRecGetBlockTagExtended(XLogReaderState *record, uint8 block_id,
						   RelFileNode *rnode, ForkNumber *forknum,
						   BlockNumber *blknum,
						   Buffer *prefetch_buffer)
{
	DecodedBkpBlock *bkpb;

	if (!XLogRecHasBlockRef(record, block_id))
		return false;

	bkpb = &record->record->blocks[block_id];
	if (rnode)
		*rnode = bkpb->rnode;
	if (forknum)
		*forknum = bkpb->forknum;
	if (blknum)
		*blknum = bkpb->blkno;
	if (prefetch_buffer)
		*prefetch_buffer = bkpb->prefetch_buffer;
	return true;
}

/**
 * XLogRecGetBlockTag - Get block tag info (PG > 14)
 *
 * @record:  XLog reader state
 * @block_id: Block ID to query
 * @rnode:   Output RelFileNode (may be NULL)
 * @forknum: Output fork number (may be NULL)
 * @blknum:  Output block number (may be NULL)
 *
 * Wrapper around XLogRecGetBlockTagExtended that asserts on missing blocks.
 */
void
XLogRecGetBlockTag(XLogReaderState *record, uint8 block_id,
				   RelFileNode *rnode, ForkNumber *forknum, BlockNumber *blknum)
{
	if (!XLogRecGetBlockTagExtended(record, block_id, rnode, forknum, blknum,
									NULL))
	{
		printf("could not locate backup block with ID %d in WAL record\n",
			 block_id);
	}
}
#else
/**
 * XLogReaderFree - Free XLog reader state (PG <= 14)
 *
 * @state: XLog reader state to free
 *
 * Closes the segment file if open, frees block data, main data,
 * error message buffer, record buffer, read buffer, and state itself.
 */
void
XLogReaderFree(XLogReaderState *state)
{
	int			block_id;

	if (state->seg.ws_file != -1)
		state->routine.segment_close(state);

	for (block_id = 0; block_id <= XLR_MAX_BLOCK_ID; block_id++)
	{
		if (state->blocks[block_id].data)
			free(state->blocks[block_id].data);
	}
	if (state->main_data)
		free(state->main_data);

	free(state->errormsg_buf);
	if (state->readRecordBuf)
		free(state->readRecordBuf);
	free(state->readBuf);
	free(state);
}

/**
 * RestoreBlockImage - Restore a full-page image from backup block (PG <= 14)
 *
 * @record:   XLog reader state containing the record
 * @block_id: Block ID to restore
 * @page:     Output buffer for restored page (BLCKSZ bytes)
 *
 * Decompresses the backup block image if necessary (PGLZ only)
 * and reconstructs the full page including any hole.
 *
 * Returns: true if image was restored, false on error
 */
bool
RestoreBlockImage(XLogReaderState *record, uint8 block_id, char *page)
{
	DecodedBkpBlock *bkpb;
	char	   *ptr;
	PGAlignedBlock tmp;

	if (!record->blocks[block_id].in_use)
		return false;
	if (!record->blocks[block_id].has_image)
		return false;

	bkpb = &record->blocks[block_id];
	ptr = bkpb->bkp_image;

	if (bkpb->bimg_info & BKPIMAGE_IS_COMPRESSED)
	{
		/* If a backup block image is compressed, decompress it */
		if (pglz_decompress(ptr, bkpb->bimg_len, tmp.data,
							BLCKSZ - bkpb->hole_length, true) < 0)
		{
			printf("invalid compressed image at %X/%X, block %d",
								  (uint32) (record->ReadRecPtr >> 32),
								  (uint32) record->ReadRecPtr,
								  block_id);
			return false;
		}
		ptr = tmp.data;
	}

	/* generate page, taking into account hole if necessary */
	if (bkpb->hole_length == 0)
	{
		memcpy(page, ptr, BLCKSZ);
	}
	else
	{
		memcpy(page, ptr, bkpb->hole_offset);
		/* must zero-fill the hole */
		MemSet(page + bkpb->hole_offset, 0, bkpb->hole_length);
		memcpy(page + (bkpb->hole_offset + bkpb->hole_length),
			   ptr + bkpb->hole_offset,
			   BLCKSZ - (bkpb->hole_offset + bkpb->hole_length));
	}

	return true;
}

/**
 * XLogBeginRead - Initialize reader to start reading at given position (PG <= 14)
 *
 * @state:  XLog reader state
 * @RecPtr: Record pointer to start reading from
 *
 * Resets the decoder and sets up the reader state to begin
 * reading from the specified position.
 */
void
XLogBeginRead(XLogReaderState *state, XLogRecPtr RecPtr)
{
	Assert(!XLogRecPtrIsInvalid(RecPtr));

	ResetDecoder(state);

	/* Begin at the passed-in record pointer. */
	state->EndRecPtr = RecPtr;
	state->ReadRecPtr = InvalidXLogRecPtr;
}

/**
 * XLogRecGetBlockTag - Get block tag info (PG <= 14)
 *
 * @record:  XLog reader state
 * @block_id: Block ID to query
 * @rnode:   Output RelFileNode (may be NULL)
 * @forknum: Output fork number (may be NULL)
 * @blknum:  Output block number (may be NULL)
 *
 * Returns: true if block reference exists, false otherwise
 */
bool
XLogRecGetBlockTag(XLogReaderState *record, uint8 block_id,
				   RelFileNode *rnode, ForkNumber *forknum, BlockNumber *blknum)
{
	DecodedBkpBlock *bkpb;

	if (!record->blocks[block_id].in_use)
		return false;

	bkpb = &record->blocks[block_id];
	if (rnode)
		*rnode = bkpb->rnode;
	if (forknum)
		*forknum = bkpb->forknum;
	if (blknum)
		*blknum = bkpb->blkno;
	return true;
}
#endif

/**
 * strlcpy - Copy string with size limit
 *
 * @dst: Destination buffer
 * @src: Source string
 * @siz: Size of destination buffer
 *
 * Copies at most siz-1 characters from src to dst, ensuring null termination.
 * Unlike strncpy, guarantees null termination and returns total length needed.
 *
 * Returns: Length of src (not including null terminator)
 */
size_t
strlcpy(char *dst, const char *src, size_t siz)
{
	char	   *d = dst;
	const char *s = src;
	size_t		n = siz;

	/* Copy as many bytes as will fit */
	if (n != 0)
	{
		while (--n != 0)
		{
			if ((*d++ = *s++) == '\0')
				break;
		}
	}

	/* Not enough room in dst, add NUL and traverse rest of src */
	if (n == 0)
	{
		if (siz != 0)
			*d = '\0';			/* NUL-terminate dst */
		while (*s++)
			;
	}

	return (s - src - 1);		/* count does not include NUL */
}

/**
 * ParseCommitRecord - Parse a transaction commit record
 *
 * @info:   Record info flags
 * @xlrec:  Raw commit record
 * @parsed: Output parsed commit structure
 *
 * Parses the commit record extracting database info, subxacts,
 * relfilenodes, invalidation messages, two-phase info, and origin.
 */
void
ParseCommitRecord(uint8 info, xl_xact_commit *xlrec, xl_xact_parsed_commit *parsed)
{
	char	   *data = ((char *) xlrec) + MinSizeOfXactCommit;

	memset(parsed, 0, sizeof(*parsed));

	parsed->xinfo = 0;			/* default, if no XLOG_XACT_HAS_INFO is
								 * present */

	parsed->xact_time = xlrec->xact_time;

	if (info & XLOG_XACT_HAS_INFO)
	{
		xl_xact_xinfo *xl_xinfo = (xl_xact_xinfo *) data;

		parsed->xinfo = xl_xinfo->xinfo;

		data += sizeof(xl_xact_xinfo);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_DBINFO)
	{
		xl_xact_dbinfo *xl_dbinfo = (xl_xact_dbinfo *) data;

		parsed->dbId = xl_dbinfo->dbId;
		parsed->tsId = xl_dbinfo->tsId;

		data += sizeof(xl_xact_dbinfo);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_SUBXACTS)
	{
		xl_xact_subxacts *xl_subxacts = (xl_xact_subxacts *) data;

		parsed->nsubxacts = xl_subxacts->nsubxacts;
		parsed->subxacts = xl_subxacts->subxacts;

		data += MinSizeOfXactSubxacts;
		data += parsed->nsubxacts * sizeof(TransactionId);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_RELFILENODES)
	{
		xl_xact_relfilenodes *xl_relfilenodes = (xl_xact_relfilenodes *) data;

		parsed->nrels = xl_relfilenodes->nrels;
		parsed->xnodes = xl_relfilenodes->xnodes;

		data += MinSizeOfXactRelfilenodes;
		data += xl_relfilenodes->nrels * sizeof(RelFileNode);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_INVALS)
	{
		xl_xact_invals *xl_invals = (xl_xact_invals *) data;

		parsed->nmsgs = xl_invals->nmsgs;
		parsed->msgs = xl_invals->msgs;

		data += MinSizeOfXactInvals;
		data += xl_invals->nmsgs * sizeof(SharedInvalidationMessage);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_TWOPHASE)
	{
		xl_xact_twophase *xl_twophase = (xl_xact_twophase *) data;

		parsed->twophase_xid = xl_twophase->xid;

		data += sizeof(xl_xact_twophase);

		if (parsed->xinfo & XACT_XINFO_HAS_GID)
		{
			strlcpy(parsed->twophase_gid, data, sizeof(parsed->twophase_gid));
			data += strlen(data) + 1;
		}
	}

	/* Note: no alignment is guaranteed after this point */

	if (parsed->xinfo & XACT_XINFO_HAS_ORIGIN)
	{
		xl_xact_origin xl_origin;

		/* no alignment is guaranteed, so copy onto stack */
		memcpy(&xl_origin, data, sizeof(xl_origin));

		parsed->origin_lsn = xl_origin.origin_lsn;
		parsed->origin_timestamp = xl_origin.origin_timestamp;

		data += sizeof(xl_xact_origin);
	}
}

/**
 * pvsnprintf - Safe vsnprintf wrapper
 *
 * @buf:  Output buffer
 * @len:  Buffer size
 * @fmt:  Format string
 * @args: Variable arguments
 *
 * Wrapper around vsnprintf that handles errors and returns
 * the space needed for the full formatted string.
 *
 * Returns: Number of characters needed (may exceed len)
 */
size_t
pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)
{
	int			nprinted;

	nprinted = vsnprintf(buf, len, fmt, args);

	/* We assume failure means the fmt is bogus, hence hard failure is OK */
	if (unlikely(nprinted < 0))
	{

		printf("vsnprintf failed with format string \"%s\"\n",
				 fmt);
		exit(1);
	}

	if ((size_t) nprinted < len)
	{
		/* Success.  Note nprinted does not include trailing null. */
		return (size_t) nprinted;
	}

	/*
	 * We assume a C99-compliant vsnprintf, so believe its estimate of the
	 * required space, and add one for the trailing null.  (If it's wrong, the
	 * logic will still work, but we may loop multiple times.)
	 *
	 * Choke if the required space would exceed MaxAllocSize.  Note we use
	 * this palloc-oriented overflow limit even when in frontend.
	 */
	if (unlikely((size_t) nprinted > MaxAllocSize - 1))
	{
		printf("out of memory\n");
		exit(1);
	}

	return nprinted + 1;
}
/**
 * appendStringInfoVA - Append formatted string using va_list
 *
 * @str:  StringInfo to append to
 * @fmt:  Format string
 * @args: Variable argument list
 *
 * Attempts to append formatted output to the StringInfo buffer.
 *
 * Returns: 0 on success, or bytes needed if buffer is too small
 */
int
appendStringInfoVA(StringInfo str, const char *fmt, va_list args)
{
	int			avail;
	size_t		nprinted;

	Assert(str != NULL);

	/*
	 * If there's hardly any space, don't bother trying, just fail to make the
	 * caller enlarge the buffer first.  We have to guess at how much to
	 * enlarge, since we're skipping the formatting work.
	 */
	avail = str->maxlen - str->len;
	if (avail < 16)
		return 32;

	nprinted = pvsnprintf(str->data + str->len, (size_t) avail, fmt, args);

	if (nprinted < (size_t) avail)
	{
		/* Success.  Note nprinted does not include trailing null. */
		str->len += (int) nprinted;
		return 0;
	}

	/* Restore the trailing null so that str is unmodified. */
	str->data[str->len] = '\0';

	/*
	 * Return pvsnprintf's estimate of the space needed.  (Although this is
	 * given as a size_t, we know it will fit in int because it's not more
	 * than MaxAllocSize.)
	 */
	return (int) nprinted;
}
/**
 * appendStringInfo - Append formatted string to StringInfo
 *
 * @str: StringInfo to append to
 * @fmt: Format string (printf-style)
 * @...: Variable arguments
 *
 * Appends a formatted string to the StringInfo, automatically
 * enlarging the buffer as needed.
 */
void
appendStringInfo(StringInfo str, const char *fmt,...)
{
	for (;;)
	{
		va_list		args;
		int			needed;

		/* Try to format the data. */
		va_start(args, fmt);
		needed = appendStringInfoVA(str, fmt, args);
		va_end(args);

		if (needed == 0)
			break;				/* success */

		/* Increase the buffer size and try again. */
		enlargeStringInfo(str, needed);
	}
}

/**
 * timestamptz_to_time_t - Convert PostgreSQL timestamp to Unix time_t
 *
 * @t: PostgreSQL TimestampTz value (microseconds since PG epoch)
 *
 * Converts a PostgreSQL timestamp with timezone to a Unix time_t value.
 *
 * Returns: Unix time_t value
 */
pg_time_t timestamptz_to_time_t(TimestampTz t)
{
    pg_time_t result;

    result = (pg_time_t)(t / USECS_PER_SEC + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));

    return result;
}

/**
 * timestamptz_to_str - Convert PostgreSQL timestamp to formatted string
 *
 * @dt: PostgreSQL TimestampTz value
 *
 * Converts a timestamp to a human-readable string with microseconds
 * and timezone. Caller must free the returned buffer.
 *
 * Returns: Pointer to allocated string buffer
 */
uintptr_t timestamptz_to_str(TimestampTz dt)
{
	char *buf = (char*)malloc(sizeof(char)*1024);
	char		ts[MAXDATELEN + 1];
	char		zone[MAXDATELEN + 1];
	time_t		result = (time_t) timestamptz_to_time_t(dt);
	struct tm  *ltime = localtime(&result);

	strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", ltime);
	strftime(zone, sizeof(zone), "%Z", ltime);

    sprintf(buf, "%s.%06d %s", ts, (int) (dt % USECS_PER_SEC), zone);

	return (uintptr_t)buf;
}

/**
 * xact_desc_commit - Extract commit record information
 *
 * @TimeFromRecord: Output transaction commit time
 * @datafileOid:    Output data file OID
 * @toastOid:       Output TOAST table OID
 * @xlrec:          Commit record data
 * @info:           Record info flags
 *
 * Parses a commit record and extracts the timestamp and relfilenodes
 * of affected tables.
 */
void
xact_desc_commit(TimestampTz *TimeFromRecord,Oid *datafileOid,Oid *toastOid, xl_xact_commit *xlrec,uint8 info)
{
	*TimeFromRecord = xlrec->xact_time;
	xl_xact_parsed_commit parsed;
	
	ParseCommitRecord(info, xlrec, &parsed);
	if(parsed.nrels > 0){
		for (int i = 0; i < parsed.nrels; i++)
		{
			if ( i == 0){
                *datafileOid = parsed.xnodes[i].relNode;
            }
            else if (i == 1){
                *toastOid = parsed.xnodes[i].relNode;
            }
			
		}
	}
	
}

/**
 * XlogRecGetBlkInfo - Get block info from XLog record (version-agnostic)
 *
 * @record:   XLog reader state
 * @block_id: Block ID to query
 * @blk:      Output block number
 * @rnode:    Output RelFileNode
 * @fork:     Output fork number
 *
 * Wrapper that calls the appropriate version-specific function.
 */
void XlogRecGetBlkInfo(XLogReaderState *record,int	block_id, BlockNumber *blk, RelFileNode *rnode, ForkNumber	*fork)
{
	#if PG_VERSION_NUM > 14
	(void) XLogRecGetBlockTagExtended(record, block_id,rnode, fork, blk, NULL);
	#else
	XLogRecGetBlockTag(record, block_id,rnode, fork, blk);
	#endif
}

/**
 * xact_desc_pg_del - Extract delete record information
 *
 * @TimeFromRecord: Output transaction time (for commit records)
 * @datafileOid:    Output data file OID
 * @record:         XLog reader state
 * @rmid:           Resource manager ID
 *
 * Extracts OID from heap delete records or timestamp from commit records.
 */
void xact_desc_pg_del(TimestampTz *TimeFromRecord,Oid *datafileOid, XLogReaderState *record,RmgrId rmid)
{

	if(rmid == HEAP_redo){
		uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
		if (info == XLOG_HEAP_DELETE)
		{			
			int block_id;
			for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
			{
				RelFileNode rlocator;
				ForkNumber	forknum;
				BlockNumber blk;

				#if PG_VERSION_NUM > 14
				(void) XLogRecGetBlockTagExtended(record, block_id,&rlocator, &forknum, &blk, NULL);
				#else
				XLogRecGetBlockTag(record, block_id,&rlocator, &forknum, &blk);
				#endif
				*datafileOid=rlocator.relNode;
			}
		}
	}
	else if (rmid == TRANSACTION_redo){
		char	   *rec = XLogRecGetData(record);
		uint8		info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;
		if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
		{
			xl_xact_commit *xlrec = (xl_xact_commit *) rec;
			*TimeFromRecord = xlrec->xact_time;
		}
	}

}

/**
 * xact_desc_pg_upd - Extract update record information
 *
 * @TimeFromRecord: Output transaction time (for commit records)
 * @datafileOid:    Output data file OID
 * @record:         XLog reader state
 * @rmid:           Resource manager ID
 *
 * Extracts OID from heap update records or timestamp from commit records.
 */
void xact_desc_pg_upd(TimestampTz *TimeFromRecord,Oid *datafileOid, XLogReaderState *record,RmgrId rmid)
{

	if(rmid == HEAP_redo){
		uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
		if (isUpdate(info))
		{			
			int block_id;
			for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
			{
				RelFileNode rlocator;
				ForkNumber	forknum;
				BlockNumber blk;

				#if PG_VERSION_NUM > 14
				(void) XLogRecGetBlockTagExtended(record, block_id,&rlocator, &forknum, &blk, NULL);
				#else
				XLogRecGetBlockTag(record, block_id,&rlocator, &forknum, &blk);
				#endif
				*datafileOid=rlocator.relNode;
			}
		}
	}
	else if (rmid == TRANSACTION_redo){
		char	   *rec = XLogRecGetData(record);
		uint8		info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;
		if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
		{
			xl_xact_commit *xlrec = (xl_xact_commit *) rec;
			*TimeFromRecord = xlrec->xact_time;
		}
	}
}

/**
 * PageAddItemExtended - Add an item to a page
 *
 *	Add an item to a page.  Return value is the offset at which it was
 *	inserted, or InvalidOffsetNumber if the item is not inserted for any
 *	reason.  A WARNING is issued indicating the reason for the refusal.
 *
 *	offsetNumber must be either InvalidOffsetNumber to specify finding a
 *	free line pointer, or a value between FirstOffsetNumber and one past
 *	the last existing item, to specify using that particular line pointer.
 *
 *	If offsetNumber is valid and flag PAI_OVERWRITE is set, we just store
 *	the item at the specified offsetNumber, which must be either a
 *	currently-unused line pointer, or one past the last existing item.
 *
 *	If offsetNumber is valid and flag PAI_OVERWRITE is not set, insert
 *	the item at the specified offsetNumber, moving existing items later
 *	in the array to make room.
 *
 *	If offsetNumber is not valid, then assign a slot by finding the first
 *	one that is both unused and deallocated.
 *
 *	If flag PAI_IS_HEAP is set, we enforce that there can't be more than
 *	MaxHeapTuplesPerPage line pointers on the page.
 *
 *	!!! EREPORT(ERROR) IS DISALLOWED HERE !!!
 */
OffsetNumber
PageAddItemExtended(Page page,
					Item item,
					Size size,
					OffsetNumber offsetNumber,
					int flags)
{
	PageHeader	phdr = (PageHeader) page;
	Size		alignedSize;
	int			lower;
	int			upper;
	ItemId		itemId;
	OffsetNumber limit;
	bool		needshuffle = false;

	/*
	 * Be wary about corrupted page pointers
	 */
	if (phdr->pd_lower < SizeOfPageHeaderData ||
		phdr->pd_lower > phdr->pd_upper ||
		phdr->pd_upper > phdr->pd_special ||
		phdr->pd_special > BLCKSZ){
			printf("corrupted page pointers: lower = %u, upper = %u, special = %u\n",
						phdr->pd_lower, phdr->pd_upper, phdr->pd_special);
			return InvalidOffsetNumber;
		}

	/*
	 * Select offsetNumber to place the new item at
	 */
	limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));

	/* was offsetNumber passed in? */
	if (OffsetNumberIsValid(offsetNumber))
	{
		/* yes, check it */
		if ((flags & PAI_OVERWRITE) != 0)
		{
			if (offsetNumber < limit)
			{
				itemId = PageGetItemId(phdr, offsetNumber);
				if (ItemIdIsUsed(itemId) || ItemIdHasStorage(itemId))
				{
					return InvalidOffsetNumber;
				}
			}
		}
		else
		{
			if (offsetNumber < limit)
				needshuffle = true; /* need to move existing linp's */
		}
	}
	else
	{
		/* offsetNumber was not passed in, so find a free slot */
		/* if no free slot, we'll put it at limit (1st open slot) */
		if (PageHasFreeLinePointers(phdr))
		{
			/*
			 * Scan line pointer array to locate a "recyclable" (unused)
			 * ItemId.
			 *
			 * Always use earlier items first.  PageTruncateLinePointerArray
			 * can only truncate unused items when they appear as a contiguous
			 * group at the end of the line pointer array.
			 */
			for (offsetNumber = FirstOffsetNumber;
				 offsetNumber < limit;	/* limit is maxoff+1 */
				 offsetNumber++)
			{
				itemId = PageGetItemId(phdr, offsetNumber);

				/*
				 * We check for no storage as well, just to be paranoid;
				 * unused items should never have storage.  Assert() that the
				 * invariant is respected too.
				 */
				Assert(ItemIdIsUsed(itemId) || !ItemIdHasStorage(itemId));

				if (!ItemIdIsUsed(itemId) && !ItemIdHasStorage(itemId))
					break;
			}
			if (offsetNumber >= limit)
			{
				/* the hint is wrong, so reset it */
				PageClearHasFreeLinePointers(phdr);
			}
		}
		else
		{
			/* don't bother searching if hint says there's no free slot */
			offsetNumber = limit;
		}
	}

	/* Reject placing items beyond the first unused line pointer */
	if (offsetNumber > limit)
	{
		return InvalidOffsetNumber;
	}

	/* Reject placing items beyond heap boundary, if heap */
	if ((flags & PAI_IS_HEAP) != 0 && offsetNumber > MaxHeapTuplesPerPage)
	{
		return InvalidOffsetNumber;
	}

	/*
	 * Compute new lower and upper pointers for page, see if it'll fit.
	 *
	 * Note: do arithmetic as signed ints, to avoid mistakes if, say,
	 * alignedSize > pd_upper.
	 */
	if (offsetNumber == limit || needshuffle)
		lower = phdr->pd_lower + sizeof(ItemIdData);
	else
		lower = phdr->pd_lower;

	alignedSize = MAXALIGN(size);

	upper = (int) phdr->pd_upper - (int) alignedSize;

	if (lower > upper)
		return InvalidOffsetNumber;

	/*
	 * OK to insert the item.  First, shuffle the existing pointers if needed.
	 */
	itemId = PageGetItemId(phdr, offsetNumber);

	if (needshuffle)
		memmove(itemId + 1, itemId,
				(limit - offsetNumber) * sizeof(ItemIdData));

	/* set the line pointer */
	ItemIdSetNormal(itemId, upper, size);

	/*
	 * Items normally contain no uninitialized bytes.  Core bufpage consumers
	 * conform, but this is not a necessary coding rule; a new index AM could
	 * opt to depart from it.  However, data type input functions and other
	 * C-language functions that synthesize datums should initialize all
	 * bytes; datumIsEqual() relies on this.  Testing here, along with the
	 * similar check in printtup(), helps to catch such mistakes.
	 *
	 * Values of the "name" type retrieved via index-only scans may contain
	 * uninitialized bytes; see comment in btrescan().  Valgrind will report
	 * this as an error, but it is safe to ignore.
	 */
	VALGRIND_CHECK_MEM_IS_DEFINED(item, size);

	/* copy the item's data onto the page */
	memcpy((char *) page + upper, item, size);

	/* adjust page header */
	phdr->pd_lower = (LocationIndex) lower;
	phdr->pd_upper = (LocationIndex) upper;

	return offsetNumber;
}

/**
 * PageInit - Initialize a page header
 *
 * @page:        Pointer to page buffer
 * @pageSize:    Size of page (must be BLCKSZ)
 * @specialSize: Size of special area at end of page
 *
 * Initializes a page header with default values, setting up the
 * lower, upper, and special pointers.
 */
void
PageInit(Page page, Size pageSize, Size specialSize)
{
	PageHeader	p = (PageHeader) page;

	specialSize = MAXALIGN(specialSize);

	Assert(pageSize == BLCKSZ);
	Assert(pageSize > specialSize + SizeOfPageHeaderData);

	/* Make sure all fields of page are zero, as well as unused space */
	MemSet(p, 0, pageSize);

	p->pd_flags = 0;
	p->pd_lower = SizeOfPageHeaderData;
	p->pd_upper = pageSize - specialSize;
	p->pd_special = pageSize - specialSize;
	PageSetPageSizeAndVersion(page, pageSize, PG_PAGE_LAYOUT_VERSION);
}

/**
 * fix_infomask_from_infobits - Convert WAL infobits to heap tuple infomask
 *
 * @infobits:  Input WAL record infobits
 * @infomask: Output tuple infomask (modified in place)
 * @infomask2: Output tuple infomask2 (modified in place)
 *
 * Translates WAL-logged infobits back to the corresponding heap tuple
 * infomask bits for XMAX-related flags.
 */
void
fix_infomask_from_infobits(uint8 infobits, uint16 *infomask, uint16 *infomask2)
{
	*infomask &= ~(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY |
				   HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_EXCL_LOCK);
	*infomask2 &= ~HEAP_KEYS_UPDATED;

	if (infobits & XLHL_XMAX_IS_MULTI)
		*infomask |= HEAP_XMAX_IS_MULTI;
	if (infobits & XLHL_XMAX_LOCK_ONLY)
		*infomask |= HEAP_XMAX_LOCK_ONLY;
	if (infobits & XLHL_XMAX_EXCL_LOCK)
		*infomask |= HEAP_XMAX_EXCL_LOCK;
	/* note HEAP_XMAX_SHR_LOCK isn't considered here */
	if (infobits & XLHL_XMAX_KEYSHR_LOCK)
		*infomask |= HEAP_XMAX_KEYSHR_LOCK;

	if (infobits & XLHL_KEYS_UPDATED)
		*infomask2 |= HEAP_KEYS_UPDATED;
}

/**
 * TransactionIdPrecedes - Check if one transaction ID precedes another
 *
 * @id1: First transaction ID
 * @id2: Second transaction ID
 *
 * Compares two transaction IDs using modulo-2^32 arithmetic for
 * normal XIDs, or simple comparison for special XIDs.
 *
 * Returns: true if id1 precedes id2
 */
bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2)
{
	/*
	 * If either ID is a permanent XID then we can just do unsigned
	 * comparison.  If both are normal, do a modulo-2^32 comparison.
	 */
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 < id2);

	diff = (int32) (id1 - id2);
	return (diff < 0);
}

/**
 * isUpdate - Check if XLog info indicates an update operation
 *
 * @info: XLog record info byte
 *
 * Checks whether the info byte represents a heap update or hot update.
 *
 * Returns: true if the record is an update operation
 */
bool isUpdate(uint8 info)
{
	if( info == XLOG_HEAP_UPDATE || info == XLOG_HEAP_HOT_UPDATE ||
		info == (XLOG_HEAP_UPDATE | XLOG_HEAP_INIT_PAGE) || info == (XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE))
	{
		return true;
	}
	else
	{
		return false;
	}
}
