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
#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "decode.h"

#define UUID_LEN 16

#define sizeLtZero 1
#define resLtZero 2
#define sizeNotZero 3

typedef enum {
	PARSE_OK = 0,
	PARSE_ERR_ALIGNMENT = -101,
	PARSE_ERR_INSUFFICIENT = -102,
	PARSE_ERR_FORMAT = -103,
	PARSE_ERR_DECOMPRESS = -104,
	PARSE_ERR_MEMORY = -105
} ParseResultCode;

#define VFMT_UNKNOWN     0x00
#define VFMT_SHORT       0x01
#define VFMT_LONG_PLAIN  0x02
#define VFMT_LONG_COMP   0x04
#define VFMT_EXTERNAL    0x08

static int serializeInt8(const char *src, unsigned int avail, unsigned int *used);
static int serializeInt16(const char *src, unsigned int avail, unsigned int *used);
static int serializeInt32(const char *src, unsigned int avail, unsigned int *used);
static int serializeInt64(const char *src, unsigned int avail, unsigned int *used);

static int serializeFloat32(const char *src, unsigned int avail, unsigned int *used);
static int serializeFloat64(const char *src, unsigned int avail, unsigned int *used);

static int char_output(const char *src, unsigned int avail, unsigned int *used);
static int get_str_from_numeric(const char *buffer, int num_size);
static int emitEncodedValue(const char *str, int orig_len);
int decode_numeric_value(const char *input_buffer, unsigned int buffer_size, unsigned int *bytes_processed);
int numeric_outputds(const char *raw_input, unsigned int buf_len, unsigned int *bytes_read);

static int date_output(const char *src, unsigned int avail, unsigned int *used);
static int time_output(const char *src, unsigned int avail, unsigned int *used);
static int timetz_output(const char *src, unsigned int avail, unsigned int *used);
static int timestamp_output(const char *src, unsigned int avail, unsigned int *used);
static int timestamptz_output(const char *src, unsigned int avail, unsigned int *used);

int go=0;
int global_curr_att;

typedef struct {
	const char *typeName;
	decodeFunc handler;
	int8_t alignment;
	int16_t fixedWidth;
} ColumnTypeHandler;

static const ColumnTypeHandler typeHandlerRegistry[] = {
	{"tinyint",      serializeInt8,          1,   1},
	{"smallint",     serializeInt16,         2,   2},
	{"int",          serializeInt32,         4,   4},
	{"bigint",       serializeInt64,         8,   8},
	{"smallserial",  serializeInt16,         2,   2},
	{"serial",       serializeInt32,         4,   4},
	{"bigserial",    serializeInt64,         8,   8},
	{"oid",          serializeInt32,         4,   4},
	{"xid",          serializeInt32,         4,   4},
	{"real",         serializeFloat32,       4,   4},
	{"float4",       serializeFloat32,       4,   4},
	{"float8",       serializeFloat64,       8,   8},
	{"numeric",      decode_numeric_value,  -1,  -1},
	{"time",         time_output,            8,   8},
	{"timetz",       timetz_output,          8,  12},
	{"date",         date_output,            4,   4},
	{"timestamp",    timestamp_output,       8,   8},
	{"timestamptz",  timestamptz_output,     8,   8},
	{"uuid",         uuid_output,            1,  16},
	{"name",         name_output,            1,  64},
	{"bool",         bool_output,            1,   1},
	{"macaddr",      decode_macaddr,         4,   6},
	{"char",         char_output,            1,   1},
	{"varchar",      parse_text_field,      -1,  -1},
	{"charn",        parse_text_field,      -1,  -1},
	{"clob",         parse_text_field,      -1,  -1},
	{"blob",         parse_text_field,      -1,  -1},
	{"bit",          decode_bit,            -1,  -1},
	{"varbit",       decode_bit,            -1,  -1},
	{"pass",         No_op,                  0,   0},
	{NULL,           NULL,                   0,   0}
};

char CURDBPathforDB[1024]="";
char CURDBPath[1024]="";
char toastId[50]="";

int loglevel_decode;

typedef struct {
	StringInfoData storage;
	int fieldCount;
	bool ready;
} ResultAccumulator;

static ResultAccumulator g_resultBuf = {{0}, 0, false};

static parray *xmanNewParray = NULL;
static bool xmanNewParrayInitDown = false;
static bool xmanNewParrayReturn = false;

static parray *xmanOldParray = NULL;
static bool xmanOldParrayInitDown = false;
static bool xmanOldParrayReturn = false;


void initNewParray(){
	xmanNewParray = parray_new();
	xmanNewParrayInitDown = true;
}

void initOldParray(){
	xmanOldParray = parray_new();
	xmanOldParrayInitDown = true;
}

void freeNewParray(){
	parray_free(xmanNewParray);
	xmanNewParrayInitDown = false;
	xmanNewParrayReturn = false;
}

void freeOldParray(){
	parray_free(xmanOldParray);
	xmanOldParrayInitDown = false;
	xmanOldParrayReturn = false;
}

static inline void prepareResultBuffer(void)
{
	if (g_resultBuf.ready)
		return;
	initStringInfo(&g_resultBuf.storage);
	g_resultBuf.fieldCount = 0;
	g_resultBuf.ready = true;
}

int addNum=0;

static char decompression_storage[64 * 1024];

char *tmpChunk=NULL;
int tmpChunkSize=0;
int tmpChunkFLag=0;

int isToastDecoded = 0;
dropContext *dc = NULL;
harray *toastHash_decode = NULL;

Oid ErrToastIdNoths = 0;

Oid getErrToastOidNoths(){
	return ErrToastIdNoths;
}

int ExportMode_decode = CSVform;

/**
 * setExportMode_decode - Set output format mode
 *
 * @setting: Export mode (CSVform or SQLform)
 *
 * Configures decoder output format.
 */
void setExportMode_decode(int setting){
	ExportMode_decode = setting;
}

/**
 * setlogLevel - Set decode logging level
 *
 * @setting: Log level value
 *
 * Configures verbosity of decode operation logging.
 */
void setlogLevel(int setting){
	loglevel_decode = setting;
}

/**
 * setIsToastDecoded - Set TOAST decode flag
 *
 * @setting: Flag value
 *
 * Indicates whether TOAST data should be decoded.
 */
void setIsToastDecoded(int setting){
	isToastDecoded = setting;
}

/**
 * setDropContext - Set drop context pointer
 *
 * @setting: Drop context pointer
 *
 * Sets the context for dropped table recovery operations.
 */
void setDropContext(dropContext *setting){
	dc = setting;
}

/**
 * setToastHash - Set TOAST hash array
 *
 * @setting: Hash array pointer
 *
 * Sets the hash array for TOAST chunk lookup.
 */
void setToastHash(harray *setting){
	toastHash_decode = setting;
}

int resTyp_decode = DELETEtyp;

/**
 * setResTyp_decode - Set result type for decode
 *
 * @setting: Result type (DELETEtyp or UPDATEtyp)
 *
 * Configures the type of records to decode.
 */
void setResTyp_decode(int setting){
	resTyp_decode = setting;
}

/**
 * showSupportTypeCom - Display supported data types
 *
 * Prints list of all PostgreSQL data types supported by the decoder.
 */
void showSupportTypeCom()
{
    printf("%s-------------.-------------%s\n", COLOR_UNLOAD, C_RESET);

	int idx = 0;
	int displayed = 0;
	while (typeHandlerRegistry[idx].typeName != NULL) {
		const char *name = typeHandlerRegistry[idx].typeName;
		if (strcmp(name, "~") != 0) {
			char formatted[20] = {0};
			if (name[0] == '_') {
				snprintf(formatted, sizeof(formatted), "%s[]", name + 1);
			} else {
				strncpy(formatted, name, sizeof(formatted) - 1);
			}
			printf("%s%-15s%s", COLOR_UNLOAD, formatted, C_RESET);
			displayed++;
			if (displayed % 2 == 0)
				printf("\n");
		}
		idx++;
	}
	printf("\n\n");
}

unsigned int
determinePageDimension(FILE *fileHandle)
{
	PageHeaderData headerSample;
	size_t fetchedCount;
	unsigned int dimension = 0;

	fetchedCount = fread(&headerSample, sizeof(PageHeaderData), 1, fileHandle);
	fseek(fileHandle, 0L, SEEK_SET);

	if (fetchedCount == 1)
		dimension = (unsigned int) PageGetPageSize((Page)&headerSample);

	return dimension;
}

/**
 * decodeLogPrint - Log decode errors and diagnostic information
 *
 * @logErr:   File pointer for error logging output
 * @content:  Parsed tuple data content string
 * @flag:     Error type indicator (sizeLtZero, resLtZero, sizeNotZero)
 * @curr_att: Current attribute/field number being processed
 * @size:     Remaining bytes in tuple
 *
 * Outputs diagnostic messages when tuple parsing encounters errors.
 * Only active when log level is set to xmanDecodeLog.
 */
void decodeLogPrint(FILE *logErr,char *content,int flag,int curr_att,int size)
{
	if(loglevel_decode == xmanDecodeLog){
		char err[strlen(content)+100];
		if(flag == sizeLtZero){
			if(loglevel_decode == xmanDecodeLog){
				sprintf(err,"\nParsed field %d but tuple length is already less than zero, all parsed data: %s",curr_att+1,content);
			}
		}
		else if(flag == resLtZero){
			if(loglevel_decode == xmanDecodeLog){
				sprintf(err,"\nField %d decode function returned error, all parsed data: %s,",curr_att + 1, content);
			}
		}
		else if(flag == sizeNotZero){
			if(loglevel_decode == xmanDecodeLog){
				sprintf(err,"\nAll %d fields parsed, but tuple has %d bytes remaining, all parsed data: %s",curr_att,size, content);
			}
		}
		fputs(err,logErr);
	}
}

/**
 * initCURDBPath - Initialize current database path
 *
 * @filepath: Path to set as current database path
 *
 * Sets the global current database path variable.
 */
void initCURDBPath(char *filepath){
	memset(CURDBPath,0,1024);
	strcpy(CURDBPath,filepath);
}

/**
 * initCURDBPathforDB - Initialize database-specific path
 *
 * @filepath: Path for the database
 *
 * Sets the database-specific path variable.
 */
void initCURDBPathforDB(char *filepath){
	memset(CURDBPathforDB,0,1024);
	strcpy(CURDBPathforDB,filepath);
}

/**
 * initToastId - Initialize TOAST table identifier
 *
 * @toastnode: TOAST node identifier string
 *
 * Sets the current TOAST table ID for chunk assembly.
 */
void initToastId(char *toastnode){
	memset(toastId,0,50);
	strcpy(toastId,toastnode);
}

static void
emitFieldValue(const char *val)
{
	if (resTyp_decode == DELETEtyp) {
		prepareResultBuffer();
		if (val == NULL)
			return;

		if (g_resultBuf.fieldCount > 0) {
			char delimiter = (ExportMode_decode == SQLform) ? ',' : '\t';
			if (global_curr_att != 0){
				appendStringInfoChar(&g_resultBuf.storage, delimiter);
			}
		}
		appendStringInfoString(&g_resultBuf.storage, val);
		g_resultBuf.fieldCount++;
	}
	else if (resTyp_decode == UPDATEtyp) {
		if (!xmanOldParrayInitDown) {
			initOldParray();	
		}
		else if (!xmanNewParrayInitDown && xmanOldParrayReturn) {
			initNewParray();
		}
		if (val == NULL)
			return;

		char *duplicated = strdup(val);
		if (!xmanOldParrayReturn)
			parray_append(xmanOldParray, duplicated);
		else
			parray_append(xmanNewParray, duplicated);
	}
}

static void
emitArrayElement(const char *elem)
{
	prepareResultBuffer();
	if (elem == NULL)
		return;

	int len = strlen(g_resultBuf.storage.data);
	char trailing = (len > 0) ? g_resultBuf.storage.data[len - 1] : '\0';

	bool needComma = (g_resultBuf.storage.data[0] != '\0' &&
					  *elem != '{' && strcmp(elem, "\"") != 0 &&
					  *elem != '}' && strcmp(g_resultBuf.storage.data, "{") != 0 &&
					  trailing != '{' && trailing != '"');

	if (strcmp(elem, "\"") == 0 && trailing == '"')
		needComma = true;

	if (needComma)
		appendStringInfoChar(&g_resultBuf.storage, ',');

	if (*elem == '{' && global_curr_att != 0)
		appendStringInfoChar(&g_resultBuf.storage, '\t');

	appendStringInfoString(&g_resultBuf.storage, elem);
}

static void
dispatchFieldOutput(bool arrayMode, const char *content)
{
	if (content == NULL)
		return;
	if (arrayMode)
		emitArrayElement(content);
	else
		emitFieldValue(content);
}

static void
formatAndEmitCore(bool to_array, const char *fmt, va_list ap)
{
	char		small[128];
	char	   *buf = small;
	size_t		cap = sizeof(small);

	small[0] = '\0';

	for (;;)
	{
		va_list trial;
		int			written;

		va_copy(trial, ap);
		written = vsnprintf(buf, cap, fmt, trial);
		va_end(trial);

		if (written >= 0 && (size_t) written < cap)
			break;

		if (written < 0)
		{
			dispatchFieldOutput(to_array, "");
			if (buf != small)
				free(buf);
			return;
		}

		cap = (size_t) written + 1;

		if (buf == small)
		{
			buf = (char *) malloc(cap);
			if (buf == NULL)
			{
				dispatchFieldOutput(to_array, small);
				return;
			}
		}
		else
		{
			char *grown = (char *) realloc(buf, cap);

			if (grown == NULL)
			{
				free(buf);
				dispatchFieldOutput(to_array, small);
				return;
			}
			buf = grown;
		}
	}

	dispatchFieldOutput(to_array, buf);

	if (buf != small)
		free(buf);
}

static void
clean_out(void)
{
	if(resTyp_decode == UPDATEtyp){
		if(xmanOldParrayReturn && xmanNewParrayReturn){
			freeOldParray();
			freeNewParray();
		}
	}
	else if(resTyp_decode == DELETEtyp)
	{
		emitFieldValue(NULL);
		resetStringInfo(&g_resultBuf.storage);
	}

}

void
*return_out(void)
{
	if(resTyp_decode == DELETEtyp){
		emitFieldValue(NULL);
		return g_resultBuf.storage.data;
	}
	else{
		if(!xmanOldParrayReturn){
			xmanOldParrayReturn = true;
			return xmanOldParray;
		}
		else if(!xmanNewParrayReturn){
			xmanNewParrayReturn = true;
			return xmanNewParray;
		}
	}
	return NULL;
}

static void emitFormattedValue(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	formatAndEmitCore(false, fmt, ap);
	va_end(ap);
}

/* Digit lookup table - derived from PostgreSQL numutils.c */
static const char DIGIT_TABLE[200] =
"00" "01" "02" "03" "04" "05" "06" "07" "08" "09"
"10" "11" "12" "13" "14" "15" "16" "17" "18" "19"
"20" "21" "22" "23" "24" "25" "26" "27" "28" "29"
"30" "31" "32" "33" "34" "35" "36" "37" "38" "39"
"40" "41" "42" "43" "44" "45" "46" "47" "48" "49"
"50" "51" "52" "53" "54" "55" "56" "57" "58" "59"
"60" "61" "62" "63" "64" "65" "66" "67" "68" "69"
"70" "71" "72" "73" "74" "75" "76" "77" "78" "79"
"80" "81" "82" "83" "84" "85" "86" "87" "88" "89"
"90" "91" "92" "93" "94" "95" "96" "97" "98" "99";

/* Calculate decimal length of 32-bit integer - derived from PostgreSQL */
static inline int
decimalLength32(const uint32 v)
{
	int t;
	static const uint32 PowersOfTen[] = {
		1, 10, 100,
		1000, 10000, 100000,
		1000000, 10000000, 100000000,
		1000000000
	};

	t = (31 - __builtin_clz(v | 1) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

/* Calculate decimal length of 64-bit integer - derived from PostgreSQL */
static inline int
decimalLength64(const uint64 v)
{
	int t;
	static const uint64 PowersOfTen[] = {
		UINT64CONST(1), UINT64CONST(10),
		UINT64CONST(100), UINT64CONST(1000),
		UINT64CONST(10000), UINT64CONST(100000),
		UINT64CONST(1000000), UINT64CONST(10000000),
		UINT64CONST(100000000), UINT64CONST(1000000000),
		UINT64CONST(10000000000), UINT64CONST(100000000000),
		UINT64CONST(1000000000000), UINT64CONST(10000000000000),
		UINT64CONST(100000000000000), UINT64CONST(1000000000000000),
		UINT64CONST(10000000000000000), UINT64CONST(100000000000000000),
		UINT64CONST(1000000000000000000), UINT64CONST(10000000000000000000)
	};

	t = (63 - __builtin_clzll(v | 1) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

/* Convert unsigned 32-bit integer to string (without NUL terminator) - derived from PostgreSQL */
static int
pg_ultoa_n(uint32 value, char *a)
{
	int olength, i = 0;

	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength32(value);

	while (value >= 10000)
	{
		const uint32 c = value - 10000 * (value / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;
		char *pos = a + olength - i;

		value /= 10000;
		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value >= 100)
	{
		const uint32 c = (value % 100) << 1;
		char *pos = a + olength - i;

		value /= 100;
		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value >= 10)
	{
		const uint32 c = value << 1;
		char *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
	{
		*a = (char) ('0' + value);
	}

	return olength;
}

/* Convert signed 32-bit integer to string - derived from PostgreSQL */
static int
pg_ltoa(int32 value, char *a)
{
	uint32 uvalue = (uint32) value;
	int len = 0;

	if (value < 0)
	{
		uvalue = (uint32) 0 - uvalue;
		a[len++] = '-';
	}
	len += pg_ultoa_n(uvalue, a + len);
	a[len] = '\0';
	return len;
}

/* Convert unsigned 64-bit integer to string (without NUL terminator) - derived from PostgreSQL */
static int
pg_ulltoa_n(uint64 value, char *a)
{
	int olength, i = 0;
	uint32 value2;

	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength64(value);

	while (value >= 100000000)
	{
		const uint64 q = value / 100000000;
		uint32 value3 = (uint32) (value - 100000000 * q);
		const uint32 c = value3 % 10000;
		const uint32 d = value3 / 10000;
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;
		const uint32 d0 = (d % 100) << 1;
		const uint32 d1 = (d / 100) << 1;
		char *pos = a + olength - i;

		value = q;
		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		memcpy(pos - 6, DIGIT_TABLE + d0, 2);
		memcpy(pos - 8, DIGIT_TABLE + d1, 2);
		i += 8;
	}

	value2 = (uint32) value;

	if (value2 >= 10000)
	{
		const uint32 c = value2 - 10000 * (value2 / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;
		char *pos = a + olength - i;

		value2 /= 10000;
		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value2 >= 100)
	{
		const uint32 c = (value2 % 100) << 1;
		char *pos = a + olength - i;

		value2 /= 100;
		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value2 >= 10)
	{
		const uint32 c = value2 << 1;
		char *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
		*a = (char) ('0' + value2);

	return olength;
}

/* Convert signed 64-bit integer to string - derived from PostgreSQL */
static int
pg_lltoa(int64 value, char *a)
{
	uint64 uvalue = value;
	int len = 0;

	if (value < 0)
	{
		uvalue = (uint64) 0 - uvalue;
		a[len++] = '-';
	}

	len += pg_ulltoa_n(uvalue, a + len);
	a[len] = '\0';
	return len;
}

/* Convert unsigned integer to string with leading zeros - derived from PostgreSQL */
static char *
pg_ultostr_zeropad(char *str, uint32 value, int32 minwidth)
{
	int len;

	if (value < 100 && minwidth == 2)
	{
		memcpy(str, DIGIT_TABLE + value * 2, 2);
		return str + 2;
	}

	len = pg_ultoa_n(value, str);
	if (len >= minwidth)
		return str + len;

	memmove(str + minwidth - len, str, len);
	memset(str, '0', minwidth - len);
	return str + minwidth;
}

/**
 * serializeInt32 - Decode int32 value from buffer
 *
 * @src:   Pointer to source data buffer
 * @avail: Available buffer size
 * @used:  Output parameter for bytes consumed
 *
 * Decodes a 32-bit integer from the buffer and converts it to string.
 * Uses divide-and-conquer optimization to reduce division operations.
 *
 * Returns: PARSE_OK on success, negative ParseResultCode on error
 */
static int
serializeInt32(const char *src, unsigned int avail, unsigned int *used)
{
	uintptr_t location = (uintptr_t) src;
	unsigned int gap = (unsigned int)(((location + 3) & ~3UL) - location);
	const char *dataStart;
	int32 rawValue;
	char repr[16];
	char *cursor;
	uint32 magnitude;
	bool signBit;

	if (avail < gap)
		return PARSE_ERR_ALIGNMENT;

	dataStart = src + gap;
	avail -= gap;

	if (avail < sizeof(int32))
		return PARSE_ERR_INSUFFICIENT;

	rawValue = *(int32 *) dataStart;

	cursor = repr + sizeof(repr) - 1;
	*cursor = '\0';

	signBit = (rawValue < 0);
	magnitude = signBit ? (uint32)(-(int64)rawValue) : (uint32)rawValue;

	if (magnitude == 0) {
		*--cursor = '0';
	} else {
		while (magnitude >= 100) {
			uint32 twoDigits = magnitude % 100;
			magnitude /= 100;
			*--cursor = '0' + (twoDigits % 10);
			*--cursor = '0' + (twoDigits / 10);
		}
		if (magnitude >= 10) {
			*--cursor = '0' + (magnitude % 10);
			*--cursor = '0' + (magnitude / 10);
		} else {
			*--cursor = '0' + magnitude;
		}
	}

	if (signBit)
		*--cursor = '-';

	emitFieldValue(cursor);
	*used = sizeof(int32) + gap;
	return PARSE_OK;
}

/**
 * serializeInt8 - Decode tinyint (int8) value from buffer
 *
 * @src:   Pointer to source data buffer
 * @avail: Available buffer size
 * @used:  Output parameter for bytes consumed
 *
 * Decodes an 8-bit integer from the buffer and converts it to string.
 *
 * Returns: PARSE_OK on success, negative ParseResultCode on error
 */
static int
serializeInt8(const char *src, unsigned int avail, unsigned int *used)
{
	const char *aligned = (const char *) TINYALIGN(src);
	unsigned int offset = (unsigned int) ((uintptr_t) aligned - (uintptr_t) src);
	int8 val;
	char textBuf[8];

	if (avail < offset)
		return PARSE_ERR_ALIGNMENT;

	avail -= offset;

	if (avail < sizeof(int8))
		return PARSE_ERR_INSUFFICIENT;

	val = *(int8 *) aligned;
	pg_ltoa((int32) val, textBuf);
	emitFieldValue(textBuf);

	*used = sizeof(int8) + offset;
	return PARSE_OK;
}

/**
 * serializeInt16 - Decode smallint (int16) value from buffer
 *
 * @src:   Pointer to source data buffer
 * @avail: Available buffer size
 * @used:  Output parameter for bytes consumed
 *
 * Decodes a 16-bit integer from the buffer and converts it to string.
 * Uses bit operations for alignment calculation.
 *
 * Returns: PARSE_OK on success, negative ParseResultCode on error
 */
static int
serializeInt16(const char *src, unsigned int avail, unsigned int *used)
{
	uintptr_t baseAddr = (uintptr_t) src;
	uintptr_t mask = sizeof(int16) - 1;
	unsigned int skip = (unsigned int)((baseAddr & mask) ? (sizeof(int16) - (baseAddr & mask)) : 0);
	const char *start;
	int16 num;
	char buffer[8];
	char *pos;
	int16 absVal;
	bool negative;

	if (avail < skip)
		return PARSE_ERR_ALIGNMENT;

	start = src + skip;
	avail -= skip;

	if (avail < sizeof(int16))
		return PARSE_ERR_INSUFFICIENT;

	num = *(int16 *) start;

	pos = buffer + sizeof(buffer) - 1;
	*pos = '\0';

	negative = (num < 0);
	absVal = negative ? -num : num;

	do {
		*--pos = '0' + (absVal % 10);
		absVal /= 10;
	} while (absVal > 0);

	if (negative)
		*--pos = '-';

	emitFieldValue(pos);
	*used = sizeof(int16) + skip;
	return PARSE_OK;
}

/**
 * serializeInt64 - Decode bigint (int64) value from buffer
 *
 * @src:   Pointer to source data buffer
 * @avail: Available buffer size
 * @used:  Output parameter for bytes consumed
 *
 * Decodes a 64-bit integer from the buffer and converts it to string.
 * Uses segmented optimization algorithm to process large numbers efficiently.
 *
 * Returns: PARSE_OK on success, negative ParseResultCode on error
 */
static int
serializeInt64(const char *src, unsigned int avail, unsigned int *used)
{
	uintptr_t addr = (uintptr_t) src;
	unsigned int padding = (unsigned int)(((addr + 7) & ~7UL) - addr);
	const char *dataPtr;
	int64 value;
	char output[32];
	char *writePos;
	uint64 absValue;
	bool isNegative;

	if (avail < padding)
		return PARSE_ERR_ALIGNMENT;

	dataPtr = src + padding;
	avail -= padding;

	if (avail < sizeof(int64))
		return PARSE_ERR_INSUFFICIENT;

	value = *(int64 *) dataPtr;

	writePos = output + sizeof(output) - 1;
	*writePos = '\0';

	isNegative = (value < 0);
	absValue = isNegative ? (uint64)(~value) + 1 : (uint64)value;

	if (absValue == 0) {
		*--writePos = '0';
	} else {
		while (absValue >= 10000) {
			uint64 segment = absValue % 10000;
			absValue /= 10000;

			*--writePos = '0' + (segment % 10);
			segment /= 10;
			*--writePos = '0' + (segment % 10);
			segment /= 10;
			*--writePos = '0' + (segment % 10);
			*--writePos = '0' + (segment / 10);
		}

		while (absValue > 0) {
			*--writePos = '0' + (absValue % 10);
			absValue /= 10;
		}
	}

	if (isNegative)
		*--writePos = '-';

	emitFieldValue(writePos);
	*used = sizeof(int64) + padding;
	return PARSE_OK;
}

static int
serializeFloat32(const char *src, unsigned int avail, unsigned int *used)
{
	const char *alignedSrc = (const char *) INTALIGN(src);
	unsigned int offset = (unsigned int) ((uintptr_t) alignedSrc - (uintptr_t) src);
	float floatVal;
	char textRepr[32];

	if (avail < offset)
		return PARSE_ERR_ALIGNMENT;

	avail -= offset;

	if (avail < sizeof(float))
		return PARSE_ERR_INSUFFICIENT;

	floatVal = *(float *) alignedSrc;
	sprintf(textRepr, "%g", (double) floatVal);

	emitFieldValue(textRepr);
	*used = sizeof(float) + offset;
	return PARSE_OK;
}

static int
serializeFloat64(const char *src, unsigned int avail, unsigned int *used)
{
	const char *alignedSrc = (const char *) DOUBLEALIGN(src);
	unsigned int offset = (unsigned int) ((uintptr_t) alignedSrc - (uintptr_t) src);
	double dblVal;
	char textRepr[32];

	if (avail < offset)
		return PARSE_ERR_ALIGNMENT;

	avail -= offset;

	if (avail < sizeof(double))
		return PARSE_ERR_INSUFFICIENT;

	dblVal = *(double *) alignedSrc;
	sprintf(textRepr, "%g", dblVal);

	emitFieldValue(textRepr);
	*used = sizeof(double) + offset;
	return PARSE_OK;
}

/*
 * decode_numeric_value - Decode and output a PostgreSQL numeric data type
 *
 * This function parses a variable-length numeric value from the input buffer
 * and converts it to its string representation for output.
 *
 * Inline expanded version - embeds dissectVarlena and get_str_from_numeric logic directly
 *
 * Parameters:
 *   input_buffer    - Pointer to the raw binary data containing the numeric value
 *   buffer_size     - Total available bytes in the input buffer
 *   bytes_processed - Output parameter: number of bytes consumed from input
 *
 * Returns:
 *   0 on success, -1 on failure (insufficient data or parse error)
 */
int
decode_numeric_value(const char *input_buffer,
                     unsigned int buffer_size,
                     unsigned int *bytes_processed)
{

    typedef enum {
        VTYPE_TOAST_PTR_NUM = 1,
        VTYPE_SHORT_NUM = 2,
        VTYPE_NORMAL_NUM = 3,
        VTYPE_COMPRESSED_NUM = 4,
        VTYPE_INVALID_NUM = 0
    } VarlenaTypeNum;

    const unsigned char *scan_ptr = (const unsigned char *) input_buffer;
    unsigned int skip_bytes = 0;
    unsigned int bytes_left = buffer_size;
    VarlenaTypeNum detected_type = VTYPE_INVALID_NUM;
    int parse_result = -1;

    static const char digit_pairs_num[200] =
        "00010203040506070809"
        "10111213141516171819"
        "20212223242526272829"
        "30313233343536373839"
        "40414243444546474849"
        "50515253545556575859"
        "60616263646566676869"
        "70717273747576777879"
        "80818283848586878889"
        "90919293949596979899";

    if (go) {
        printf("\n[Varlena Parser] Initial scan:\n"
               "  1-byte format: %d (len=%d)\n"
               "  1-byte external: %d\n"
               "  4-byte uncompressed: %d (len=%d)\n"
               "  4-byte compressed: %d (len=%d)\n",
               VARATT_IS_1B(input_buffer), VARSIZE_1B(input_buffer),
               VARATT_IS_1B_E(input_buffer),
               VARATT_IS_4B_U(input_buffer), VARSIZE_4B(input_buffer),
               VARATT_IS_4B_C(input_buffer), VARSIZE_4B(input_buffer));
    }

    while (bytes_left > 0 && *scan_ptr == 0x00) {
        if (VARATT_IS_4B_U((const char *) scan_ptr) &&
            VARSIZE_4B((const char *) scan_ptr) <= bytes_left &&
            VARSIZE_4B((const char *) scan_ptr) != 0)
            break;

        scan_ptr++;
        bytes_left--;
        skip_bytes++;

        if (go) {
            printf("[Varlena Parser] Skipped padding byte, new position:\n"
                   "  1-byte: %d (len=%d), 4-byte-U: %d (len=%d), 4-byte-C: %d (len=%d)\n",
                   VARATT_IS_1B((const char *) scan_ptr), VARSIZE_1B((const char *) scan_ptr),
                   VARATT_IS_4B_U((const char *) scan_ptr), VARSIZE_4B((const char *) scan_ptr),
                   VARATT_IS_4B_C((const char *) scan_ptr), VARSIZE_4B((const char *) scan_ptr));
        }
    }
    input_buffer = (const char *) scan_ptr;

    if (VARATT_IS_1B_E(input_buffer))
        detected_type = VTYPE_TOAST_PTR_NUM;
    else if (VARATT_IS_1B(input_buffer))
        detected_type = VTYPE_SHORT_NUM;
    else if (bytes_left >= 4 && VARATT_IS_4B_U(input_buffer))
        detected_type = VTYPE_NORMAL_NUM;
    else if (bytes_left >= 8 && VARATT_IS_4B_C(input_buffer))
        detected_type = VTYPE_COMPRESSED_NUM;

    switch (detected_type) {
    case VTYPE_TOAST_PTR_NUM:
        {
            uint32 toast_size = VARSIZE_EXTERNAL(input_buffer);

            if (toast_size > bytes_left)
                break;

            parse_result = DeToast(input_buffer, bytes_left, bytes_processed, &get_str_from_numeric);
            *bytes_processed = skip_bytes + toast_size;
        }
        break;

    case VTYPE_SHORT_NUM:
        {
            uint8 short_size = VARSIZE_1B(input_buffer);
            int num_size;
            struct NumericData *num_header;
            NumericDigit *digit_array;
            char output_buf[512];
            char *write_ptr;
            int sign_flag, weight_val, scale_val, digit_count;
            int digit_idx, frac_idx;

            if (short_size > bytes_left)
                break;

            num_size = short_size - 1;

            num_header = (struct NumericData *) calloc(10, num_size);
            if (!num_header) {
                parse_result = -2;
                break;
            }

            memcpy((char *) num_header, input_buffer + 1, num_size);

            if (NUMERIC_IS_SPECIAL(num_header)) {
                if (NUMERIC_IS_NINF(num_header)) {
                    emitFieldValue("-Infinity");
                    parse_result = 0;
                } else if (NUMERIC_IS_PINF(num_header)) {
                    emitFieldValue("Infinity");
                    parse_result = 0;
                } else if (NUMERIC_IS_NAN(num_header)) {
                    emitFieldValue("NaN");
                    parse_result = 0;
                } else {
                    parse_result = -2;
                }
                free(num_header);
                *bytes_processed = skip_bytes + short_size;
                break;
            }

            sign_flag = NUMERIC_SIGN(num_header);
            weight_val = NUMERIC_WEIGHT(num_header);
            scale_val = NUMERIC_DSCALE(num_header);

            if (num_size == NUMERIC_HEADER_SIZE(num_header)) {
                emitFormattedValue("%d", 0);
                free(num_header);
                parse_result = 0;
                *bytes_processed = skip_bytes + short_size;
                break;
            }

            digit_count = num_size / sizeof(NumericDigit);
            digit_array = (NumericDigit *) ((char *) num_header + NUMERIC_HEADER_SIZE(num_header));

            write_ptr = output_buf;

            if (sign_flag == NUMERIC_NEG)
                *write_ptr++ = '-';

            if (weight_val < 0) {
                *write_ptr++ = '0';
                digit_idx = weight_val + 1;
            } else {
                for (digit_idx = 0; digit_idx <= weight_val; digit_idx++) {
                    NumericDigit current_digit = (digit_idx < digit_count) ? digit_array[digit_idx] : 0;

                    if (digit_idx == 0) {
                        if (current_digit >= 1000) {
                            *write_ptr++ = '0' + (current_digit / 1000);
                            current_digit %= 1000;
                            goto print_hundreds_short;
                        } else if (current_digit >= 100) {
                            goto print_hundreds_short;
                        } else if (current_digit >= 10) {
                            goto print_tens_short;
                        } else {
                            *write_ptr++ = '0' + current_digit;
                            continue;
                        }

print_hundreds_short:
                        *write_ptr++ = '0' + (current_digit / 100);
                        current_digit %= 100;
print_tens_short:
                        memcpy(write_ptr, &digit_pairs_num[current_digit * 2], 2);
                        write_ptr += 2;
                    } else {
                        *write_ptr++ = '0' + (current_digit / 1000);
                        current_digit %= 1000;
                        *write_ptr++ = '0' + (current_digit / 100);
                        current_digit %= 100;
                        memcpy(write_ptr, &digit_pairs_num[current_digit * 2], 2);
                        write_ptr += 2;
                    }
                }
            }

            if (scale_val > 0) {
                char *frac_end_ptr;

                *write_ptr++ = '.';
                frac_end_ptr = write_ptr + scale_val;

                for (frac_idx = 0; frac_idx < scale_val; digit_idx++, frac_idx += DEC_DIGITS) {
                    NumericDigit frac_digit = (digit_idx >= 0 && digit_idx < digit_count)
                        ? digit_array[digit_idx] : 0;

                    *write_ptr++ = '0' + (frac_digit / 1000);
                    frac_digit %= 1000;
                    *write_ptr++ = '0' + (frac_digit / 100);
                    frac_digit %= 100;
                    memcpy(write_ptr, &digit_pairs_num[frac_digit * 2], 2);
                    write_ptr += 2;
                }

                write_ptr = frac_end_ptr;
            }

            *write_ptr = '\0';
            emitFieldValue(output_buf);
            free(num_header);
            parse_result = 0;
            *bytes_processed = skip_bytes + short_size;
        }
        break;

    case VTYPE_NORMAL_NUM:
        {
            uint32 normal_size = VARSIZE_4B(input_buffer);
            int num_size;
            struct NumericData *num_header;
            NumericDigit *digit_array;
            char output_buf[512];
            char *write_ptr;
            int sign_flag, weight_val, scale_val, digit_count;
            int digit_idx, frac_idx;

            if (normal_size > bytes_left)
                break;

            num_size = normal_size - 4;

            num_header = (struct NumericData *) calloc(10, num_size);
            if (!num_header) {
                parse_result = -2;
                break;
            }

            memcpy((char *) num_header, input_buffer + 4, num_size);

            if (NUMERIC_IS_SPECIAL(num_header)) {
                if (NUMERIC_IS_NINF(num_header)) {
                    emitFieldValue("-Infinity");
                    parse_result = 0;
                } else if (NUMERIC_IS_PINF(num_header)) {
                    emitFieldValue("Infinity");
                    parse_result = 0;
                } else if (NUMERIC_IS_NAN(num_header)) {
                    emitFieldValue("NaN");
                    parse_result = 0;
                } else {
                    parse_result = -2;
                }
                free(num_header);
                *bytes_processed = skip_bytes + normal_size;
                break;
            }

            sign_flag = NUMERIC_SIGN(num_header);
            weight_val = NUMERIC_WEIGHT(num_header);
            scale_val = NUMERIC_DSCALE(num_header);

            if (num_size == NUMERIC_HEADER_SIZE(num_header)) {
                emitFormattedValue("%d", 0);
                free(num_header);
                parse_result = 0;
                *bytes_processed = skip_bytes + normal_size;
                break;
            }

            digit_count = num_size / sizeof(NumericDigit);
            digit_array = (NumericDigit *) ((char *) num_header + NUMERIC_HEADER_SIZE(num_header));

            write_ptr = output_buf;

            if (sign_flag == NUMERIC_NEG)
                *write_ptr++ = '-';

            if (weight_val < 0) {
                *write_ptr++ = '0';
                digit_idx = weight_val + 1;
            } else {
                for (digit_idx = 0; digit_idx <= weight_val; digit_idx++) {
                    NumericDigit current_digit = (digit_idx < digit_count) ? digit_array[digit_idx] : 0;

                    if (digit_idx == 0) {
                        if (current_digit >= 1000) {
                            *write_ptr++ = '0' + (current_digit / 1000);
                            current_digit %= 1000;
                            goto print_hundreds_normal;
                        } else if (current_digit >= 100) {
                            goto print_hundreds_normal;
                        } else if (current_digit >= 10) {
                            goto print_tens_normal;
                        } else {
                            *write_ptr++ = '0' + current_digit;
                            continue;
                        }

print_hundreds_normal:
                        *write_ptr++ = '0' + (current_digit / 100);
                        current_digit %= 100;
print_tens_normal:
                        memcpy(write_ptr, &digit_pairs_num[current_digit * 2], 2);
                        write_ptr += 2;
                    } else {
                        *write_ptr++ = '0' + (current_digit / 1000);
                        current_digit %= 1000;
                        *write_ptr++ = '0' + (current_digit / 100);
                        current_digit %= 100;
                        memcpy(write_ptr, &digit_pairs_num[current_digit * 2], 2);
                        write_ptr += 2;
                    }
                }
            }

            if (scale_val > 0) {
                char *frac_end_ptr;

                *write_ptr++ = '.';
                frac_end_ptr = write_ptr + scale_val;

                for (frac_idx = 0; frac_idx < scale_val; digit_idx++, frac_idx += DEC_DIGITS) {
                    NumericDigit frac_digit = (digit_idx >= 0 && digit_idx < digit_count)
                        ? digit_array[digit_idx] : 0;

                    *write_ptr++ = '0' + (frac_digit / 1000);
                    frac_digit %= 1000;
                    *write_ptr++ = '0' + (frac_digit / 100);
                    frac_digit %= 100;
                    memcpy(write_ptr, &digit_pairs_num[frac_digit * 2], 2);
                    write_ptr += 2;
                }

                write_ptr = frac_end_ptr;
            }

            *write_ptr = '\0';
            emitFieldValue(output_buf);
            free(num_header);
            parse_result = 0;
            *bytes_processed = skip_bytes + normal_size;
        }
        break;

    case VTYPE_COMPRESSED_NUM:
        {
            uint32 compressed_size = VARSIZE_4B(input_buffer);
            uint32 uncompressed_size;
            int decomp_status;
            ToastCompressionId compression_algo;
            int num_size;
            struct NumericData *num_header;
            NumericDigit *digit_array;
            char output_buf[512];
            char *write_ptr;
            int sign_flag, weight_val, scale_val, digit_count;
            int digit_idx, frac_idx;

            if (compressed_size > bytes_left)
                break;

            uncompressed_size = VARDATA_COMPRESSED_GET_EXTSIZE(input_buffer);

            if (uncompressed_size > sizeof(decompression_storage)) {
                printf("THE VARLENA IS %d BYTES , TOO LONG FOR decompression_storage ARRAY\n",
                       uncompressed_size);
                emitFieldValue("(DATA COMPRESSED)");
                *bytes_processed = skip_bytes + compressed_size;
                parse_result = 0;
                break;
            }

            compression_algo = VARDATA_COMPRESSED_GET_COMPRESS_METHOD(input_buffer);
            if (compression_algo == TOAST_PGLZ_COMPRESSION_ID) {
                decomp_status = pglz_decompress(VARDATA_4B_C(input_buffer),
                                                compressed_size - 2 * sizeof(uint32),
                                                decompression_storage,
                                                uncompressed_size, true);
            } else if (compression_algo == TOAST_LZ4_COMPRESSION_ID) {
                decomp_status = LZ4_decompress_safe(VARDATA_4B_C(input_buffer),
                                                    decompression_storage,
                                                    compressed_size - 2 * sizeof(uint32),
                                                    uncompressed_size);
            } else {
                decomp_status = -1;
            }

            if (decomp_status != (int)uncompressed_size || decomp_status < 0) {
                printf("CORRUPTED DATA,CANCELING DECOMPRESSING\n");
                *bytes_processed = skip_bytes + compressed_size;
                parse_result = 0;
                break;
            }

            num_size = uncompressed_size;

            num_header = (struct NumericData *) calloc(10, num_size);
            if (!num_header) {
                parse_result = -2;
                *bytes_processed = skip_bytes + compressed_size;
                break;
            }

            memcpy((char *) num_header, decompression_storage, num_size);

            if (NUMERIC_IS_SPECIAL(num_header)) {
                if (NUMERIC_IS_NINF(num_header)) {
                    emitFieldValue("-Infinity");
                    parse_result = 0;
                } else if (NUMERIC_IS_PINF(num_header)) {
                    emitFieldValue("Infinity");
                    parse_result = 0;
                } else if (NUMERIC_IS_NAN(num_header)) {
                    emitFieldValue("NaN");
                    parse_result = 0;
                } else {
                    parse_result = -2;
                }
                free(num_header);
                *bytes_processed = skip_bytes + compressed_size;
                break;
            }

            sign_flag = NUMERIC_SIGN(num_header);
            weight_val = NUMERIC_WEIGHT(num_header);
            scale_val = NUMERIC_DSCALE(num_header);

            if (num_size == NUMERIC_HEADER_SIZE(num_header)) {
                emitFormattedValue("%d", 0);
                free(num_header);
                parse_result = 0;
                *bytes_processed = skip_bytes + compressed_size;
                break;
            }

            digit_count = num_size / sizeof(NumericDigit);
            digit_array = (NumericDigit *) ((char *) num_header + NUMERIC_HEADER_SIZE(num_header));

            write_ptr = output_buf;

            if (sign_flag == NUMERIC_NEG)
                *write_ptr++ = '-';

            if (weight_val < 0) {
                *write_ptr++ = '0';
                digit_idx = weight_val + 1;
            } else {
                for (digit_idx = 0; digit_idx <= weight_val; digit_idx++) {
                    NumericDigit current_digit = (digit_idx < digit_count) ? digit_array[digit_idx] : 0;

                    if (digit_idx == 0) {
                        if (current_digit >= 1000) {
                            *write_ptr++ = '0' + (current_digit / 1000);
                            current_digit %= 1000;
                            goto print_hundreds_compressed;
                        } else if (current_digit >= 100) {
                            goto print_hundreds_compressed;
                        } else if (current_digit >= 10) {
                            goto print_tens_compressed;
                        } else {
                            *write_ptr++ = '0' + current_digit;
                            continue;
                        }

print_hundreds_compressed:
                        *write_ptr++ = '0' + (current_digit / 100);
                        current_digit %= 100;
print_tens_compressed:
                        memcpy(write_ptr, &digit_pairs_num[current_digit * 2], 2);
                        write_ptr += 2;
                    } else {
                        *write_ptr++ = '0' + (current_digit / 1000);
                        current_digit %= 1000;
                        *write_ptr++ = '0' + (current_digit / 100);
                        current_digit %= 100;
                        memcpy(write_ptr, &digit_pairs_num[current_digit * 2], 2);
                        write_ptr += 2;
                    }
                }
            }

            if (scale_val > 0) {
                char *frac_end_ptr;

                *write_ptr++ = '.';
                frac_end_ptr = write_ptr + scale_val;

                for (frac_idx = 0; frac_idx < scale_val; digit_idx++, frac_idx += DEC_DIGITS) {
                    NumericDigit frac_digit = (digit_idx >= 0 && digit_idx < digit_count)
                        ? digit_array[digit_idx] : 0;

                    *write_ptr++ = '0' + (frac_digit / 1000);
                    frac_digit %= 1000;
                    *write_ptr++ = '0' + (frac_digit / 100);
                    frac_digit %= 100;
                    memcpy(write_ptr, &digit_pairs_num[frac_digit * 2], 2);
                    write_ptr += 2;
                }

                write_ptr = frac_end_ptr;
            }

            *write_ptr = '\0';
            emitFieldValue(output_buf);
            free(num_header);
            parse_result = 0;
            *bytes_processed = skip_bytes + compressed_size;
        }
        break;

    case VTYPE_INVALID_NUM:
    default:
        parse_result = -1;
        break;
    }

    return parse_result;
}

int numeric_outputds(const char *raw_input, unsigned int buf_len, unsigned int *bytes_read)
{
       return dissectVarlenaText(raw_input, buf_len, bytes_read, &get_str_from_numeric);
}

static int
bool_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes)
{
	if (data_length < sizeof(bool))
		return -1;

	bool b = *(const bool *) input_data;
	char result[2];

	result[0] = (b) ? 't' : 'f';
	result[1] = '\0';

	if (ExportMode_decode == SQLform)
		addQuotesToString(result);

	emitFormattedValue(result);
	*consumed_bytes = sizeof(bool);
	return 0;
}

static int
uuid_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes)
{
	static const char hex_chars[] = "0123456789abcdef";
	char buf[2 * UUID_LEN + 5];
	char *p = buf;
	int i;
	const unsigned char *uuid = (const unsigned char *) input_data;

	if (data_length < UUID_LEN)
		return -1;

	for (i = 0; i < UUID_LEN; i++)
	{
		int			hi;
		int			lo;

		if (i == 4 || i == 6 || i == 8 || i == 10)
			*p++ = '-';

		hi = uuid[i] >> 4;
		lo = uuid[i] & 0x0F;

		*p++ = hex_chars[hi];
		*p++ = hex_chars[lo];
	}
	*p = '\0';

	if (ExportMode_decode == SQLform)
		addQuotesToString(buf);

	*consumed_bytes = UUID_LEN;
	emitFormattedValue(buf);
	return 0;
}

static int
decode_macaddr(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes)
{
	char result[32];

	if (data_length < 6)
		return -1;

	snprintf(result, sizeof(result), "%02x:%02x:%02x:%02x:%02x:%02x",
			 (unsigned char) input_data[0], (unsigned char) input_data[1], (unsigned char) input_data[2],
			 (unsigned char) input_data[3], (unsigned char) input_data[4], (unsigned char) input_data[5]);

	if (ExportMode_decode == SQLform)
		addQuotesToString(result);

	emitFormattedValue(result);
	*consumed_bytes = 6;
	return 0;
}

int parse_text_field(const char *raw_data, unsigned int buf_capacity, unsigned int *bytes_consumed)
{
       int parse_status;
       int (*encoder_callback)(const char *, int);

       encoder_callback = &emitEncodedValue;

       if (raw_data == NULL || bytes_consumed == NULL) {
              return -1;
       }

       parse_status = dissectVarlena(raw_data, buf_capacity, bytes_consumed, encoder_callback);

       return parse_status;
}

/*
 * char_output - converts 'x' to "x"
 *
 * The possible output formats are:
 * 1. 0x00 is represented as an empty string.
 * 2. 0x01..0x7F are represented as a single ASCII byte.
 * 3. 0x80..0xFF are represented as \ooo (backslash and 3 octal digits).
 */
static int
char_output(const char *src_data, unsigned int capacity, unsigned int *consumed_bytes)
{
	char ch;
	char result[5];

	if (capacity < sizeof(char))
		return -2;

	ch = *src_data;

	if (IS_HIGHBIT_SET(ch))
	{
		result[0] = '\\';
		result[1] = (((unsigned char) ch) >> 6) + '0';
		result[2] = ((((unsigned char) ch) >> 3) & 07) + '0';
		result[3] = (((unsigned char) ch) & 07) + '0';
		result[4] = '\0';
	}
	else
	{
		result[0] = ch;
		result[1] = '\0';
	}

	emitFieldValue(result);
	*consumed_bytes = sizeof(char);
	return 0;
}

static int
name_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes)
{
	size_t		len;

	if (data_length < NAMEDATALEN)
		return -1;

	len = strnlen(input_data, NAMEDATALEN);

	char result[NAMEDATALEN + 1];
	memcpy(result, input_data, len);
	result[len] = '\0';

	if (ExportMode_decode == SQLform)
		addQuotesToString(result);

	emitFormattedValue(result);
	*consumed_bytes = NAMEDATALEN;
	return 0;
}

/*
 * Align data, parse varlena header, detoast and decompress.
 * Last parameters responds for actual parsing according to type.
 */
/**
 * dissectVarlena - Parse variable-length data from buffer
 *
 * @input_data:    Pointer to source data buffer
 * @data_length: Available buffer size
 * @consumed_bytes:  Output parameter for bytes consumed
 * @xman:      Callback function for processing extracted data
 *
 * Independent implementation of variable-length data parser.
 * Uses state machine approach to identify different formats.
 *
 * Returns: 0 on success, negative value on error
 */
static int
dissectVarlena(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes, int (*xman)(const char *, int))
{
	typedef enum {
		VTYPE_TOAST_PTR = 1,
		VTYPE_SHORT = 2,
		VTYPE_NORMAL = 3,
		VTYPE_COMPRESSED = 4,
		VTYPE_INVALID = 0
	} VarlenaType;

	const unsigned char *scan_ptr = (const unsigned char *) input_data;
	unsigned int skip_bytes = 0;
	unsigned int bytes_left = data_length;
	VarlenaType detected_type = VTYPE_INVALID;
	int parse_result = -1;

	const char *current_pos;

	if (go) {
		printf("\n[Varlena Parser] Initial scan:\n"
			   "  1-byte format: %d (len=%d)\n"
			   "  1-byte external: %d\n"
			   "  4-byte uncompressed: %d (len=%d)\n"
			   "  4-byte compressed: %d (len=%d)\n",
			   VARATT_IS_1B(input_data), VARSIZE_1B(input_data),
			   VARATT_IS_1B_E(input_data),
			   VARATT_IS_4B_U(input_data), VARSIZE_4B(input_data),
			   VARATT_IS_4B_C(input_data), VARSIZE_4B(input_data));
	}

	while (bytes_left > 0 && *scan_ptr == 0x00) {
		if (VARATT_IS_4B_U((const char *) scan_ptr) &&
			VARSIZE_4B((const char *) scan_ptr) <= bytes_left &&
			VARSIZE_4B((const char *) scan_ptr) != 0)
			break;

		scan_ptr++;
		bytes_left--;
		skip_bytes++;

		if (go) {
			printf("[Varlena Parser] Skipped padding byte, new position:\n"
				   "  1-byte: %d (len=%d), 4-byte-U: %d (len=%d), 4-byte-C: %d (len=%d)\n",
				   VARATT_IS_1B((const char *) scan_ptr), VARSIZE_1B((const char *) scan_ptr),
				   VARATT_IS_4B_U((const char *) scan_ptr), VARSIZE_4B((const char *) scan_ptr),
				   VARATT_IS_4B_C((const char *) scan_ptr), VARSIZE_4B((const char *) scan_ptr));
		}
	}
	current_pos = (const char *) scan_ptr;

	if (VARATT_IS_1B_E(current_pos))
		detected_type = VTYPE_TOAST_PTR;
	else if (VARATT_IS_1B(current_pos))
		detected_type = VTYPE_SHORT;
	else if (bytes_left >= 4 && VARATT_IS_4B_U(current_pos))
		detected_type = VTYPE_NORMAL;
	else if (bytes_left >= 8 && VARATT_IS_4B_C(current_pos))
		detected_type = VTYPE_COMPRESSED;

	switch (detected_type) {
	case VTYPE_TOAST_PTR:
		{
			uint32 toast_size = VARSIZE_EXTERNAL(current_pos);

			if (toast_size > bytes_left)
				break;

			parse_result = DeToast(current_pos, bytes_left, consumed_bytes, xman);
			*consumed_bytes = skip_bytes + toast_size;
		}
		break;

	case VTYPE_SHORT:
		{
			uint8 short_size = VARSIZE_1B(current_pos);

			if (short_size > bytes_left)
				break;

			parse_result = xman(current_pos + 1, short_size - 1);
			*consumed_bytes = skip_bytes + short_size;
		}
		break;

	case VTYPE_NORMAL:
		{
			uint32 normal_size = VARSIZE_4B(current_pos);

			if (normal_size > bytes_left)
				break;

			parse_result = xman(current_pos + 4, normal_size - 4);
			*consumed_bytes = skip_bytes + normal_size;
		}
		break;

	case VTYPE_COMPRESSED:
		{
			uint32 compressed_size = VARSIZE_4B(current_pos);
			uint32 uncompressed_size;
			int decomp_status;
			ToastCompressionId compression_algo;

			if (compressed_size > bytes_left)
				break;

			uncompressed_size = VARDATA_COMPRESSED_GET_EXTSIZE(current_pos);

			if (uncompressed_size > sizeof(decompression_storage)) {
				printf("THE VARLENA IS %d BYTES , TOO LONG FOR decompression_storage ARRAY\n",
					   uncompressed_size);
				emitFieldValue("(DATA COMPRESSED)");
				*consumed_bytes = skip_bytes + compressed_size;
				parse_result = 0;
				break;
			}

			compression_algo = VARDATA_COMPRESSED_GET_COMPRESS_METHOD(current_pos);
			if (compression_algo == TOAST_PGLZ_COMPRESSION_ID) {
				decomp_status = pglz_decompress(VARDATA_4B_C(current_pos),
												compressed_size - 2 * sizeof(uint32),
												decompression_storage,
												uncompressed_size, true);
			} else if (compression_algo == TOAST_LZ4_COMPRESSION_ID) {
				decomp_status = LZ4_decompress_safe(VARDATA_4B_C(current_pos),
													decompression_storage,
													compressed_size - 2 * sizeof(uint32),
													uncompressed_size);
			} else {
				decomp_status = -1;
			}

			if (decomp_status != (int)uncompressed_size || decomp_status < 0) {
				printf("CORRUPTED DATA,CANCELING DECOMPRESSING\n");
				*consumed_bytes = skip_bytes + compressed_size;
				parse_result = 0;
				break;
			}

			parse_result = xman(decompression_storage, uncompressed_size);
			*consumed_bytes = skip_bytes + compressed_size;
		}
		break;

	case VTYPE_INVALID:
	default:
		parse_result = -1;
		break;
	}

	return parse_result;
}

static int
dissectVarlenaText(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes, int (*xman)(const char *, int))
{
	enum
	{
		VARLENA_EXTERNAL,
		VARLENA_1B,
		VARLENA_4B_U,
		VARLENA_4B_C,
		VARLENA_UNKNOWN
	} kind = VARLENA_UNKNOWN;
	const unsigned char *walker = (const unsigned char *) input_data;
	unsigned int padding = 0;
	unsigned int remaining = data_length;
	int zeroCnt = 0;
	int result = -9;
	const char *current_pos;

	if(go){
		printf("\nis1B:%d ,len: %d \nis1B_E:%d\nis4B:%d, len:%d \nis4BCompressed:%d,len:%d\n",
		VARATT_IS_1B(input_data),VARSIZE_1B(input_data),
		VARATT_IS_1B_E(input_data),
		VARATT_IS_4B_U(input_data),VARSIZE_4B(input_data),
		VARATT_IS_4B_C(input_data),VARSIZE_4B(input_data));
	}
	while (remaining > 0 && *walker == 0x00)
	{
		if( VARATT_IS_4B_U((const char *) walker) &&
			VARSIZE_4B((const char *) walker) <= remaining &&
			VARSIZE_4B((const char *) walker) != 0)
		{
			break;
		}
		remaining--;
		walker++;
		padding++;
		zeroCnt++;
		if(go){
			printf("\nis1B:%d ,len: %d \nis4B:%d, len:%d \nis4BCompressed:%d,len:%d\n",
			VARATT_IS_1B((const char *) walker),VARSIZE_1B((const char *) walker),
			VARATT_IS_4B_U((const char *) walker),VARSIZE_4B((const char *) walker),
			VARATT_IS_4B_C((const char *) walker),VARSIZE_4B((const char *) walker));
		}
	}
	current_pos = (const char *) walker;

	if (VARATT_IS_1B_E(current_pos))
		kind = VARLENA_EXTERNAL;
	else if (VARATT_IS_1B(current_pos))
		kind = VARLENA_1B;
	else if (remaining >= 4 && VARATT_IS_4B_U(current_pos))
		kind = VARLENA_4B_U;
	else if (remaining >= 8 && VARATT_IS_4B_C(current_pos))
		kind = VARLENA_4B_C;

	switch (kind)
	{
		case VARLENA_EXTERNAL:
		{
			uint32		total = VARSIZE_EXTERNAL(current_pos);

			if (total > remaining)
			{
				result = -1;
				break;
			}

			result = extractToastedPayloadDs(current_pos, remaining, consumed_bytes, xman);

			*consumed_bytes = padding + total;
			break;
		}

		case VARLENA_1B:
		{
			uint8		total = VARSIZE_1B(current_pos);

			if (total > remaining)
			{
				result = -1;
				break;
			}

			result = xman(current_pos + 1, total - 1);
			*consumed_bytes = padding + total;
			break;
		}

		case VARLENA_4B_U:
		{
			uint32		total = VARSIZE_4B(current_pos);

			if (total > remaining || total < 5)
			{
				result = -1;
				break;
			}

			result = xman(current_pos + 4, total - 4);
			*consumed_bytes = padding + total;
			break;
		}

		case VARLENA_4B_C:
		{
			int						decompress_ret;
			uint32					total = VARSIZE_4B(current_pos);
			uint32					decompressed_len = 0;
#if PG_VERSION_NUM >= 14
			ToastCompressionId		cmid;
#endif

			if (total > remaining)
			{
				result = -1;
				break;
			}

			decompressed_len = VARDATA_COMPRESSED_GET_EXTSIZE(current_pos);

			if (decompressed_len > sizeof(decompression_storage))
			{
				*consumed_bytes = padding + total;
				result = 0;
				break;
			}

			cmid = VARDATA_COMPRESSED_GET_COMPRESS_METHOD(current_pos);
			switch(cmid)
			{
				case TOAST_PGLZ_COMPRESSION_ID:
					decompress_ret = pglz_decompress(VARDATA_4B_C(current_pos), total - 2 * sizeof(uint32),
													decompression_storage, decompressed_len, true);
					break;
				case TOAST_LZ4_COMPRESSION_ID:
					decompress_ret = LZ4_decompress_safe(VARDATA_4B_C(current_pos), decompression_storage,
														 total - 2 * sizeof(uint32), decompressed_len);
					break;
				default:
					decompress_ret = -1;
					break;
			}

			if ((decompress_ret != decompressed_len) || (decompress_ret < 0))
			{
				*consumed_bytes = padding + total;
				result = 0;
				break;
			}

			result = xman(decompression_storage, decompressed_len);
			*consumed_bytes = padding + total;
			break;
		}

		case VARLENA_UNKNOWN:
		default:
			break;
	}

	return result;
}

/*
 * Append given string to current COPY line and encode special symbols
 * like \r, \n, \t and \\.
 */
static void append_escaped_char(char ch, char *buf, int *idx, bool escape_csv)
{
	if (!escape_csv)
	{
		buf[(*idx)++] = ch;
		return;
	}

	if (ch == '\0')
	{
		buf[(*idx)++] = '\\';
		buf[(*idx)++] = '0';
		return;
	}
	if (ch == '\r')
	{
		buf[(*idx)++] = '\\';
		buf[(*idx)++] = 'r';
		return;
	}
	if (ch == '\n')
	{
		buf[(*idx)++] = '\\';
		buf[(*idx)++] = 'n';
		return;
	}
	if (ch == '\t')
	{
		buf[(*idx)++] = '\\';
		buf[(*idx)++] = 'r';
		return;
	}
	if (ch == '\\')
	{
		buf[(*idx)++] = '\\';
		buf[(*idx)++] = '\\';
		return;
	}

	buf[(*idx)++] = ch;
}

static int
emitEncodedValue(const char *str, int orig_len)
{

	int			curr_offset = 0;
	int			len = orig_len;
    char *tmp_buff = malloc(2 * orig_len + 1);

	if (tmp_buff == NULL)
	{
		perror("malloc");
		exit(1);
	}


	while (len > 0)
	{
		append_escaped_char(*str, tmp_buff, &curr_offset, ExportMode_decode == CSVform);
		str++;
		len--;
	}

	tmp_buff[curr_offset] = '\0';

	if(ExportMode_decode == SQLform){
		replace_improper_symbols(tmp_buff);
		addQuotesToString(tmp_buff);
	}

	emitFieldValue(tmp_buff);
	free(tmp_buff);
	return 0;
}

/**
 * get_str_from_numeric - Convert PostgreSQL numeric type to string
 *
 * @buffer:   Pointer to numeric data buffer
 * @num_size: Size of numeric data in bytes
 *
 * Independent implementation of numeric type decoder.
 * Uses iterative approach to build digit string with lookup table optimization.
 *
 * Returns: 0 on success, negative value on error
 */
static int get_str_from_numeric(const char *buffer, int num_size)
{
	static const char digit_pairs[200] =
		"00010203040506070809"
		"10111213141516171819"
		"20212223242526272829"
		"30313233343536373839"
		"40414243444546474849"
		"50515253545556575859"
		"60616263646566676869"
		"70717273747576777879"
		"80818283848586878889"
		"90919293949596979899";

	struct NumericData *num_header;
	NumericDigit *digit_array;
	char output_buf[512];
	char *write_ptr = output_buf;
	int sign_flag, weight_val, scale_val, digit_count;
	int digit_idx, frac_idx;

	num_header = (struct NumericData *) calloc(10, num_size);
	if (!num_header)
		return -2;

	memcpy((char *) num_header, buffer, num_size);

	if (NUMERIC_IS_SPECIAL(num_header)) {
		int ret_code = -2;
		if (NUMERIC_IS_NINF(num_header)) {
			emitFieldValue("-Infinity");
			ret_code = 0;
		} else if (NUMERIC_IS_PINF(num_header)) {
			emitFieldValue("Infinity");
			ret_code = 0;
		} else if (NUMERIC_IS_NAN(num_header)) {
			emitFieldValue("NaN");
			ret_code = 0;
		}
		free(num_header);
		return ret_code;
	}

	sign_flag = NUMERIC_SIGN(num_header);
	weight_val = NUMERIC_WEIGHT(num_header);
	scale_val = NUMERIC_DSCALE(num_header);

	if (num_size == NUMERIC_HEADER_SIZE(num_header)) {
		emitFormattedValue("%d", 0);
		free(num_header);
		return 0;
	}

	digit_count = num_size / sizeof(NumericDigit);
	digit_array = (NumericDigit *) ((char *) num_header + NUMERIC_HEADER_SIZE(num_header));

	if (sign_flag == NUMERIC_NEG)
		*write_ptr++ = '-';

	if (weight_val < 0) {
		*write_ptr++ = '0';
		digit_idx = weight_val + 1;
	} else {
		for (digit_idx = 0; digit_idx <= weight_val; digit_idx++) {
			NumericDigit current_digit = (digit_idx < digit_count) ? digit_array[digit_idx] : 0;

			if (digit_idx == 0) {
				if (current_digit >= 1000) {
					*write_ptr++ = '0' + (current_digit / 1000);
					current_digit %= 1000;
					goto print_hundreds;
				} else if (current_digit >= 100) {
					goto print_hundreds;
				} else if (current_digit >= 10) {
					goto print_tens;
				} else {
					*write_ptr++ = '0' + current_digit;
					continue;
				}

print_hundreds:
				*write_ptr++ = '0' + (current_digit / 100);
				current_digit %= 100;
print_tens:
				memcpy(write_ptr, &digit_pairs[current_digit * 2], 2);
				write_ptr += 2;
			} else {
				*write_ptr++ = '0' + (current_digit / 1000);
				current_digit %= 1000;
				*write_ptr++ = '0' + (current_digit / 100);
				current_digit %= 100;
				memcpy(write_ptr, &digit_pairs[current_digit * 2], 2);
				write_ptr += 2;
			}
		}
	}

	if (scale_val > 0) {
		char *frac_end_ptr;

		*write_ptr++ = '.';
		frac_end_ptr = write_ptr + scale_val;

		for (frac_idx = 0; frac_idx < scale_val; digit_idx++, frac_idx += DEC_DIGITS) {
			NumericDigit frac_digit = (digit_idx >= 0 && digit_idx < digit_count)
				? digit_array[digit_idx] : 0;

			*write_ptr++ = '0' + (frac_digit / 1000);
			frac_digit %= 1000;
			*write_ptr++ = '0' + (frac_digit / 100);
			frac_digit %= 100;
			memcpy(write_ptr, &digit_pairs[frac_digit * 2], 2);
			write_ptr += 2;
		}

		write_ptr = frac_end_ptr;
	}

	*write_ptr = '\0';
	emitFieldValue(output_buf);
	free(num_header);
	return 0;
}

static int DeToast(const char *buffer,unsigned int buff_size,unsigned int* out_size,int (*xman)(const char *, int))
{
	int		result = 0;

	if (VARATT_IS_EXTERNAL_ONDISK(buffer))
	{
		varatt_external toast_ptr;
		char	   *toast_data = NULL;

		int32		toast_ext_size;
		char	toast_relation_path[500];
		char		toast_relation_filename[550];
		FILE	   *toast_rel_fp;
		unsigned int block_options = 0;
		unsigned int control_options = 0;

		VARATT_EXTERNAL_GET_POINTER(toast_ptr, buffer);

		#if PG_VERSION_NUM >= 14
		toast_ext_size = VARATT_EXTERNAL_GET_EXTSIZE(toast_ptr);
		#else
		toast_ext_size = toast_ptr.va_extsize;
		#endif

		#if MAINDEBUG == 1
		sprintf(CURDBPath,"/home/11pg/data/base/16384/");
		#endif

		strcpy(toast_relation_path,CURDBPath);

		if(strcmp(toastId,"TOASTNODE") == 0){
			char a[20];
			sprintf(a,"%d",toast_ptr.va_toastrelid);
			initToastId(a);
		}

		sprintf(toast_relation_filename, "%s/%s", toast_relation_path,toastId);
		toast_rel_fp = fopen(toast_relation_filename, "rb");
		if(toast_rel_fp == NULL){
			if( resTyp_decode == UPDATEtyp){
				memset(toast_relation_path,0,500);
				memset(toast_relation_filename,0,550);
				strcpy(toast_relation_path,CURDBPathforDB);
				sprintf(toast_relation_filename, "%s/%s", toast_relation_path,toastId);
				toast_rel_fp = fopen(toast_relation_filename, "rb");
				if(toast_rel_fp == NULL){
					return -1;
				}
			}
			else{
				return -1;
			}
		}

		unsigned int toast_relation_block_size = determinePageDimension(toast_rel_fp);
		fseek(toast_rel_fp, 0, SEEK_SET);
		toast_data = malloc(toast_ptr.va_rawsize*2);
		unsigned int	toastDataRead = 0;

		result = assembleToastByIndex(
		toast_ptr.va_valueid,
		toast_ext_size,
		toast_data,
		toastHash_decode,
		toast_relation_filename);

		if (result == SUCCESS_RET)
		{
			if (VARATT_EXTERNAL_IS_COMPRESSED(toast_ptr)){
				result = UnpackToastPayload(toast_data, toast_ext_size, xman);
				if(result == -1 && resTyp_decode == UPDATEtyp){
				}
			}
			else{
				result = xman(toast_data, toast_ext_size);
			}
		}
		else if(result == FAILURE_RET)
		{
		}
		free(toast_data);
		fclose(toast_rel_fp);
	}
	else
	{
		return -1;
	}

	return result;
}

int assembleToastByIndex(Oid toastOid,unsigned int toastExternalSize,char *toastData,harray *toastHash,char *toastfilePath)
{
    unsigned int bytesToFormat;
	int blockSize;
	FILE *fp=NULL;

	int result = FAILURE_RET;
    char *block = NULL;

	if(toastHash == NULL)
		return FAILURE_RET;

	parray *chunkInfosInner = parray_new();

	char toastOidVal[20];
	sprintf(toastOidVal,"%d",toastOid);
	unsigned int index = hash(toastHash, toastOidVal, toastHash->allocated);
	Node* node = toastHash->table[index];
	while (node != NULL) {
		chunkInfo *elem1 = (chunkInfo*)node->data;
		if(elem1->toid == toastOid){
			parray_append(chunkInfosInner,elem1);
		}
		node=node->next;
	}
	if(parray_num(chunkInfosInner) == 0){
		return FAILURE_RET;
	}

	int num_groups;
    parray **groups = group_chunks(chunkInfosInner, &num_groups);

	int toastRead;
	for (int g = 0; g < num_groups; g++) {
		parray *chunkInfos = groups[g];
		unsigned int	chunkSize = 0;
		toastRead = 0;

		fp = fopen(toastfilePath,"rb");
		if(!fp){
			parray_free(chunkInfos);
			printf("can not open %s \n",toastfilePath);
			return FAILURE_RET;
		}
		blockSize = determinePageDimension(fp);
		block = (char *)malloc(blockSize);
		if (!block)
		{
			parray_free(chunkInfos);
			free(block);
			printf("\nFAILED TO ALLOCATE SIZE OF <%d> BYTES \n",blockSize);
			return FAILURE_RET;
		}
		for(int x=0;x<parray_num(chunkInfos);x++){
			chunkInfo *elem = parray_get(chunkInfos,x);
			uint64_t blkoff = (uint64_t)elem->blk * (uint64_t)blockSize;
			if (toastRead >= toastExternalSize) {
				break;
			}

			if (fseek(fp, blkoff, SEEK_SET) != 0) {
				fprintf(stderr, "fseek failed for block %lu\n", elem->blk);
				continue;
			}

			fread(block, 1, blockSize, fp);

			char *tuple_data = &block[elem->toff];
			HeapTupleHeader	header = (HeapTupleHeader)tuple_data;
			char	   *data = tuple_data + header->t_hoff + 8;
			unsigned int	size = 0;

			chunkSize = VARSIZE(data) - VARHDRSZ;
			if(chunkSize > toastExternalSize ||
			   toastRead+chunkSize > toastExternalSize){
				continue;
			}
			memcpy(toastData + toastRead, VARDATA(data), chunkSize);
			toastRead +=chunkSize;
		}

		if(toastRead >= toastExternalSize){
			for (int g = 0; g < num_groups; g++) {
				parray *elem = groups[g];
				parray_free(elem);
			}
			free(groups);
			fclose(fp);
			free(block);
			return SUCCESS_RET;
		}

		fclose(fp);
		free(block);
		return FAILURE_RET;
	}

    for (int g = 0; g < num_groups; g++) {
		parray *elem = groups[g];
        parray_free(elem);
    }
    free(groups);
    return FAILURE_RET;
}

static int extractToastedPayloadDs(const char *input, unsigned int input_len, unsigned int *consumed, int (*emit_value)(const char *, int))
{
	varatt_external meta;
	int status = -1;

	(void) input_len;
	(void) consumed; /* unused, preserved signature */

	if (!VARATT_IS_EXTERNAL_ONDISK(input))
		return -1;

	VARATT_EXTERNAL_GET_POINTER(meta, input);

#if PG_VERSION_NUM >= 14
	const int32 external_len = VARATT_EXTERNAL_GET_EXTSIZE(meta);
#else
	const int32 external_len = meta.va_extsize;
#endif

	if (!isToastDecoded)
		return meta.va_valueid;

	const size_t workspace_len = meta.va_rawsize * 2;
	char *payload = (char *) malloc(workspace_len);

	if (payload == NULL)
		return -1;

	memset(payload, 0, workspace_len);

	char toastfilePath[MAXPGPATH] = {0};
	snprintf(toastfilePath, sizeof(toastfilePath), "%s/.toast/dbf", dc->csvPrefix);

	status = assembleToastByIndex(meta.va_valueid,
									external_len,
									payload,
									dc->toastOids,
									toastfilePath);

	if (status == SUCCESS_RET)
	{
		status = VARATT_EXTERNAL_IS_COMPRESSED(meta)
					 ? UnpackToastPayload(payload, external_len, emit_value)
					 : emit_value(payload, external_len);
		if (status > 0)
			status = 0;
	}
	else if (status == FAILURE_RET)
	{
		status = -1;
	}

	free(payload);
	return status;
}

static int UnpackToastPayload(const char *packed, int32 packed_len, int (*consumer)(const char *, int))
{
	int			inflated = -1;
	size_t		raw_target = TOAST_COMPRESS_RAWSIZE(packed);
	char	   *scratch = (char *) malloc(raw_target);

	(void) consumer; /* behavior preserved: parse hook unused */

	if (scratch == NULL)
		return -1;

	switch (TOAST_COMPRESS_RAWMETHOD(packed))
	{
		case TOAST_PGLZ_COMPRESSION_ID:
			inflated = pglz_decompress(TOAST_COMPRESS_RAWDATA(packed),packed_len - TOAST_COMPRESS_HEADER_SIZE,
									   scratch,raw_target,true);
			break;
		case TOAST_LZ4_COMPRESSION_ID:
			inflated = LZ4_decompress_safe(TOAST_COMPRESS_RAWDATA(packed),scratch,
										   packed_len - TOAST_COMPRESS_HEADER_SIZE,raw_target);
			break;
		default:
			inflated = -1;
			break;
	}

	if (inflated >= 0)
		emitEncodedValue(scratch, inflated);

	free(scratch);
	return inflated;
}

static int
consumeAlignedOid(const char *raw, unsigned int remaining, unsigned int *consumed, Oid *out_value)
{
	const char *aligned = (const char *) INTALIGN(raw);
	unsigned int skip = (unsigned int) ((uintptr_t) aligned - (uintptr_t) raw);

	if (remaining < skip)
		return -1;

	memcpy(out_value, aligned, sizeof(Oid));
	*consumed = skip + sizeof(Oid);

	return 0;
}

static int
copyInlineBytes(const char *raw, unsigned int available, unsigned int *consumed, char *dest, unsigned int *payload_len)
{
	if (!VARATT_IS_EXTENDED(raw))
	{
		uint32 total = VARSIZE(raw);

		*payload_len = total - VARHDRSZ;
		*consumed = total;
		memcpy(dest, VARDATA(raw), *payload_len);
	}
	else
	{
		printf("FAIL TO DECODE TOAST VALUE\n");
	}

	(void) available;
	return 0;
}

int32 pglz_decompress(const char *source, int32 slen, char *dest,int32 rawsize, bool check_complete)
{
	const unsigned char *sp;
	const unsigned char *srcend;
	unsigned char *dp;
	unsigned char *destend;

	sp = (const unsigned char *) source;
	srcend = ((const unsigned char *) source) + slen;
	dp = (unsigned char *) dest;
	destend = dp + rawsize;
	while (sp < srcend && dp < destend)
	{
		/*
		 * Read one control byte and process the next 8 items (or as many as
		 * remain in the compressed input).
		 */
		unsigned char ctrl = *sp++;
		int			ctrlc;

		for (ctrlc = 0; ctrlc < 8 && sp < srcend && dp < destend; ctrlc++)
		{
			if (ctrl & 1)
			{
				/*
				 * Set control bit means we must read a match tag. The match
				 * is coded with two bytes. First byte uses lower nibble to
				 * code length - 3. Higher nibble contains upper 4 bits of the
				 * offset. The next following byte contains the lower 8 bits
				 * of the offset. If the length is coded as 18, another
				 * extension tag byte tells how much longer the match really
				 * was (0-255).
				 */
				int32		len;
				int32		off;

				len = (sp[0] & 0x0f) + 3;
				off = ((sp[0] & 0xf0) << 4) | sp[1];
				sp += 2;
				if (len == 18)
					len += *sp++;

				/*
				 * Check for corrupt data: if we fell off the end of the
				 * source, or if we obtained off = 0, we have problems.  (We
				 * must check this, else we risk an infinite loop below in the
				 * face of corrupt data.)
				 */
				if (unlikely(sp > srcend || off == 0))
					return -1;

				/*
				 * Don't emit more data than requested.
				 */
				len = Min(len, destend - dp);

				/*
				 * Now we copy the bytes specified by the tag from OUTPUT to
				 * OUTPUT (copy len bytes from dp - off to dp).  The copied
				 * areas could overlap, so to avoid undefined behavior in
				 * memcpy(), be careful to copy only non-overlapping regions.
				 *
				 * Note that we cannot use memmove() instead, since while its
				 * behavior is well-defined, it's also not what we want.
				 */
				while (off < len)
				{
					/*
					 * We can safely copy "off" bytes since that clearly
					 * results in non-overlapping source and destination.
					 */
					memcpy(dp, dp - off, off);
					len -= off;
					dp += off;

					/*----------
					 * This bit is less obvious: we can double "off" after
					 * each such step.  Consider this raw input:
					 *		112341234123412341234
					 * This will be encoded as 5 literal bytes "11234" and
					 * then a match tag with length 16 and offset 4.  After
					 * memcpy'ing the first 4 bytes, we will have emitted
					 *		112341234
					 * so we can double "off" to 8, then after the next step
					 * we have emitted
					 *		11234123412341234
					 * Then we can double "off" again, after which it is more
					 * than the remaining "len" so we fall out of this loop
					 * and finish with a non-overlapping copy of the
					 * remainder.  In general, a match tag with off < len
					 * implies that the decoded data has a repeat length of
					 * "off".  We can handle 1, 2, 4, etc repetitions of the
					 * repeated string per memcpy until we get to a situation
					 * where the final copy step is non-overlapping.
					 *
					 * (Another way to understand this is that we are keeping
					 * the copy source point dp - off the same throughout.)
					 *----------
					 */
					off += off;
				}
				memcpy(dp, dp - off, len);
				dp += len;
			}
			else
			{
				/*
				 * An unset control bit means LITERAL BYTE. So we just copy
				 * one from INPUT to OUTPUT.
				 */
				*dp++ = *sp++;
			}

			/*
			 * Advance the control bit
			 */
			ctrl >>= 1;
		}
	}

	/*
	 * That's it.
	 */
	return (char *) dp - dest;
}

/**
 * j2date - Convert Julian day number to Gregorian calendar date
 *
 * @jd:    Julian day number
 * @year:  Output parameter for year
 * @month: Output parameter for month
 * @day:   Output parameter for day
 *
 * Directly adopts PostgreSQL official j2date implementation.
 * Source: src/backend/utils/adt/datetime.c
 */
static void j2date(int jd, int *year, int *month, int *day)
{
	unsigned int julian;
	unsigned int quad;
	unsigned int extra;
	int			y;

	julian = jd;
	julian += 32044;
	quad = julian / 146097;
	extra = (julian - quad * 146097) * 4 + 3;
	julian += 60 + quad * 3 + extra / 146097;
	quad = julian / 1461;
	julian -= quad * 1461;
	y = julian * 4 / 1461;
	julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
		+ 123;
	y += quad * 4;
	*year = y - 4800;
	quad = julian * 2141 / 65536;
	*day = julian - 7834 * quad / 256;
	*month = (quad + 10) % MONTHS_PER_YEAR + 1;
}

/**
 * date_output - Decode PostgreSQL date type to string
 *
 * @input_data:    Pointer to source data buffer
 * @data_length: Available buffer size
 * @consumed_bytes:  Output parameter for bytes consumed
 *
 * Decodes date value and converts to ISO format (YYYY-MM-DD).
 * Derived from PostgreSQL date_out.
 *
 * Returns: 0 on success, negative value on error
 */
static int date_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes)
{
	const char *aligned_buf = (const char *) INTALIGN(input_data);
	unsigned int padding = (unsigned int) ((uintptr_t) aligned_buf - (uintptr_t) input_data);
	int32 date_val;
	int32 jd, yr, mon, day;
	char buf[32];
	char *ptr;

	if (data_length < padding)
		return -1;

	data_length -= padding;

	if (data_length < sizeof(int32))
		return -2;

	*consumed_bytes = sizeof(int32) + padding;
	date_val = *(int32 *) aligned_buf;

	if (date_val == PG_INT32_MIN) {
		emitFieldValue("-infinity");
		return 0;
	}
	if (date_val == PG_INT32_MAX) {
		emitFieldValue("infinity");
		return 0;
	}

	jd = date_val + POSTGRES_EPOCH_JDATE;
	j2date(jd, &yr, &mon, &day);

	/* Format date in PostgreSQL style (ISO format: YYYY-MM-DD) */
	ptr = buf;
	ptr = pg_ultostr_zeropad(ptr, (yr > 0) ? yr : -(yr - 1), 4);
	*ptr++ = '-';
	ptr = pg_ultostr_zeropad(ptr, mon, 2);
	*ptr++ = '-';
	ptr = pg_ultostr_zeropad(ptr, day, 2);

	if (yr <= 0) {
		memcpy(ptr, " BC", 3);
		ptr += 3;
	}
	*ptr = '\0';

	switch (ExportMode_decode) {
		case SQLform:
			addQuotesToString(buf);
			emitFieldValue(buf);
			break;
		case CSVform:
			emitFieldValue(buf);
			break;
	}
	return 0;
}

/**
 * timestamp_internal_output - Decode PostgreSQL timestamp/timestamptz type
 *
 * @input_data:     Pointer to source data buffer
 * @data_length:  Available buffer size
 * @consumed_bytes:   Output parameter for bytes consumed
 * @include_tz: Whether to include timezone information
 *
 * Independent implementation using pipeline time decomposition algorithm.
 * Derived from PostgreSQL timestamp_out/timestamptz_out.
 *
 * Returns: 0 on success, negative value on error
 */
static int timestamp_internal_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes, bool include_tz)
{
	#define FORMAT_FIXED_WIDTH(ptr, val, width) do { \
		int _v = (val); \
		int _w = (width); \
		char *_p = (ptr) + _w; \
		while (_w-- > 0) { \
			*--_p = '0' + (_v % 10); \
			_v /= 10; \
		} \
	} while(0)

	uintptr_t addr = (uintptr_t) input_data;
	unsigned int pad_bytes = (unsigned int)(((addr + 7) & ~7UL) - addr);
	const char *data_src;
	int64 timestamp_usec;
	int32 days_since_epoch, julian_day;
	int64 microseconds_in_day, total_seconds;
	int32 year, month, day, hour, minute, second;
	int64 microsecond;
	char output_string[80];
	char *write_cursor;

	if (data_length < pad_bytes)
		return -1;

	data_src = input_data + pad_bytes;
	data_length -= pad_bytes;

	if (data_length < sizeof(int64))
		return -2;

	*consumed_bytes = sizeof(int64) + pad_bytes;
	timestamp_usec = *(int64 *) data_src;

	if (timestamp_usec == DT_NOBEGIN) {
		emitFieldValue("'-infinity'");
		return 0;
	}
	if (timestamp_usec == DT_NOEND) {
		emitFieldValue("'infinity'");
		return 0;
	}

	days_since_epoch = (int32)(timestamp_usec / USECS_PER_DAY);
	microseconds_in_day = timestamp_usec - ((int64)days_since_epoch * USECS_PER_DAY);

	if (microseconds_in_day < INT64CONST(0)) {
		microseconds_in_day += USECS_PER_DAY;
		days_since_epoch -= 1;
	}

	julian_day = days_since_epoch + POSTGRES_EPOCH_JDATE;
	j2date(julian_day, &year, &month, &day);

	microsecond = microseconds_in_day % USECS_PER_SEC;
	total_seconds = microseconds_in_day / USECS_PER_SEC;

	second = (int32)(total_seconds % 60);
	total_seconds /= 60;
	minute = (int32)(total_seconds % 60);
	hour = (int32)(total_seconds / 60);

	write_cursor = output_string;

	int display_year = (year > 0) ? year : -(year - 1);
	FORMAT_FIXED_WIDTH(write_cursor, display_year, 4);
	write_cursor += 4;
	*write_cursor++ = '-';

	FORMAT_FIXED_WIDTH(write_cursor, month, 2);
	write_cursor += 2;
	*write_cursor++ = '-';

	FORMAT_FIXED_WIDTH(write_cursor, day, 2);
	write_cursor += 2;
	*write_cursor++ = ' ';

	FORMAT_FIXED_WIDTH(write_cursor, hour, 2);
	write_cursor += 2;
	*write_cursor++ = ':';

	FORMAT_FIXED_WIDTH(write_cursor, minute, 2);
	write_cursor += 2;
	*write_cursor++ = ':';

	FORMAT_FIXED_WIDTH(write_cursor, second, 2);
	write_cursor += 2;
	*write_cursor++ = '.';

	FORMAT_FIXED_WIDTH(write_cursor, (uint32)microsecond, 6);
	write_cursor += 6;

	if (include_tz) {
		*write_cursor++ = '+';
		*write_cursor++ = '0';
		*write_cursor++ = '0';
	}

	if (year <= 0) {
		*write_cursor++ = ' ';
		*write_cursor++ = 'B';
		*write_cursor++ = 'C';
	}

	*write_cursor = '\0';

	switch (ExportMode_decode) {
		case SQLform:
			addQuotesToString(output_string);
			emitFieldValue(output_string);
			break;
		case CSVform:
			emitFieldValue(output_string);
			break;
	}

	#undef FORMAT_FIXED_WIDTH
	return 0;
}

static int time_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes)
{
	const char *aligned_buf = (const char *) LONGALIGN(input_data);
	unsigned int padding = (unsigned int) ((uintptr_t) aligned_buf - (uintptr_t) input_data);
	int64 time_val;
	int64 secs, usecs;
	int32 hr, min, sec;
	char buf[32];
	char *ptr;

	if (data_length < padding)
		return -1;

	data_length -= padding;

	if (data_length < sizeof(int64))
		return -2;

	*consumed_bytes = sizeof(int64) + padding;
	time_val = *(int64 *) aligned_buf;

	secs = time_val / USECS_PER_SEC;
	usecs = time_val % USECS_PER_SEC;
	hr = (int32)(secs / 3600);
	min = (int32)((secs / 60) % 60);
	sec = (int32)(secs % 60);

	ptr = buf;
	ptr = pg_ultostr_zeropad(ptr, hr, 2);
	*ptr++ = ':';
	ptr = pg_ultostr_zeropad(ptr, min, 2);
	*ptr++ = ':';
	ptr = pg_ultostr_zeropad(ptr, sec, 2);
	*ptr++ = '.';
	ptr = pg_ultostr_zeropad(ptr, (uint32)usecs, 6);
	*ptr = '\0';

	switch (ExportMode_decode) {
		case SQLform:
			addQuotesToString(buf);
			emitFieldValue(buf);
			break;
		case CSVform:
			emitFieldValue(buf);
			break;
	}
	return 0;
}

static int timetz_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes)
{
	const char *aligned_buf = (const char *) LONGALIGN(input_data);
	unsigned int padding = (unsigned int) ((uintptr_t) aligned_buf - (uintptr_t) input_data);
	int64 time_val;
	int32 tz_offset;
	int64 secs, usecs;
	int32 hr, min, sec;
	int32 tz_min, tz_hr, tz_m;
	char buf[48];
	char *ptr;
	size_t data_size = sizeof(int64) + sizeof(int32);

	if (data_length < padding)
		return -1;

	data_length -= padding;

	if (data_length < data_size)
		return -2;

	*consumed_bytes = data_size + padding;
	time_val = *(int64 *) aligned_buf;
	tz_offset = *(int32 *) (aligned_buf + sizeof(int64));

	secs = time_val / USECS_PER_SEC;
	usecs = time_val % USECS_PER_SEC;
	hr = (int32)(secs / 3600);
	min = (int32)((secs / 60) % 60);
	sec = (int32)(secs % 60);

	tz_min = -(tz_offset / 60);
	tz_hr = abs(tz_min / 60);
	tz_m = abs(tz_min % 60);

	ptr = buf;
	ptr = pg_ultostr_zeropad(ptr, hr, 2);
	*ptr++ = ':';
	ptr = pg_ultostr_zeropad(ptr, min, 2);
	*ptr++ = ':';
	ptr = pg_ultostr_zeropad(ptr, sec, 2);
	*ptr++ = '.';
	ptr = pg_ultostr_zeropad(ptr, (uint32)usecs, 6);
	*ptr++ = (tz_min >= 0) ? '+' : '-';
	ptr = pg_ultostr_zeropad(ptr, tz_hr, 2);
	*ptr++ = ':';
	ptr = pg_ultostr_zeropad(ptr, tz_m, 2);
	*ptr = '\0';

	switch (ExportMode_decode) {
		case SQLform:
			addQuotesToString(buf);
			emitFieldValue(buf);
			break;
		case CSVform:
			emitFieldValue(buf);
			break;
	}
	return 0;
}

static int timestamp_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes)
{
	#define FORMAT_FIXED_WIDTH_TS(ptr, val, width) do { \
		int _v = (val); \
		int _w = (width); \
		char *_p = (ptr) + _w; \
		while (_w-- > 0) { \
			*--_p = '0' + (_v % 10); \
			_v /= 10; \
		} \
	} while(0)

	uintptr_t addr = (uintptr_t) input_data;
	unsigned int pad_bytes = (unsigned int)(((addr + 7) & ~7UL) - addr);
	const char *data_src;
	int64 timestamp_usec;
	int32 days_since_epoch, julian_day;
	int64 microseconds_in_day, total_seconds;
	int32 year, month, day, hour, minute, second;
	int64 microsecond;
	char output_string[80];
	char *write_cursor;

	if (data_length < pad_bytes)
		return -1;

	data_src = input_data + pad_bytes;
	data_length -= pad_bytes;

	if (data_length < sizeof(int64))
		return -2;

	*consumed_bytes = sizeof(int64) + pad_bytes;
	timestamp_usec = *(int64 *) data_src;

	if (timestamp_usec == DT_NOBEGIN) {
		emitFieldValue("'-infinity'");
		return 0;
	}
	if (timestamp_usec == DT_NOEND) {
		emitFieldValue("'infinity'");
		return 0;
	}

	days_since_epoch = (int32)(timestamp_usec / USECS_PER_DAY);
	microseconds_in_day = timestamp_usec - ((int64)days_since_epoch * USECS_PER_DAY);

	if (microseconds_in_day < INT64CONST(0)) {
		microseconds_in_day += USECS_PER_DAY;
		days_since_epoch -= 1;
	}

	julian_day = days_since_epoch + POSTGRES_EPOCH_JDATE;
	j2date(julian_day, &year, &month, &day);

	microsecond = microseconds_in_day % USECS_PER_SEC;
	total_seconds = microseconds_in_day / USECS_PER_SEC;

	second = (int32)(total_seconds % 60);
	total_seconds /= 60;
	minute = (int32)(total_seconds % 60);
	hour = (int32)(total_seconds / 60);

	write_cursor = output_string;

	int display_year = (year > 0) ? year : -(year - 1);
	FORMAT_FIXED_WIDTH_TS(write_cursor, display_year, 4);
	write_cursor += 4;
	*write_cursor++ = '-';

	FORMAT_FIXED_WIDTH_TS(write_cursor, month, 2);
	write_cursor += 2;
	*write_cursor++ = '-';

	FORMAT_FIXED_WIDTH_TS(write_cursor, day, 2);
	write_cursor += 2;
	*write_cursor++ = ' ';

	FORMAT_FIXED_WIDTH_TS(write_cursor, hour, 2);
	write_cursor += 2;
	*write_cursor++ = ':';

	FORMAT_FIXED_WIDTH_TS(write_cursor, minute, 2);
	write_cursor += 2;
	*write_cursor++ = ':';

	FORMAT_FIXED_WIDTH_TS(write_cursor, second, 2);
	write_cursor += 2;
	*write_cursor++ = '.';

	FORMAT_FIXED_WIDTH_TS(write_cursor, (uint32)microsecond, 6);
	write_cursor += 6;

	if (year <= 0) {
		*write_cursor++ = ' ';
		*write_cursor++ = 'B';
		*write_cursor++ = 'C';
	}

	*write_cursor = '\0';

	switch (ExportMode_decode) {
		case SQLform:
			addQuotesToString(output_string);
			emitFieldValue(output_string);
			break;
		case CSVform:
			emitFieldValue(output_string);
			break;
	}

	#undef FORMAT_FIXED_WIDTH_TS
	return 0;
}

/*
 * AppendTimestampSeconds - 按照 PostgreSQL 风格追加秒和小数秒部分
 * 改编自 PostgreSQL datetime.c 中的 AppendSeconds 和 AppendTimestampSeconds
 * 自动去除小数秒尾部的零
 */
static char *
AppendTimestampSeconds(char *cp, int sec, int fsec)
{
	cp = pg_ultostr_zeropad(cp, sec, 2);

	if (fsec != 0)
	{
		int32		value = (fsec < 0) ? -fsec : fsec;
		char	   *end = &cp[7];  /* 最多6位小数 + 小数点 */
		bool		gotnonzero = false;
		int			precision = 6;

		*cp++ = '.';

		/* 从低位到高位处理，跳过尾部的零 */
		while (precision--)
		{
			int32		oldval = value;
			int32		remainder;

			value /= 10;
			remainder = oldval - value * 10;

			if (remainder)
				gotnonzero = true;

			if (gotnonzero)
				cp[precision] = '0' + remainder;
			else
				end = &cp[precision];
		}

		return end;
	}
	else
		return cp;
}

/*
 * EncodeTimezone - 按照 PostgreSQL 风格编码时区偏移
 * 改编自 PostgreSQL datetime.c 中的 EncodeTimezone
 * tz 是以秒为单位的时区偏移，正值表示西区
 */
static char *
EncodeTimezone(char *str, int tz)
{
	int		hour, min, sec;

	sec = (tz < 0) ? -tz : tz;
	min = sec / 60;
	sec -= min * 60;
	hour = min / 60;
	min -= hour * 60;

	/* TZ 的符号与显示符号相反 */
	*str++ = (tz <= 0 ? '+' : '-');

	if (sec != 0)
	{
		str = pg_ultostr_zeropad(str, hour, 2);
		*str++ = ':';
		str = pg_ultostr_zeropad(str, min, 2);
		*str++ = ':';
		str = pg_ultostr_zeropad(str, sec, 2);
	}
	else if (min != 0)
	{
		str = pg_ultostr_zeropad(str, hour, 2);
		*str++ = ':';
		str = pg_ultostr_zeropad(str, min, 2);
	}
	else
		str = pg_ultostr_zeropad(str, hour, 2);

	return str;
}

static int timestamptz_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes)
{
	#define FORMAT_FIXED_WIDTH_TSTZ(ptr, val, width) do { \
		int _v = (val); \
		int _w = (width); \
		char *_p = (ptr) + _w; \
		while (_w-- > 0) { \
			*--_p = '0' + (_v % 10); \
			_v /= 10; \
		} \
	} while(0)

	uintptr_t addr = (uintptr_t) input_data;
	unsigned int pad_bytes = (unsigned int)(((addr + 7) & ~7UL) - addr);
	const char *data_src;
	int64 timestamp_usec;
	int32 year, month, day, hour, minute, second;
	int64 microsecond;
	int tz = 0;
	char output_string[80];
	char *write_cursor;

	if (data_length < pad_bytes)
		return -1;

	data_src = input_data + pad_bytes;
	data_length -= pad_bytes;

	if (data_length < sizeof(int64))
		return -2;

	*consumed_bytes = sizeof(int64) + pad_bytes;
	timestamp_usec = *(int64 *) data_src;

	if (timestamp_usec == DT_NOBEGIN) {
		emitFieldValue("'-infinity'");
		return 0;
	}
	if (timestamp_usec == DT_NOEND) {
		emitFieldValue("'infinity'");
		return 0;
	}

	/* 将 PostgreSQL timestamp 转换为 Unix time_t */
	time_t unix_time = (time_t)(timestamp_usec / USECS_PER_SEC +
		((int64)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));

	struct tm tm_local;
	localtime_r(&unix_time, &tm_local);

	/* 获取本地时间的各个分量 */
	year = tm_local.tm_year + 1900;
	month = tm_local.tm_mon + 1;
	day = tm_local.tm_mday;
	hour = tm_local.tm_hour;
	minute = tm_local.tm_min;
	second = tm_local.tm_sec;

	/* 获取微秒部分 */
	microsecond = timestamp_usec % USECS_PER_SEC;
	if (microsecond < 0)
		microsecond += USECS_PER_SEC;

	/* 获取时区偏移（秒），PostgreSQL 的 tz 符号与 tm_gmtoff 相反 */
	tz = -(int)tm_local.tm_gmtoff;

	write_cursor = output_string;

	int display_year = (year > 0) ? year : -(year - 1);
	FORMAT_FIXED_WIDTH_TSTZ(write_cursor, display_year, 4);
	write_cursor += 4;
	*write_cursor++ = '-';

	FORMAT_FIXED_WIDTH_TSTZ(write_cursor, month, 2);
	write_cursor += 2;
	*write_cursor++ = '-';

	FORMAT_FIXED_WIDTH_TSTZ(write_cursor, day, 2);
	write_cursor += 2;
	*write_cursor++ = ' ';

	FORMAT_FIXED_WIDTH_TSTZ(write_cursor, hour, 2);
	write_cursor += 2;
	*write_cursor++ = ':';

	FORMAT_FIXED_WIDTH_TSTZ(write_cursor, minute, 2);
	write_cursor += 2;
	*write_cursor++ = ':';

	/* 使用 AppendTimestampSeconds 处理秒和微秒，自动去除尾部的零 */
	write_cursor = AppendTimestampSeconds(write_cursor, second, (int)microsecond);

	/* 时区输出 */
	write_cursor = EncodeTimezone(write_cursor, tz);

	if (year <= 0) {
		*write_cursor++ = ' ';
		*write_cursor++ = 'B';
		*write_cursor++ = 'C';
	}

	*write_cursor = '\0';

	switch (ExportMode_decode) {
		case SQLform:
			addQuotesToString(output_string);
			emitFieldValue(output_string);
			break;
		case CSVform:
			emitFieldValue(output_string);
			break;
	}

	#undef FORMAT_FIXED_WIDTH_TSTZ
	return 0;
}

static int No_op(const char *unused_data, unsigned int remaining_bytes, unsigned int *skipped)
{
	*skipped = remaining_bytes;
	return 0;
}

int AddList2Prcess(decodeFunc *array2Process,char *type,char *BOOTTYPE)
{
	int pos = 0;
	if (*type == '\0')
		return 1;

	while (typeHandlerRegistry[pos].typeName != NULL)
	{
		if (strcmp(typeHandlerRegistry[pos].typeName, type) == 0)
		{
			if (strcmp(BOOTTYPE, TABLE_BOOTTYPE) == 0 && strcmp(type, "char") == 0) {
				array2Process[addNum] = &parse_text_field;
				addNum++;
				return 1;
			}
			else {
				array2Process[addNum] = typeHandlerRegistry[pos].handler;
				addNum++;
				return 1;
			}
		}
		pos++;
	}

	printf("%sUNSUPPORTED DATATYPE<%s>%s\n", COLOR_ERROR, type, C_RESET);
	return 0;
}

char *dropDecodeExtend(pg_attributeDesc *allDesc,decodeFunc *array2Process,const char *tupleData, unsigned int tupleSize,char *BOOTTYPE,FILE *logSucc,FILE *logErr)
{
	go=0;
	HeapTupleHeader header = (HeapTupleHeader) tupleData;
	const char *data = tupleData + header->t_hoff;
	unsigned int size = tupleSize - header->t_hoff;
	int			curr_attr=0;
	int			curr_attrFake=0;
	bool attrmiss = false;
	clean_out();
	uint32		off=0;			/* offset in tuple data */
	bool		slow=false;			/* can we use/set attcacheoff? */
	int nAttrInTuple = HeapTupleHeaderGetNatts(header);
	int nAttr = Min(nAttrInTuple, addNum);
	if(nAttrInTuple != addNum)
		attrmiss = true;
	for (curr_attrFake = 0; curr_attrFake < nAttrInTuple; curr_attrFake++)
	{
		int	res;
		int padding=0;
		unsigned int AttrSize = 0;

		pg_attributeDesc *oneDesc = &allDesc[curr_attrFake];
		int thisisDrop = 0;

		if(strcmp(oneDesc->attname,"dropped") == 0)
			thisisDrop = 1;

		if ((header->t_infomask & HEAP_HASNULL) && att_isnull(curr_attrFake, header->t_bits))
		{
			if(!thisisDrop){
				if(ExportMode_decode == CSVform)
					emitFieldValue("\\N");
				else if (ExportMode_decode == SQLform)
					emitFieldValue("NULL");
				curr_attr++;
			}

			continue;
		}

		if (size <= 0)
		{
			decodeLogPrint(logErr,g_resultBuf.storage.data,sizeLtZero,curr_attrFake,size);
			return "NoWayOut";
		}

		off = att_align_pointer(off, oneDesc->attalign[0], atoi(oneDesc->attlen),data + off);

		if(!thisisDrop){
			const char *xdata = data + off;
			global_curr_att = curr_attr;
			res = array2Process[curr_attr] (xdata, size, &AttrSize);

			if (res < 0)
			{
				decodeLogPrint(logErr,g_resultBuf.storage.data,resLtZero,curr_attrFake,size);
				return "NoWayOut";
			}

			curr_attr++;
		}

		off = att_addlength_pointer(off, atoi(oneDesc->attlen), data + off);

		if (DEBUG &&strcmp(g_resultBuf.storage.data,debugStr) == 0){
			printf(" ");
		}
	}

	if (size != off)
	{
		decodeLogPrint(logErr,g_resultBuf.storage.data,sizeNotZero,curr_attrFake,size);
		return "NoWayOut";
	}

	if (attrmiss)
	{
		for(int h=0;h<(addNum - nAttrInTuple);h++){
			if(ExportMode_decode == CSVform)
				emitFieldValue("\\N");
			else if (ExportMode_decode == SQLform)
				emitFieldValue("NULL");
		}
	}
	char *xman=return_out();
	return xman;
}

/**
 * NodropDecodeExtend - Decode tuple without dropped columns
 *
 * @allDesc:       Array of attribute descriptors
 * @array2Process: Array of decode functions for each attribute
 * @tupleData:     Raw tuple data buffer
 * @tupleSize:     Size of tuple data in bytes
 * @BOOTTYPE:      Boot type string
 * @logSucc:       File pointer for success logging
 * @logErr:        File pointer for error logging
 *
 * Core tuple parsing engine for scenarios without dropped columns.
 * Uses streaming architecture to scan binary payload field by field.
 *
 * Returns: Decoded tuple string or NULL on failure
 */
char *NodropDecodeExtend(pg_attributeDesc *allDesc,decodeFunc *array2Process,const char *tupleData, unsigned int tupleSize,char *BOOTTYPE,FILE *logSucc,FILE *logErr)
{
	#define PARSE_ABORT_SENTINEL "NoWayOut"
	#define EMIT_CSV_NULL() emitFieldValue("\\N")
	#define EMIT_SQL_NULL() emitFieldValue("NULL")

	go = 0;
	clean_out();

	HeapTupleHeader tupleMetaBlock = (HeapTupleHeader) tupleData;
	uint16 headerByteSpan = tupleMetaBlock->t_hoff;

	const char *payloadCursor = tupleData + headerByteSpan;
	unsigned int remainingPayloadBytes = tupleSize - headerByteSpan;

	int embeddedFieldCount = HeapTupleHeaderGetNatts(tupleMetaBlock);
	int effectiveFieldBound = (embeddedFieldCount < addNum) ? embeddedFieldCount : addNum;
	bool hasTrailingDefaults = (embeddedFieldCount != addNum);

	bits8 *nullityBitVector = tupleMetaBlock->t_bits;
	uint16 infoMaskSnapshot = tupleMetaBlock->t_infomask;
	bool nullCheckEnabled = (infoMaskSnapshot & HEAP_HASNULL) != 0;

	int fieldOrdinal = 0;
	while (fieldOrdinal < effectiveFieldBound)
	{
		unsigned int consumedByteCount = 0;
		int decoderReturnCode;
		global_curr_att = fieldOrdinal;
		if (nullCheckEnabled && att_isnull(fieldOrdinal, nullityBitVector))
		{
			switch (ExportMode_decode) {
				case CSVform: EMIT_CSV_NULL(); break;
				case SQLform: EMIT_SQL_NULL(); break;
			}

			if (go) {
				printf("Field %d is null, %d bytes remaining\n", fieldOrdinal + 1, remainingPayloadBytes);
				printf("Current parsed data: %s\n\n", g_resultBuf.storage.data);
			}

			fieldOrdinal++;
			continue;
		}

		if (remainingPayloadBytes <= 0)
		{
			if (resTyp_decode == DELETEtyp)
				decodeLogPrint(logErr, g_resultBuf.storage.data, sizeLtZero, fieldOrdinal, remainingPayloadBytes);
			return PARSE_ABORT_SENTINEL;
		}

		decoderReturnCode = array2Process[fieldOrdinal](payloadCursor, remainingPayloadBytes, &consumedByteCount);

		if (decoderReturnCode < 0)
		{
			if (resTyp_decode == DELETEtyp)
				decodeLogPrint(logErr, g_resultBuf.storage.data, resLtZero, fieldOrdinal, remainingPayloadBytes);
			return PARSE_ABORT_SENTINEL;
		}

		payloadCursor += consumedByteCount;
		remainingPayloadBytes -= consumedByteCount;

		if (DEBUG && strcmp(g_resultBuf.storage.data, debugStr) == 0)
			printf(" ");

		if (go)
		{
			char diagnosticBuffer[100];
			if (fieldOrdinal == 0)
			{
				sprintf(diagnosticBuffer, "Total %d bytes\nField %d parsed %d bytes, %d bytes remaining\n",
					remainingPayloadBytes + consumedByteCount, fieldOrdinal + 1, consumedByteCount, remainingPayloadBytes);
				printf("%s", diagnosticBuffer);
			}
			else
			{
				sprintf(diagnosticBuffer, "Field %d parsed %d bytes, %d bytes remaining\n",
					fieldOrdinal + 1, consumedByteCount, remainingPayloadBytes);
			}
			printf("Current parsed data: %s\n\n", g_resultBuf.storage.data);
		}

		fieldOrdinal++;
	}

	if (remainingPayloadBytes != 0)
	{
		if (resTyp_decode == DELETEtyp)
			decodeLogPrint(logErr, g_resultBuf.storage.data, sizeNotZero, fieldOrdinal, remainingPayloadBytes);
		return PARSE_ABORT_SENTINEL;
	}

	if (hasTrailingDefaults)
	{
		int deficitCount = addNum - embeddedFieldCount;
		for (int padIdx = 0; padIdx < deficitCount; padIdx++)
		{
			switch (ExportMode_decode) {
				case CSVform: EMIT_CSV_NULL(); break;
				case SQLform: EMIT_SQL_NULL(); break;
			}
		}
	}

	#undef PARSE_ABORT_SENTINEL
	#undef EMIT_CSV_NULL
	#undef EMIT_SQL_NULL

	return return_out();
}

char* xmanDecode(int dropExist,pg_attributeDesc *allDesc,decodeFunc *array2Process,const char *tupleData, unsigned int tupleSize,char *BOOTTYPE,FILE *logSucc,FILE *logErr)
{
	char *xman=NULL;
	if(dropExist == 0){
		xman=NodropDecodeExtend(allDesc,array2Process,tupleData,tupleSize,BOOTTYPE,logSucc,logErr);
	}
	else{
		xman=dropDecodeExtend(allDesc,array2Process,tupleData,tupleSize,BOOTTYPE,logSucc,logErr);
	}

	return xman;
}
int a=0;
char* xmanDecodeDrop(decodeFunc *array2Process,const char *tupleData, unsigned int tupleSize,dropContext *dc,int *isToast)
{
	setDropContext(dc);
	HeapTupleHeader header = (HeapTupleHeader) tupleData;
	const char *data = tupleData + header->t_hoff;
	int size = tupleSize - header->t_hoff;

	if(size > BLCKSZ){
		return "NoWayOut";
	}

	#if PG_VERSION_NUM ==18
	int alignNum=9;
	int nattsNum=19;
	int typeStopNum=2;
	int schStopNum=2;
	#elif PG_VERSION_NUM >=16 && PG_VERSION_NUM < 18
	int alignNum=10;
	int nattsNum=18;
	int typeStopNum=2;
	int schStopNum=2;
	#elif PG_VERSION_NUM >= 14 && PG_VERSION_NUM < 16
	int alignNum=11;
    int nattsNum=18;
	int typeStopNum=2;
	int schStopNum=2;
	#endif

	int			curr_attr=0;
	bool attrmiss = false;
	clean_out();
	parray *toids=parray_new();

	int nAttrInTuple = HeapTupleHeaderGetNatts(header);
	if(nAttrInTuple != dc->nAttr){
		parray_free(toids);
		return "NoWayOut";
	}
	for (curr_attr = 0; curr_attr < nAttrInTuple; curr_attr++)
	{
		int	res;
		int padding=0;
		int AttrSize = 0;

		if ((header->t_infomask & HEAP_HASNULL) && att_isnull(curr_attr, header->t_bits))
		{
			if(ExportMode_decode == CSVform)
				emitFieldValue("\\N");
			else if (ExportMode_decode == SQLform)
				emitFieldValue("NULL");
			continue;
		}

		if (size <= 0)
		{
			parray_free(toids);
			return "NoWayOut";
		}
		if(strcmp(dc->BOOTTYPE,CLASS_BOOTTYPE) == 0 && curr_attr == nattsNum){
			break;
		}
		else if(strcmp(dc->BOOTTYPE,ATTR_BOOTTYPE) == 0 && curr_attr == alignNum){
			if(!isValidString(g_resultBuf.storage.data)){
				parray_free(toids);
				return "NoWayOut";
			}
			break;
		}
		else if(strcmp(dc->BOOTTYPE,TYPE_BOOTTYPE) == 0 && curr_attr == typeStopNum){
			if(!isValidString(g_resultBuf.storage.data)){
				parray_free(toids);
				return "NoWayOut";
			}
			break;
		}
		else if(strcmp(dc->BOOTTYPE,SCHEMA_BOOTTYPE) == 0 && curr_attr == schStopNum){
			if(!isValidString(g_resultBuf.storage.data)){
				parray_free(toids);
				return "NoWayOut";
			}
			break;
		}
		res = array2Process[curr_attr] (data, size, &AttrSize);

		if (DEBUG &&strcmp(g_resultBuf.storage.data,debugStr) == 0){
			printf(" ");
		}

		if (res < 0)
		{
			parray_free(toids);
			return "NoWayOut";
		}
		else if(res > 0 ){
			intptr_t val = res;
			parray_append(toids,(void *)val);
			*isToast = 1;
		}
		size -= AttrSize;
		data += AttrSize;
	}

	if (size != 0 && strcmp(dc->BOOTTYPE,TABLE_BOOTTYPE) == 0)
	{
		parray_free(toids);
		return "NoWayOut";
	}

	if(!is_valid_string(g_resultBuf.storage.data)){
		parray_free(toids);
		return "NoWayOut";
	}

	char *xman=return_out();
	if(size == 0 && *isToast == 1 && strcmp(dc->BOOTTYPE,TABLE_BOOTTYPE) == 0){
		for (int j = 0; j < parray_num(toids); j++)
		{
			int val = (intptr_t)parray_get(toids,j);
			harray_append(dc->toastOids,HARRAYINT,&val,val);
			char *toastOidPath = malloc(100);
			char *valstr = malloc(20);
			sprintf(valstr,"%d\n",val);
			sprintf(toastOidPath,"%s/.toast/.toastoid",dc->csvPrefix);
			FILE *toastOidfp = fopen(toastOidPath,"a");
			fputs(valstr,toastOidfp);
			fclose(toastOidfp);
			free(toastOidPath);
			free(valstr);
		}
	}
	parray_free(toids);
	return xman;
}

char *xmandecodeSys(pg_attributeDesc *allDesc,decodeFunc *array2Process,const char *tupleData, unsigned int tupleSize,char *BOOTTYPE,int addNumLocal)
{
	go=0;
	HeapTupleHeader header = (HeapTupleHeader) tupleData;
	const char *data = tupleData + header->t_hoff;
	unsigned int size = tupleSize - header->t_hoff;
	int			curr_attr=0;
	clean_out();

	int nAttrInTuple = HeapTupleHeaderGetNatts(header);
	int nAttr = Min(nAttrInTuple, addNumLocal);

	for (curr_attr = 0; curr_attr < nAttr; curr_attr++)
	{
		int	res;
		int padding=0;
		unsigned int AttrSize = 0;

		if ((header->t_infomask & HEAP_HASNULL) && att_isnull(curr_attr, header->t_bits))
		{
			if(ExportMode_decode == CSVform)
				emitFieldValue("\\N");
			else if (ExportMode_decode == SQLform)
				emitFieldValue("NULL");

			if(go){
				printf("Field %d is null, %d bytes remaining\n",curr_attr+1,size);
				printf("Current parsed data: %s\n\n",g_resultBuf.storage.data);
			}
			continue;
		}

		if (size <= 0)
		{
			return "NoWayOut";
		}

		global_curr_att = curr_attr;
		res = array2Process[curr_attr] (data, size, &AttrSize);

		if (res < 0)
		{
			return "NoWayOut";
		}

		size -= AttrSize;
		data += AttrSize;
	}

	if (size != 0)
	{
		return "NoWayOut";
	}

	char *xman=return_out();
	return xman;
}

static unsigned int blockVersion = 0;

void resetArray2Process(decodeFunc *array2Process){
    int i;
    for (i = 0; i < addNum; i++) {
        array2Process[i] = NULL;
    }
    addNum = 0;
}

void commaStrWriteIntoFIleAttr(char *str,FILE *file)
{

	#if PG_VERSION_NUM ==18
	int relidNum=1;
	int nameNum=2;
	int typidNum=3;
	int lenNum=4;
	int numNum=5;
	int modNum=6;
	int alignNum=9;
	#elif PG_VERSION_NUM >=16 && PG_VERSION_NUM < 18
	int relidNum=1;
	int nameNum=2;
	int typidNum=3;
	int lenNum=4;
	int numNum=5;
	int modNum=7;
	int alignNum=10;
	#elif PG_VERSION_NUM >= 14 && PG_VERSION_NUM < 16
	int relidNum=1;
	int nameNum=2;
	int typidNum=3;
	int lenNum=5;
	int numNum=6;
	int modNum=9;
	int alignNum=11;
	#endif

    char tmpstr1[MiddleAllocSize]="";

    int colcount=1;
    char *token=strtok(str, "\t");
    while (token != NULL){
        char tmpstr2[100]="";
        strcpy(tmpstr2,token);
        removeSpaces(tmpstr2);
        if (colcount == relidNum){
            sprintf(tmpstr1, "%s%s", tmpstr2, "\t");
        }
        else if (colcount == nameNum){
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\t");
        }
        else if (colcount == typidNum){
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\t");
        }
		else if(colcount == lenNum){
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\t");
		}
        else if (colcount == numNum){
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\t");
        }
        else if (colcount == modNum){
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\t");
        }
        else if (colcount == alignNum){
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\n");
            fputs(tmpstr1, file);
        }
        token = strtok(NULL, "\t");
        colcount++;
    }
}

void commaStrWriteIntoFileCLASS(char *str,FILE *file)
{
	#if PG_VERSION_NUM == 18
    int oidNum=1;
    int nameNum=2;
    int nspNum=3;
    int filenodeNum=8;
    int toastNum=14;
    int kindNum=18;
    int nattsNum=19;
	#elif PG_VERSION_NUM <18
    int oidNum=1;
    int nameNum=2;
    int nspNum=3;
    int filenodeNum=8;
    int toastNum=13;
    int kindNum=17;
    int nattsNum=18;
	#endif

    char tmpstr1[MiddleAllocSize]="";

    char filenode[MiddleAllocSize]="";
    char relkind[MiddleAllocSize]="";

    int colcount=1;
    char *token=strtok(str, "\t");
    while (token != NULL){
        char tmpstr2[100]="";
        strcpy(tmpstr2,token);

        if (colcount == oidNum)
        {
            sprintf(tmpstr1, "%s%s", tmpstr2, "\t");
        }

        else if (colcount == nameNum)
        {
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\t");
        }

        else if (colcount == nspNum)
        {
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\t");
        }
        else if (colcount == filenodeNum)
        {
            strcpy(filenode,token);
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\t");
        }
        else if (colcount == toastNum)
        {
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\t");
        }
        else if (colcount == kindNum)
        {
            sprintf(relkind,"%s",tmpstr2);
        }

        else if (colcount == nattsNum)
        {
            if ( (strcmp(relkind,"r") == 0 || strcmp(relkind,"t") == 0) && strcmp(filenode,"0") != 0)
            {
				strcat(tmpstr1,tmpstr2);
				strcat(tmpstr1,"\n");
                fputs(tmpstr1, file);
            }
        }
        token = strtok(NULL, "\t");
        colcount++;
    }
}

struct varlena *pglz_decompress_datum(const struct varlena *value)
{
#if PG_VERSION_NUM >= 14
	struct varlena *result;
	int32		rawsize;

	result = (struct varlena *) malloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ);

	rawsize = pglz_decompress((char *) value + VARHDRSZ_COMPRESSED,
							  VARSIZE(value) - VARHDRSZ_COMPRESSED,
							  VARDATA(result),
							  VARDATA_COMPRESSED_GET_EXTSIZE(value),true);

	if (rawsize < 0){
		return NULL;
	}

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#else
	struct varlena *result;

	Assert(VARATT_IS_COMPRESSED(value));

	result = (struct varlena *)
		malloc(TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);
	SET_VARSIZE(result, TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);

	if (pglz_decompress(TOAST_COMPRESS_RAWDATA(value),
							  VARSIZE(value) - VARHDRSZ_COMPRESSED,
							  VARDATA(result),
							  TOAST_COMPRESS_RAWSIZE(value)
											 )< 0)
		printf("compressed data is corrupted");

	return result;
#endif
}

struct varlena *lz4_decompress_datum(const struct varlena *value)
{
	#if PG_VERSION_NUM >= 14
	int32		rawsize;
	struct varlena *result;

	result = (struct varlena *) malloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ);

	rawsize = LZ4_decompress_safe((char *) value + VARHDRSZ_COMPRESSED,
								  VARDATA(result),
								  VARSIZE(value) - VARHDRSZ_COMPRESSED,
								  VARDATA_COMPRESSED_GET_EXTSIZE(value));
	if (rawsize < 0)
		printf("compressed lz4 data is corrupt");

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
	#else
	return NULL;
	#endif
}

static struct varlena *toast_decompress_datum(struct varlena *attr)
{
	ToastCompressionId cmid;

	Assert(VARATT_IS_COMPRESSED(attr));

	/*
	 * Fetch the compression method id stored in the compression header and
	 * decompress the data using the appropriate decompression routine.
	 */
	cmid = TOAST_COMPRESS_METHOD(attr);
	switch (cmid)
	{
		case TOAST_PGLZ_COMPRESSION_ID:
			return pglz_decompress_datum(attr);
		case TOAST_LZ4_COMPRESSION_ID:
			return lz4_decompress_datum(attr);
		default:
			return NULL;		/* keep compiler quiet */
	}
}

struct varlena *toast_fetch_datumds(struct varlena *attr,Oid *toid)
{
	struct varlena *result;
	struct varatt_external toast_ptr;
	int32		toast_ext_size;
	int32		residx,
				nextidx;
	int32		numchunks;
	Pointer		chunk;
	bool		isnull;
	char	   *chunkdata;
	int32		chunksize;
	int			num_indexes;
	int			validIndex;

	if (!VARATT_IS_EXTERNAL_ONDISK(attr))
		printf("toast_fetch_datum shouldn't be called for non-ondisk datums\n");

	VARATT_EXTERNAL_GET_POINTER(toast_ptr, attr);
	if(!isToastDecoded){
		*toid = toast_ptr.va_valueid;
		return NULL;
	}
	else{
		int toastRead = 0;
		char *toastData = malloc(toast_ptr.va_rawsize*2);
		memset(toastData,0,toast_ptr.va_rawsize*2);

		#if PG_VERSION_NUM >= 14
		toast_ext_size = VARATT_EXTERNAL_GET_EXTSIZE(toast_ptr);
		#else
		toast_ext_size = toast_ptr.va_extsize;
		#endif

		char toastfilePath[MAXPGPATH]={0};
		sprintf(toastfilePath, "%s/.toast/dbf", dc->csvPrefix);
		harray *toastHash = dc->toastOids;
		int detoastret = assembleToastByIndex(
			toast_ptr.va_valueid,
			toast_ext_size,
			toastData,
			toastHash,
			toastfilePath);

		if(detoastret == FAILURE_RET){
			free(toastData);
			toastData = NULL;
			return NULL;
		}
		result = (struct varlena *) malloc(toast_ext_size + VARHDRSZ);

		if (VARATT_EXTERNAL_IS_COMPRESSED(toast_ptr))
			SET_VARSIZE_COMPRESSED(result, toast_ext_size + VARHDRSZ);
		else
			SET_VARSIZE(result, toast_ext_size + VARHDRSZ);

		memcpy(VARDATA(result),toastData,toast_ext_size);
		free(toastData);
		toastData = NULL;

		return result;
	}

}

struct varlena *detoast_attr(struct varlena *attr,Oid *oid)
{
	if(!is_address_valid(attr)){
		return NULL;
	}
	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		/*
		 * This is an externally stored datum --- fetch it back from there
		 */
		attr = toast_fetch_datumds(attr,oid);
		if(attr == NULL){
			return NULL;
		}
		if (VARATT_IS_COMPRESSED(attr))
		{
			struct varlena *tmp = attr;

			attr = toast_decompress_datum(tmp);
			free(tmp);
		}
	}
	else if (VARATT_IS_EXTERNAL_INDIRECT(attr))
	{
	}
	else if (VARATT_IS_EXTERNAL_EXPANDED(attr))
	{

	}
	else if (VARATT_IS_COMPRESSED(attr))
	{
		attr = toast_decompress_datum(attr);
	}
	else if (VARATT_IS_SHORT(attr))
	{
		Size		data_size = VARSIZE_SHORT(attr) - VARHDRSZ_SHORT;
		if(data_size > 10240000){
			return NULL;
		}
		Size		new_size = data_size + VARHDRSZ;
		struct varlena *new_attr;

		new_attr = (struct varlena *) malloc(new_size);
		SET_VARSIZE(new_attr, new_size);
		memcpy(VARDATA(new_attr), VARDATA_SHORT(attr), data_size);
		attr = new_attr;
	}

	return attr;
}

struct varlena *pg_detoast_datum(struct varlena *datum,Oid *toid)
{
	Size data_size = VARSIZE_ANY(datum);
	if(data_size > BLCKSZ || data_size <= 0){
		return NULL;
	}
	if (VARATT_IS_EXTENDED(datum))
		return detoast_attr(datum,toid);
	else
		return datum;
}

static int decode_bit(const char *bit_data, unsigned int data_capacity, unsigned int *size_read)
{
	*size_read = VARSIZE_ANY(bit_data);

	Oid oid_ref = 0;
	VarBit *bit_val = (VarBit *)PG_DETOAST_DATUM(bit_data, &oid_ref);

	if (bit_val == NULL)
		return -1;

	VARBIT_CORRECTLY_PADDED(bit_val);

	int bit_count = VARBITLEN(bit_val);
	if (bit_count < 0 || bit_count > 1000)
		return -1;

	char *output_str = (char *) malloc(bit_count + 1);
	bits8 *src_ptr = VARBITS(bit_val);
	char *dst_ptr = output_str;

	int bit_idx = 0;
	while (bit_idx <= bit_count - BITS_PER_BYTE) {
		bits8 byte_val = *src_ptr++;
		for (int bit_pos = 0; bit_pos < BITS_PER_BYTE; bit_pos++) {
			*dst_ptr++ = IS_HIGHBIT_SET(byte_val) ? '1' : '0';
			byte_val <<= 1;
		}
		bit_idx += BITS_PER_BYTE;
	}

	if (bit_idx < bit_count) {
		bits8 final_byte = *src_ptr;
		while (bit_idx < bit_count) {
			*dst_ptr++ = IS_HIGHBIT_SET(final_byte) ? '1' : '0';
			final_byte <<= 1;
			bit_idx++;
		}
	}

	*dst_ptr = '\0';
	emitFieldValue(output_str);
	return 0;
}

