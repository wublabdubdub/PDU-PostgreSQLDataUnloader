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
#include <string.h>
#include <assert.h>
#include "basic.h"
#include "stringinfo.h"

#define MaxAllocSize	((Size) 0x3fffffff)

/**
 * initStringInfo - Initialize a StringInfoData struct
 *
 * @str: Pointer to the StringInfo structure to initialize
 *
 * Initializes a StringInfoData struct (with previously undefined contents)
 * to describe an empty string. Allocates an initial buffer of 1024 bytes.
 */
void
initStringInfo(StringInfo str)
{
	int			size = 1024;

	str->data = (char *) malloc(size);
	str->maxlen = size;
	resetStringInfo(str);
}

/**
 * resetStringInfo - Reset the StringInfo to empty state
 *
 * @str: Pointer to the StringInfo structure to reset
 *
 * Resets the StringInfo: the data buffer remains valid, but its
 * previous content, if any, is cleared. The buffer is zeroed out.
 */
void
resetStringInfo(StringInfo str)
{
	str->data[0] = '\0';
	memset(str->data,0,str->maxlen);
	str->len = 0;
	str->cursor = 0;
}

/**
 * appendStringInfoString - Append a null-terminated string
 *
 * @str: Pointer to the StringInfo structure
 * @s:   Null-terminated string to append
 *
 * Appends a null-terminated string to the StringInfo buffer.
 */
void
appendStringInfoString(StringInfo str, const char *s)
{
	appendBinaryStringInfo(str, s, strlen(s));
}

/**
 * appendBinaryStringInfo - Append arbitrary binary data
 *
 * @str:     Pointer to the StringInfo structure
 * @data:    Binary data to append
 * @datalen: Length of data in bytes
 *
 * Appends arbitrary binary data to a StringInfo, allocating more space
 * if necessary. A trailing null byte is always maintained.
 */
void
appendBinaryStringInfo(StringInfo str, const char *data, int datalen)
{
	assert(str != NULL);

	enlargeStringInfo(str, datalen);

	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;

	str->data[str->len] = '\0';
}

/**
 * enlargeStringInfo - Ensure buffer has enough space for additional bytes
 *
 * @str:    Pointer to the StringInfo structure
 * @needed: Number of additional bytes needed (excluding null terminator)
 *
 * Makes sure there is enough space for 'needed' more bytes. External callers
 * usually need not concern themselves with this, since all stringinfo.c
 * routines do it automatically. However, if a caller knows that a StringInfo
 * will eventually become X bytes large, it can save some realloc overhead by
 * enlarging the buffer before starting to store data in it.
 */
void
enlargeStringInfo(StringInfo str, int needed)
{
	Size		newlen;
	Size		limit;
	char	   *old_data;

	limit = MaxAllocSize;

	if (needed < 0)
	{
		printf("INVALID ENLARGEMENT REQUEST SIZE: %d", needed);
		exit(1);
	}

	if (((Size) needed) >= (limit - (Size) str->len))
	{
		printf("Error: current string %d bytes, requested %d bytes.",
			   str->len, needed);
		exit(1);
	}

	needed += str->len + 1;

	if (needed <= str->maxlen)
		return;

	newlen = 2 * str->maxlen;
	while (needed > newlen)
		newlen = 2 * newlen;

	if (newlen > limit)
		newlen = limit;

	old_data = str->data;
	str->data = (char *) realloc(str->data, (Size) newlen);
	if (str->data == NULL)
	{
		free(old_data);
		printf("Error: realloc() FAIL!\n");
		exit(1);
	}

	str->maxlen = newlen;
}

/**
 * appendStringInfoChar - Append a single character
 *
 * @str: Pointer to the StringInfo structure
 * @ch:  Character to append
 *
 * Appends a single character to the StringInfo buffer.
 */
void
appendStringInfoChar(StringInfo str, char ch)
{
	if (str->len + 1 >= str->maxlen)
		enlargeStringInfo(str, 1);

	str->data[str->len] = ch;
	str->len++;
	str->data[str->len] = '\0';
}

/**
 * makeStringInfo - Create a new StringInfo structure
 *
 * Allocates and initializes a new StringInfo structure.
 *
 * Returns: Pointer to the newly created StringInfo
 */
StringInfo
makeStringInfo(void)
{
	StringInfo	res;

	res = (StringInfo) malloc(sizeof(StringInfoData));

	initStringInfo(res);

	return res;
}
