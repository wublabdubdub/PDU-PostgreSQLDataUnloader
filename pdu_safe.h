/*
 * pdu_safe.h - Safe memory allocation and string operation wrappers
 *
 * Provides NULL-checked wrappers for malloc, calloc, realloc, strdup, and fopen
 * to prevent NULL pointer dereference crashes throughout the PDU project.
 */
#ifndef PDU_SAFE_H
#define PDU_SAFE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <limits.h>

typedef struct PduScanField
{
    char *data;
    size_t capacity;
} PduScanField;

#define PDU_SCAN_FIELD(value) { (value), sizeof(value) }
#define PDU_SCAN_FIELD_COUNT(values) (sizeof(values) / sizeof((values)[0]))

/*
 * Read whitespace-delimited fields without allowing a token to overflow its
 * destination.  This preserves fscanf("%s") semantics for metadata files,
 * while making the destination capacity explicit at every call site.
 */
static inline int pdu_scan_fields(FILE *file, PduScanField *fields,
                                  size_t field_count)
{
    size_t i;

    if (file == NULL || fields == NULL || field_count > INT_MAX) {
        return -1;
    }

    for (i = 0; i < field_count; i++) {
        int ch;
        size_t length = 0;
        int overflow = 0;

        if (fields[i].data == NULL || fields[i].capacity == 0) {
            return -1;
        }

        do {
            ch = fgetc(file);
        } while (ch != EOF && isspace((unsigned char) ch));

        if (ch == EOF) {
            return (int) i;
        }

        do {
            if (length + 1 < fields[i].capacity) {
                fields[i].data[length++] = (char) ch;
            } else {
                overflow = 1;
            }
            ch = fgetc(file);
        } while (ch != EOF && !isspace((unsigned char) ch));

        fields[i].data[length] = '\0';
        if (overflow) {
            return -1;
        }
    }

    return (int) field_count;
}

static inline int pdu_count_file_lines(FILE *file, int *line_count)
{
    size_t count = 0;
    int ch;
    int last = EOF;

    if (file == NULL || line_count == NULL) {
        return -1;
    }

    while ((ch = fgetc(file)) != EOF) {
        if (ch == '\n') {
            count++;
        }
        last = ch;
    }

    if (ferror(file)) {
        return -1;
    }
    if (last != EOF && last != '\n') {
        count++;
    }
    if (count > INT_MAX) {
        return -1;
    }

    *line_count = (int) count;
    rewind(file);
    return 0;
}

static inline void *pdu_malloc(size_t size)
{
    void *ptr = malloc(size);
    if (ptr == NULL && size > 0) {
        fprintf(stderr, "pdu_malloc: out of memory (requested %zu bytes)\n", size);
    }
    return ptr;
}

static inline void *pdu_calloc(size_t nmemb, size_t size)
{
    void *ptr = calloc(nmemb, size);
    if (ptr == NULL && nmemb > 0 && size > 0) {
        fprintf(stderr, "pdu_calloc: out of memory (requested %zu * %zu bytes)\n", nmemb, size);
    }
    return ptr;
}

static inline void *pdu_realloc(void *old, size_t size)
{
    void *ptr = realloc(old, size);
    if (ptr == NULL && size > 0) {
        fprintf(stderr, "pdu_realloc: out of memory (requested %zu bytes)\n", size);
    }
    return ptr;
}

static inline char *pdu_strdup(const char *s)
{
    size_t len;
    char *ptr;

    if (s == NULL) {
        fprintf(stderr, "pdu_strdup: NULL input\n");
        return NULL;
    }

    len = strlen(s) + 1;
    ptr = malloc(len);
    if (ptr == NULL) {
        fprintf(stderr, "pdu_strdup: out of memory\n");
        return NULL;
    }

    memcpy(ptr, s, len);
    return ptr;
}

static inline FILE *pdu_fopen(const char *path, const char *mode)
{
    if (path == NULL || mode == NULL) {
        fprintf(stderr, "pdu_fopen: NULL argument\n");
        return NULL;
    }
    FILE *fp = fopen(path, mode);
    if (fp == NULL) {
        fprintf(stderr, "pdu_fopen: cannot open '%s' (%s): %s\n", path, mode, strerror(errno));
    }
    return fp;
}

#endif /* PDU_SAFE_H */
