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
