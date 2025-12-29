/*-------------------------------------------------------------------------
 *
 * parray.h: pointer array collection.
 *
 * Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2024-2025 ZhangChen
 *
 * This code is released under the PostgreSQL License.
 * See LICENSE-PostgreSQL.
 *
 * Modifications by ZhangChen licensed under BSL 1.1;
 * see LICENSE.
 *
 *-------------------------------------------------------------------------
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <assert.h>
#include <stdbool.h>
/*
 * "parray" hold pointers to objects in a linear memory area.
 * Client use "parray *" to access parray object.
 */
typedef struct parray parray;
#define pgut_new(type)			((type *) pgut_malloc(sizeof(type)))
void *pgut_malloc(size_t size);
void *pgut_realloc(void *p, size_t size);
typedef bool (*criterion_fn)(void *value, void *args);
typedef void (*cleanup_fn)(void *ref);

extern parray *parray_new(void);
extern void parray_expand(parray *array, size_t newnum);
extern void parray_free(parray *array);
extern void parray_append(parray *array, void *val);
extern void parray_insert(parray *array, size_t index, void *val);
extern parray *parray_concat(parray *head, const parray *tail);
extern void parray_set(parray *array, size_t index, void *val);
extern void *parray_get(const parray *array, size_t index);
extern void *parray_remove(parray *array, size_t index);
extern bool parray_rm(parray *array, const void *key, int(*compare)(const void *, const void *));
extern size_t parray_num(const parray *array);
extern void parray_qsort(parray *array, int(*compare)(const void *, const void *));
extern void *parray_bsearch(parray *array, const void *key, int(*compare)(const void *, const void *));
extern int parray_bsearch_index(parray *array, const void *key, int(*compare)(const void *, const void *));
extern void parray_walk(parray *array, void (*action)(void *));
extern bool parray_contains(parray *array, void *elem);
extern void parray_remove_if(parray *array, criterion_fn criterion, void *args, cleanup_fn clean);
void parray_duplicate(parray *src, parray *dst);

