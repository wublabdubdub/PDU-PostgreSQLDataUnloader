/*-------------------------------------------------------------------------
 *
 * parray.c: pointer array collection.
 *
 * Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2024-2025 ZhangChen
 *
 * This code is released under the PostgreSQL License.
 * See LICENSE-PostgreSQL.
 *
 * Modifications by ZhangChen licensed under the Apache License, Version 2.0;
 * see LICENSE.
 *
 *-------------------------------------------------------------------------
 */

#include "parray.h"

/*
 * struct parray - Internal structure for dynamic pointer array
 *
 * Members of struct parray are hidden from client code.
 * This structure manages a dynamically resizable array of void pointers.
 */
struct parray
{
	void **data;
	size_t alloced;
	size_t used;
};

/**
 * pgut_malloc - Allocate memory with error reporting
 *
 * @size: Number of bytes to allocate
 *
 * Allocates the requested amount of memory. If allocation fails,
 * prints an error message to stdout.
 *
 * Returns: Pointer to allocated memory, or NULL on failure
 */
void *
pgut_malloc(size_t size)
{
	char *ret;
	if ((ret = malloc(size)) == NULL)
		printf("could not allocate memory (%lu bytes)",
			(unsigned long) size);
	return ret;
}

/**
 * pgut_realloc - Reallocate memory with error reporting
 *
 * @p:    Pointer to existing memory block (or NULL)
 * @size: New size in bytes
 *
 * Reallocates the memory block to the specified size. If reallocation
 * fails, prints an error message to stdout.
 *
 * Returns: Pointer to reallocated memory, or NULL on failure
 */
void *
pgut_realloc(void *p, size_t size)
{
	char *ret;

	if ((ret = realloc(p, size)) == NULL)
		printf( "could not re-allocate memory (%lu bytes)",
			(unsigned long) size);
	return ret;
}

/**
 * parray_new - Create a new dynamic pointer array
 *
 * Allocates and initializes a new parray object with an initial
 * capacity of 1024 elements.
 *
 * Returns: Pointer to the newly created parray (never returns NULL)
 */
parray *
parray_new(void)
{
	parray *a = pgut_new(parray);

	a->data = NULL;
	a->used = 0;
	a->alloced = 0;

	parray_expand(a, 1024);

	return a;
}

/**
 * parray_expand - Expand array capacity to specified size
 *
 * @array:   Pointer to the parray to expand
 * @newsize: New capacity (number of elements)
 *
 * Expands the internal storage of the parray to hold at least newsize
 * elements. Newly allocated slots are initialized to NULL. Does nothing
 * if current capacity is already sufficient.
 */
void
parray_expand(parray *array, size_t newsize)
{
	void **p;

	if (newsize <= array->alloced)
		return;

	p = pgut_realloc(array->data, sizeof(void *) * newsize);

	memset(p + array->alloced, 0, (newsize - array->alloced) * sizeof(void *));

	array->alloced = newsize;
	array->data = p;
}

/**
 * parray_free - Free a parray and its internal storage
 *
 * @array: Pointer to the parray to free (may be NULL)
 *
 * Releases all memory associated with the parray. Safe to call with NULL.
 * Note: Does not free the elements stored in the array.
 */
void
parray_free(parray *array)
{
	if (array == NULL)
		return;
	free(array->data);
	free(array);
}

/**
 * parray_append - Append an element to the end of the array
 *
 * @array: Pointer to the parray
 * @elem:  Element to append
 *
 * Adds the element to the end of the array, automatically expanding
 * the array capacity if necessary.
 */
void
parray_append(parray *array, void *elem)
{
	if (array->used + 1 > array->alloced)
		parray_expand(array, array->alloced * 2);

	array->data[array->used++] = elem;
}

/**
 * parray_insert - Insert an element at a specific index
 *
 * @array: Pointer to the parray
 * @index: Position at which to insert the element
 * @elem:  Element to insert
 *
 * Inserts the element at the specified index, shifting existing elements
 * to make room. Automatically expands the array if necessary.
 */
void
parray_insert(parray *array, size_t index, void *elem)
{
	if (array->used + 1 > array->alloced)
		parray_expand(array, array->alloced * 2);

	memmove(array->data + index + 1, array->data + index,
		(array->alloced - index - 1) * sizeof(void *));
	array->data[index] = elem;

	if (array->used < index + 1)
		array->used = index + 1;
	else
		array->used++;
}

/**
 * parray_concat - Concatenate two parrays
 *
 * @dest: Destination parray
 * @src:  Source parray to append
 *
 * Appends a copy of all elements from src to the end of dest.
 * The source array is not modified.
 *
 * Returns: Pointer to the destination parray
 */
parray *
parray_concat(parray *dest, const parray *src)
{
	parray_expand(dest, dest->used + src->used);

	memcpy(dest->data + dest->used, src->data, src->used * sizeof(void *));
	dest->used += parray_num(src);

	return dest;
}

/**
 * parray_set - Set an element at a specific index
 *
 * @array: Pointer to the parray
 * @index: Index at which to set the element
 * @elem:  Element to store
 *
 * Sets the element at the specified index. Automatically expands
 * the array if the index exceeds current capacity.
 */
void
parray_set(parray *array, size_t index, void *elem)
{
	if (index > array->alloced - 1)
		parray_expand(array, index + 1);

	array->data[index] = elem;

	if (array->used < index + 1)
		array->used = index + 1;
}

/**
 * parray_get - Retrieve an element at a specific index
 *
 * @array: Pointer to the parray
 * @index: Index of the element to retrieve
 *
 * Returns: The element at the specified index, or NULL if index is out of bounds
 */
void *
parray_get(const parray *array, size_t index)
{
	if (index > array->alloced - 1)
		return NULL;
	return array->data[index];
}

/**
 * parray_remove - Remove and return an element at a specific index
 *
 * @array: Pointer to the parray
 * @index: Index of the element to remove
 *
 * Removes the element at the specified index and shifts remaining
 * elements to fill the gap.
 *
 * Returns: The removed element, or NULL if index is out of bounds
 */
void *
parray_remove(parray *array, size_t index)
{
	void *val;

	if (index > array->used)
		return NULL;

	val = array->data[index];

	if (index < array->alloced - 1)
		memmove(array->data + index, array->data + index + 1,
			(array->alloced - index - 1) * sizeof(void *));

	array->used--;

	return val;
}

/**
 * parray_rm - Remove the first element matching a key
 *
 * @array:   Pointer to the parray
 * @key:     Key to search for
 * @compare: Comparison function (returns 0 on match)
 *
 * Searches for an element matching the key using the provided comparison
 * function and removes the first match found.
 *
 * Returns: true if an element was removed, false otherwise
 */
bool
parray_rm(parray *array, const void *key, int(*compare)(const void *, const void *))
{
	int i;

	for (i = 0; i < array->used; i++)
	{
		if (compare(&key, &array->data[i]) == 0)
		{
			parray_remove(array, i);
			return true;
		}
	}
	return false;
}

/**
 * parray_num - Get the number of elements in the array
 *
 * @array: Pointer to the parray (may be NULL)
 *
 * Returns: Number of elements in the array, or 0 if array is NULL
 */
size_t
parray_num(const parray *array)
{
	return array!= NULL ? array->used : (size_t) 0;
}

/**
 * parray_qsort - Sort the array using quicksort
 *
 * @array:   Pointer to the parray
 * @compare: Comparison function for qsort
 *
 * Sorts the elements in the array using the standard qsort algorithm.
 */
void
parray_qsort(parray *array, int(*compare)(const void *, const void *))
{
	qsort(array->data, array->used, sizeof(void *), compare);
}

/**
 * parray_walk - Apply a function to each element in the array
 *
 * @array:  Pointer to the parray
 * @action: Function to call for each element
 *
 * Iterates through all elements and calls the action function on each.
 */
void
parray_walk(parray *array, void (*action)(void *))
{
	int i;
	for (i = 0; i < array->used; i++)
		action(array->data[i]);
}

/**
 * parray_bsearch - Binary search for an element in a sorted array
 *
 * @array:   Pointer to the parray (must be sorted)
 * @key:     Key to search for
 * @compare: Comparison function for bsearch
 *
 * Performs binary search on a sorted array.
 *
 * Returns: Pointer to the matching element, or NULL if not found
 */
void *
parray_bsearch(parray *array, const void *key, int(*compare)(const void *, const void *))
{
	return bsearch(&key, array->data, array->used, sizeof(void *), compare);
}

/**
 * parray_bsearch_index - Binary search returning element index
 *
 * @array:   Pointer to the parray (must be sorted)
 * @key:     Key to search for
 * @compare: Comparison function for bsearch
 *
 * Performs binary search and returns the index of the found element.
 *
 * Returns: Index of the matching element, or -1 if not found
 */
int
parray_bsearch_index(parray *array, const void *key, int(*compare)(const void *, const void *))
{
	void **elem = parray_bsearch(array, key, compare);
	return elem != NULL ? elem - array->data : -1;
}

/**
 * parray_contains - Check if array contains a specific element
 *
 * @array: Pointer to the parray
 * @elem:  Element to search for (pointer comparison)
 *
 * Searches the array for the exact pointer value.
 *
 * Returns: true if element is found, false otherwise
 */
bool parray_contains(parray *array, void *elem)
{
	int i;

	for (i = 0; i < parray_num(array); i++)
	{
		if (parray_get(array, i) == elem)
			return true;
	}
	return false;
}

/**
 * parray_remove_if - Remove elements matching a criterion
 *
 * @array:     Pointer to the parray
 * @criterion: Function that returns true for elements to remove
 * @args:      Additional arguments passed to criterion function
 * @clean:     Cleanup function called for each removed element
 *
 * Removes all elements for which the criterion function returns true.
 * The cleanup function is called on each removed element before removal.
 * Uses an efficient in-place removal algorithm.
 */
void
parray_remove_if(parray *array, criterion_fn criterion, void *args, cleanup_fn clean) {
	int i = 0;
	int j = 0;

	while(j < parray_num(array)) {
		void *value = array->data[j];
		if(criterion(value, args)) {
			clean(value);
			j++;
			continue;
		}

		if(i != j)
			array->data[i] = array->data[j];

		i++;
		j++;
	}

	array->used -= j - i;
}

/**
 * parray_duplicate - Copy all elements from source to destination array
 *
 * @src: Source parray to copy from
 * @dst: Destination parray to copy to
 *
 * Appends all elements from the source array to the destination array.
 * Note: Only copies the pointers, not the underlying data.
 */
void
parray_duplicate(parray *src, parray *dst) {
	int i=0;

	for ( i = 0; i < parray_num(src); i++)
	{
		void *elem = parray_get(src,i);
		parray_append(dst,elem);
	}
}
