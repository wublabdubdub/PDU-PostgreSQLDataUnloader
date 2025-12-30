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
 *
 * This header file defines data type decoding structures and functions
 * for interpreting PostgreSQL internal data formats (NUMERIC, JSONB, etc.)
 */
#include "pg_xlogreader.h"
#include <stdbool.h>
#include "info.h"

void setResTyp_decode(int setting);

void setExportMode_decode(int setting);

void setlogLevel(int setting);

void showSupportTypeCom();

typedef int (*decodeFunc) (const char *tupleSingleAttr, unsigned int tupleSizeSingleAttr,unsigned int *sizeDecoded);

typedef void (*commaSeperFunc) (char *str,FILE *file);

typedef int16 NumericDigit;
#define DEC_DIGITS     4      
#define Digit uint16
typedef struct NumericVar
{
	int			ndigits;		/* # of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			sign;			/* NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF */
	int			dscale;			/* display scale */
	NumericDigit *buf;			/* start of palloc'd space for digits[] */
	NumericDigit *digits;		/* base-NBASE digits */
} NumericVar;
struct NumericShort
{
       uint16          n_header;               /* Sign + display scale + weight */
       NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

struct NumericLong
{
       uint16          n_sign_dscale;  /* Sign + display scale */
       int16           n_weight;               /* Weight of 1st digit  */
       NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

union NumericChoice
{
       uint16          n_header;               /* Header word */
       struct NumericLong n_long;      /* Long form (4-byte header) */
       struct NumericShort n_short;    /* Short form (2-byte header) */
};

struct NumericData
{
       union NumericChoice choice; /* choice of format */
};
typedef struct NumericData *Numeric;
/*
 * Interpretation of high bits.
 */

struct NumericDataWithvlen
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	union NumericChoice choice; /* choice of format */
};
typedef struct NumericDataWithvlen *NumericTrue;

#define NUMERIC_SIGN_MASK      0xC000
#define NUMERIC_POS                    0x0000
#define NUMERIC_NEG                    0x4000
#define NUMERIC_SHORT          0x8000
#define NUMERIC_SPECIAL                0xC000

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_SHORT(n)            (NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
#define NUMERIC_IS_SPECIAL(n)  (NUMERIC_FLAGBITS(n) == NUMERIC_SPECIAL)

#define NUMERIC_HDRSZ  (VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

/*
 * If the flag bits are NUMERIC_SHORT or NUMERIC_SPECIAL, we want the short
 * header; otherwise, we want the long one.  Instead of testing against each
 * value, we can just look at the high bit, for a slight efficiency gain.
 */

#define NUMERIC_HEADER_IS_SHORT(n)	(((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
       (sizeof(uint16) + \
        (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))
#define NUMERIC_HEADER_SIZE_JSONB(n) \
       (VARHDRSZ + sizeof(uint16) + \
        (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

/*
 * Definitions for special values (NaN, positive infinity, negative infinity).
 *
 * The two bits after the NUMERIC_SPECIAL bits are 00 for NaN, 01 for positive
 * infinity, 11 for negative infinity.  (This makes the sign bit match where
 * it is in a short-format value, though we make no use of that at present.)
 * We could mask off the remaining bits before testing the active bits, but
 * currently those bits must be zeroes, so masking would just add cycles.
 */
#define NUMERIC_EXT_SIGN_MASK  0xF000  /* high bits plus NaN/Inf flag bits */
#define NUMERIC_NAN                            0xC000
#define NUMERIC_PINF                   0xD000
#define NUMERIC_NINF                   0xF000
#define NUMERIC_INF_SIGN_MASK  0x2000

#define NUMERIC_EXT_FLAGBITS(n)        ((n)->choice.n_header & NUMERIC_EXT_SIGN_MASK)
#define NUMERIC_IS_NAN(n)              ((n)->choice.n_header == NUMERIC_NAN)
#define NUMERIC_IS_PINF(n)             ((n)->choice.n_header == NUMERIC_PINF)
#define NUMERIC_IS_NINF(n)             ((n)->choice.n_header == NUMERIC_NINF)
#define NUMERIC_IS_INF(n) \
       (((n)->choice.n_header & ~NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF)

/*
 * Short format definitions.
 */

#define NUMERIC_SHORT_SIGN_MASK                        0x2000
#define NUMERIC_SHORT_DSCALE_MASK              0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT             7
#define NUMERIC_SHORT_DSCALE_MAX               \
       (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define NUMERIC_SHORT_WEIGHT_MASK              0x003F
#define NUMERIC_SHORT_WEIGHT_MAX               NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN               (-(NUMERIC_SHORT_WEIGHT_MASK+1))

/*
 * Extract sign, display scale, weight.  These macros extract field values
 * suitable for the NumericVar format from the Numeric (on-disk) format.
 *
 * Note that we don't trouble to ensure that dscale and weight read as zero
 * for an infinity; however, that doesn't matter since we never convert
 * "special" numerics to NumericVar form.  Only the constants defined below
 * (const_nan, etc) ever represent a non-finite value as a NumericVar.
 */

#define NUMERIC_DSCALE_MASK                    0x3FFF
#define NUMERIC_DSCALE_MAX                     NUMERIC_DSCALE_MASK

#define NUMERIC_SIGN(n) \
       (NUMERIC_IS_SHORT(n) ? \
               (((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? \
                NUMERIC_NEG : NUMERIC_POS) : \
               (NUMERIC_IS_SPECIAL(n) ? \
                NUMERIC_EXT_FLAGBITS(n) : NUMERIC_FLAGBITS(n)))
#define NUMERIC_DSCALE(n)      (NUMERIC_HEADER_IS_SHORT((n)) ? \
       ((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) \
               >> NUMERIC_SHORT_DSCALE_SHIFT \
       : ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(n)      (NUMERIC_HEADER_IS_SHORT((n)) ? \
       (((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
               ~NUMERIC_SHORT_WEIGHT_MASK : 0) \
        | ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) \
       : ((n)->choice.n_long.n_weight))


#define NUMBER_HDRSZ offsetof(NumberData,digits)


#define NUMBER_MASK_SCALE_NAN      (0xff00)
#define NUMBER_MASK_SIGN    (1 << 7)
#define NUMBER_MASK_WEIGHT  ((1 << 7) -1)
#define NUMBER_MASK_SIGN_WEIGHT    (0x00ff)
#define NUMBER_MASK_DSCALE    (0xFF)


#define NUMBER_WEIGHT_INF    63
#define NUMBER_WEIGHT_BIAS  64


#define NUMBER_SIGN_POS     NUMBER_MASK_SIGN
#define NUMBER_SIGN_NEG     0

#define NUMBER_NAN NUMBER_MASK_SCALE_NAN
#define NUMBER_IS_INF(header) ((NUMBER_WEIGHT(header)) == NUMBER_WEIGHT_INF)
#define NUMBER_IS_POS(header) ((NUMBER_SIGN(header)) == NUMBER_SIGN_POS)
#define NUMBER_IS_ZERO(header) ((header & 0xff) == NUMBER_ZERO_HEADER)
#define NUMBER_IS_NAN(header) ((uint16)(header & NUMBER_MASK_SCALE_NAN) == NUMBER_NAN)

#define NUMBER_VAR_IS_NAN(var)((var)->nan == NUMBER_NAN)
#define NUMBER_VAR_IS_INF(var)((var)->weight == NUMER_VAR_WEIGHT_INF)
#define NUMBER_VAR_IS_ZERO(var)((var)->weight == 0 && (var)->ndigits == 0)
#define NUMBER_VAR_IS_POS(var)((var)->sign == NUMBER_SIGN_POS)
#define NUMBER_IS_SPECIAL(Var) (NUMBER_VAR_IS_NAN(Var)||NUMBER_VAR_IS_INF(var))

#define NUMBER_NDIGITS(num) ((VARSIZE(num) - NUMBER_HDRSZ) / sizeof(Digit))
#define NUMBER_SCALE(header) ((uint16)((header & NUMBER_MASK_SCALE_NAN) >> 8))
#define NUMBER_SIGN(header) ((uint16)(header & NUMBER_MASK_SIGN))
#define NUMBER_DSCALE(header) ((uint16)((header >> 8 )& NUMBER_MASK_DSCALE))
#define NUMBER_WEIGHT(header) (NUMBER_IS_POS(header))\
                                   ?((header & NUMBER_MASK_WEIGHT) - NUMBER_WEIGHT_BIAS)\
                                   :(((~header) & NUMBER_MASK_WEIGHT) - NUMBER_WEIGHT_BIAS)






unsigned int
determinePageDimension(FILE *fileHandle);

void initCURDBPath(char *filepath);

void initCURDBPathforDB(char *filepath);

void initToastId(char *toastnode);

int decode_numeric_value(const char *input_buffer, unsigned int buffer_size, unsigned int *bytes_processed);

int
parse_text_field(const char *raw_data, unsigned int buf_capacity, unsigned int *bytes_consumed);

int32 pglz_decompress(const char *source, int32 slen, char *dest,int32 rawsize, bool check_complete);

int AddList2Prcess(decodeFunc *array2Process,char *type,char *BOOTTYPE);

void resetArray2Process(decodeFunc *array2Process);

char* xmanDecode(int dropExist,pg_attributeDesc *allDesc,decodeFunc *array2Process,const char *tupleData, unsigned int tupleSize,char *BOOTTYPE,FILE *logSucc,FILE *logErr);



void commaStrWriteIntoFileCLASS(char *str,FILE *file);

void commaStrWriteIntoFIleAttr(char *str,FILE *file);

#define PG_DETOAST_DATUM(datum,oid) pg_detoast_datum((struct varlena *) DatumGetPointer(datum),(Oid *)oid)


struct varlena *
pglz_decompress_datum(const struct varlena *value);

struct varlena *
lz4_decompress_datum(const struct varlena *value);

struct varlena *
detoast_attr(struct varlena *attr,Oid *toid);

struct varlena *pg_detoast_datum(struct varlena *datum,Oid *toid);


char* xmanDecodeDrop(decodeFunc *array2Process,const char *tupleData, unsigned int tupleSize,dropContext *dc,int *isToast);


void setIsToastDecoded(int setting);

char *xmandecodeSys(pg_attributeDesc *allDesc,decodeFunc *array2Process,const char *tupleData, unsigned int tupleSize,char *BOOTTYPE,int addNumLocal);

Oid getErrToastOidNoths();

int assembleToastByIndex(Oid toastOid,unsigned int toastExternalSize,char *toastData,harray *toastHash,char *toastfilePath);

void setToastHash(harray *setting);


static int UnpackToastPayload(const char *packed, int32 packed_len, int (*consumer)(const char *, int));

static int uuid_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes);

static int name_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes);

static int bool_output(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes);

static int decode_macaddr(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes);

static int decode_bit(const char *bit_data, unsigned int data_capacity, unsigned int *size_read);

static int No_op(const char *unused_data, unsigned int remaining_bytes, unsigned int *skipped);

static int dissectVarlena(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes, int (*xman)(const char *, int));

static int DeToast(const char *buffer,unsigned int buff_size,unsigned int* out_size,int (*xman)(const char *, int));

static int extractToastedPayloadDs(const char *input, unsigned int input_len, unsigned int *consumed, int (*emit_value)(const char *, int));

static int UnpackToastPayload(const char *packed, int32 packed_len, int (*consumer)(const char *, int));

static int dissectVarlenaText(const char *input_data, unsigned int data_length, unsigned int *consumed_bytes, int (*xman)(const char *, int));

void freeOldParray();


void freeNewParray();

void initOldParray();

void initNewParray();
