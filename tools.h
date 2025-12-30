/*
 * PDU - PostgreSQL Data Unloader
 * Copyright (c) 2024-2025 ZhangChen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TOOLS_H
#define TOOLS_H

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <stddef.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include "parray.h"
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <fcntl.h>
#include <linux/fiemap.h>
#include <inttypes.h> 
#include <regex.h>


#define MAXDATELEN		128
#define C_RESET       "\033[0m"
/* Color scheme */
#define C_SUBTITLE    "\033[38;5;68m"
#define C_FILEPATH    "\033[38;5;39m"
#define C_EDITION     "\033[38;5;63;1m"
#define C_TIMESTAMP   "\033[38;5;29m"

#define C_GREY1        "\033[38;5;247m"
#define C_GREY2        "\033[38;5;250m"
#define C_GREY3        "\033[38;5;242m"

/* Green colors */
#define C_MGREEN1      "\033[38;5;108m"
#define C_MGREEN2        "\033[38;5;72m"
#define C_MGREEN3        "\033[38;5;66m"

/* Blue colors */
#define C_BLUE1        "\033[38;5;153m"
#define C_BLUE2        "\033[38;5;110m"
#define C_BLUE3       "\033[38;5;103m"
#define C_BLUE4       "\033[38;5;60m"

/* Yellow colors */
#define C_YELLOW1     "\033[38;5;229m"
#define C_YELLOW2     "\033[38;5;187m"
#define C_YELLOW3     "\033[38;5;101m"
#define C_BROWN1      "\033[38;5;137m"
#define C_BROWN3      "\033[38;5;101m"
/* Purple colors */
#define C_PURPLE1     "\033[38;5;133m"
#define C_PURPLE2     "\033[38;5;176m"
#define C_PURPLE3     "\033[38;5;97m"

/* White colors */
#define C_WHITE2      "\033[38;5;254m"
#define C_WHITE3      "\033[38;5;253m"
#define C_WHITE4      "\033[38;5;252m"
#define C_WHITE5      "\033[38;5;251m"
/* Red colors */
#define C_RED3     "\033[38;5;131m"
#define C_RED2     "\033[38;5;203;1m"
#define C_RED1     "\033[38;5;196;1m"
#define C_WINERED1        "\033[38;5;168m"
#define C_WINERED2        "\033[38;5;174m"
#define C_WINERED3        "\033[38;5;95m"
#define C_WINERED4        "\033[38;5;131m"


#define COLOR_COPYRIGHT C_MGREEN1
#define COLOR_VERSION C_MGREEN2
#define COLOR_EDITION C_MGREEN3
#define COLOR_CONTACT C_PURPLE1

#define COLOR_CMD C_BLUE3

#define COLOR_TABLE C_BLUE2
#define COLOR_SCHEMA C_BLUE2
#define COLOR_DB C_BLUE2

#define COLOR_SCAN
#define COLOR_RESTORE
#define COLOR_UNLOAD C_WHITE2

#define COLOR_PARAM C_RED3

#define COLOR_ERROR C_RED1
#define COLOR_WARNING C_RED2
#define COLOR_SUCC     "\033[38;5;35m"

#define COLOR_helpBasic C_BLUE1
#define COLOR_helpSwitch C_BLUE2
#define COLOR_helpShow C_BLUE3
#define COLOR_helpUnload C_PURPLE1
#define COLOR_helpRestore C_PURPLE2
#define COLOR_helpParam C_PURPLE3


#define MiddleAllocSize	10240
#define MAX_FILES 10240
#define MAX_FILENAME_LENGTH 256
#define MAXPGPATH 1024
#define FILENODE "filenode.txt"
#define DB_BOOT "pg_database.txt"
#define SCHEMA_BOOT "pg_schema.txt"
#define CLASS_BOOT "pg_class.txt"
#define CLASS_BOOT_FINAL "pg_class_final.txt"
#define ATTR_BOOT "pg_attr.txt"
#define TYP_BOOT "pg_type.txt"
#define TABLE_BOOT "tables.txt"

#define DB_BOOTTYPE "pg_database"
#define SCHEMA_BOOTTYPE "pg_schema"
#define CLASS_BOOTTYPE "pg_class"
#define ATTR_BOOTTYPE "pg_attr"
#define TYPE_BOOTTYPE "pg_type"
#define TABLE_BOOTTYPE "table"
#define SAVE_BOOTTYPE "save"


#define CMD_BOOTSTRAP 1
#define CMD_USE 2
#define CMD_SET 3
#define CMD_SHOW 4
#define CMD_DESC 5
#define CMD_UNLOAD 6
#define CMD_SCAN 7
#define CMD_RESTORE 8
#define CMD_ADD 9
#define CMD_XFS 10
#define CMD_PARAM 11
#define CMD_SHOWPARAM 12
#define CMD_RESETPARAM 13
#define CMD_SHOWTYPE 14
#define CMD_DROPSCAN 15
#define CMD_INFO 16
#define CMD_CHECKWAL 17
#define CMD_META 18

#define CMD_EXIT 254
#define CMD_UNKNOWN 255

#define INT2ARRAY 0
#define INT4ARRAY 1
#define INT8ARRAY 2
#define FLOAT4ARRAY 3
#define FLOAT8ARRAY 4
#define TIMESTAARRAY 5
#define DATEARRAY 6
#define BOOLARRAY 7
#define VARCHARARRAY 8


#define HARRAYATTR 0
#define HARRAYDEL 1
#define HARRAYTAB 2
#define HARRAYINT 3
#define HARRAYTOAST 4
#define HARRAYBLOCK 5
#define HARRAYLLINT 6

#define DEL 0
#define DROP 1
#define DELRESTORE 2

#define SCANINIT 0
#define RESTOREINIT 1

#define TxRestore 0
#define periodRestore 1


#define SQLform 1
#define CSVform 2
#define DBform 3

#define FormmerHalf 0
#define LatterHalf 1
#define FULL 2
#define None 3

#define UTF8encoding 0
#define GBKencoding 1

#define UPDATEtyp 0 
#define DELETEtyp 1

typedef unsigned short uint16;	/* == 16 bits */
typedef unsigned int uint32;	/* == 32 bits */
typedef unsigned long long int uint64;


// #define pgDatabaseFile "global/1262"
// #define pgSchemaFile "2615"
// #define pgClassFile "1259"
// #define pgAttrFile "1249"
// #define pgTypeFile "1247"
// typedef unsigned int uint32;	
typedef uint32 TransactionId;
typedef long long int int64;	/* == 64 bits */
typedef int64 TimestampTz;
typedef int64 pg_time_t;
typedef unsigned int Oid;
typedef uint32 BlockNumber;

typedef struct
{
	char *oriTyp;
	char *stdTyp;
}TypMapping;

typedef struct 
{
	char name[100];
	size_t value;
}params;


static TypMapping typmap_table[] =
{
	{
		"uint8", "bigint"
	},
	{
		"int8", "bigint"
	},
	{
		"int4", "int"
	},
	{
		"uint4","int"
	},
	{
		"xid", "int"
	},
	{
		"int2", "smallint"
	},
	{
		"uint2", "smallint"
	},
	{
		"int1", "tinyint"
	},
	{
		"uint1", "tinyint"
	},
    {
        "oradate","timestamp"
    },
    {
        "_oradate","_timestamp"
    },
	{
		"bpchar","char"
	},
	{
		"character","char"
	},
	{
		"varchar2","varchar"
	},
	{
		"varcharn","varchar"
	},
	{
		"text","varchar"
	},
	{
		"json","varchar"
	},
	{
		"xml","varchar"
	},
	{
		"longblob","blob"
	}
};

typedef struct 
{
	char *attname;
	char *attalign;
	char *attlen;
}addTabMapping;

static addTabMapping alignLen_table[] =
{
	{
		"float4","i","4"
	},
	{
		"varchar","i","-1"
	},
	{
		"_char","i","-1"
	},
	{
		"_bool","i","-1"
	},
	{
		"bpchar","i","-1"
	},
	{
		"_int","i","-1"
	},
	{
		"timetz","d","12"
	},
	{
		"bytea","i","-1"
	},
	{
		"oid","i","4"
	},
	{
		"date","i","4"
	},
	{
		"_varchar","i","-1"
	},
	{
		"geography","d","-1"
	},
	{
		"inet","i","-1"
	},
	{
		"int","i","4"
	},
	{
		"bit","i","-1"
	},
	{
		"numeric","i","-1"
	},
	{
		"macaddr","i","6"
	},
	{
		"smallint","s","2"
	},
	{
		"text","i","-1"
	},
	{
		"timestamp","d","8"
	},
	{
		"float8","d","8"
	},
	{
		"bigint","d","8"
	},
	{
		"geometry","d","-1"
	},
	{
		"_timestamp","d","-1"
	},
	{
		"interval","d","16"
	},
	{
		"_bigint","d","-1"
	},
	{
		"uuid","c","16"
	},
	{
		"_oid","i","-1"
	},
	{
		"jsonb","i","-1"
	},
	{
		"time","d","8"
	},
	{
		"bool","c","1"
	},
	{
		"raster","d","-1"
	},
	{
		"json","i","-1"
	},
	{
		"_date","i","-1"
	},
	{
		"char","c","1"
	},
	{
		"_float8","d","-1"
	},
	{
		"name","c","64"
	},
	{
		"timestamptz","d","8"
	},
	{
		"_text","i","-1"
	},
	{
		"_float4","i","-1"
	},
	{
		"varbit","i","-1"
	},
	{
		"_smallint","i","-1"
	}
};

char *getVersionNum(char initDBPath[1024]);

/* Database structure */
typedef struct
{
	char oid[100];
	char database[50];
	char tbloid[100];
	char dbpath[MAXPGPATH];
} DBstruct;

/* Schema structure */
typedef struct
{
    char nspname[64];
    char oid[64];
}SCHstruct;

/* Table structure */
typedef struct
{
	char oid[50];
    char filenode[50];
    char toastoid[50];
    char toastnode[50];
    char nsp[50];
	char tab[50];
    char attr[10240];
    char typ[10240];
	char nattr[50];
	char attmod[1024];
	char attlen[1024];
	char attalign[1024];
} TABstruct;

typedef struct
{
	char attname[100];
	char atttyp[100];
	char attlen[10];
	char attalign[2];
	char attalignby[5];
} pg_attributeDesc;

/* Table size structure */
typedef struct
{
	char tab[50];
    char vol[100];
} TABSIZEstruct;

/* Attribute structure */
typedef struct
{
	char relid[50];
    char attr[50];
	char typid[50];
	char attlen[20];
	char attrnum[50];
	char attrmod[50];
	char attalign[2];
} ATTRstruct;

/* Extended attribute structure */
typedef struct
{
	char oid[50];
    char attr[10240];
    char typ[10240];
	char attmod[1024];
	char natt[30];
} ATTRUltrastruct;


typedef uint16 OffsetNumber;
/* TOAST chunk info structure */
typedef struct chunkInfo
{
	Oid toid;
	uint32 chunkid;
	BlockNumber blk;
	OffsetNumber toff;
	int suffix;
}chunkInfo;

typedef struct dsPageOff
{
	off_t pageOff;
	uint32 itemOff;
}dsPageOff;


/* Type structure */
typedef struct
{
	char oid[50];
    char typname[50];
} TYPstruct;

typedef struct
{
	char walnames[50];
} WALFILE;

typedef struct Node {
    void* data;
    struct Node* next;
} Node;

typedef struct {
    Node** table;
    int allocated;
    int used;
} harray;

/* Delete operation structure */
typedef struct
{
	char startLSN[50];
	char endLSN[50];
	char startLSNforTOAST[50];
	char endLSNforTOAST[50];
	TransactionId tx;
	int delCount;
	TimestampTz txtime;
	char datafile[50];
	char oldDatafile[50];
	char toast[50];
	char oldToast[50];
	char tabname[50];
	char typ[10240];
	TABstruct *taboid;
	parray *Txs;
	char startwal[70];
	char endwal[70];
} DELstruct;

typedef struct
{
	TransactionId tx;
	Oid datafile;
	Oid toast;
	char *attribute;
	TimestampTz txtime;
} TRUNCstruct;

typedef struct {
	char *filename;
	char *attr2Decode;
	char *bootFileName;
	char *BOOTTYPE;
	char *logPathSucc;
	char *logPathErr;
    off_t start_offset;
    off_t end_offset;
	int hundred;
} readItemsArgs;

typedef struct TypeSolution {
    char **types; // Array of type names for each column
    int **cur_off; // Array of type names for each column
    struct TypeSolution *next;
	int nAttr;
	int solnum;
	char *attrNames;
} TypeSolution;

#define MAX_COL_NUM_DROPSCAN 500

typedef int (*decodeFuncs) (const char *tupleSingleAttr, unsigned int tupleSizeSingleAttr,unsigned int *sizeDecoded);

typedef struct dropContext
{
	harray *toastOids;
	char tabname[100];
	char types[102400];
	char BOOTTYPE[20];
	char *csvPrefix;
	char *recPath;
	char *dictPath;
	int nAttr;
	decodeFuncs attr2Process[MAX_COL_NUM_DROPSCAN];
	int totalItems;
	int totalBlks;
	off_t currSrtOffset;
	int currItems;
	int currBlks;
	int ErrItems;
	int renamed;
	off_t lastMatchedOffset;
	int matched;
	int has_gibberish;
	int total_gibberish;
	parray *PageOffs;
	uint32 pageSavedNum;
	int (*dcProc)(int, struct dropContext *, char *, int *, int *, off_t *, int);
	int (*dropFileRename)(struct dropContext *);
	void (*commaSeperFunc)(char *str,FILE *file);
}dropContext;

typedef struct systemDropContext
{
	TABstruct *taboid;
	pg_attributeDesc *allDesc;
	decodeFuncs attr2Process[MAX_COL_NUM_DROPSCAN];
	char savepath[50];
	char *xman;
}systemDropContext;


typedef struct dropElem
{
	char tabname[100];
	TransactionId Tx;
	Oid oid;
	Oid filenode;
	TimestampTz timestamp;
}dropElem;


typedef struct dropScanIdxContext
{
	off_t lastPageOffset;
}dropScanIdxContext;

typedef struct LsnBlkInfo
{
	char LSN[30];
	BlockNumber blk;
	int isFatal;
}LsnBlkInfo;



unsigned int hash(harray* harray, char *val, int allocatedVal);

harray *harray_new(int flag);

void harray_append(harray* harray, int flag, void *elem, uint64 val);

void harray_expand(harray *array,int flag, size_t newsize);

int harray_search(harray* harray, int flag , uint64 val);

size_t harray_num(const harray *array);

void harray_free(harray* harray);


int getLineNum(char *filename);

void trimLastValue(const char* str1, char* str2);

void getStdTyp(char *str,char *ret);

int createDir(char *dirname);

int removeEntry(const char *path);

int removeDir(const char *path);

void getAttrTypForm(char attr[10240],char typ[10240],char attmod[10240]);

void getAttrTypSimple(char res[10240],char typ[10240]);

void ata2DDL(char attr[10240],char typ[10240],char attmod[10240],FILE *ddl);

FILE* fileGetLines(char *filename,int *numlines);


void commaStrWriteIntoFileDB(char *str,FILE *file);

void commaStrWriteIntoFileSCH_TYP(char *str,FILE *file);

void dboidWithTblIntoFile(DBstruct *databaseoid,int dosize,char *file);

void AttrXman2AttrStrcut(char *src,ATTRstruct *oneattr);

void commaStrWriteIntoDecodeTab(char *str,FILE *file);

void genCopy(char *csvpath,FILE *copyfp);

void addQuotesToString(char *str);

int getPgAttrDesc(TABstruct *taboid,pg_attributeDesc *allDesc);

void getAttrUltra(harray *attr_harray,TYPstruct *typoid,int typoidlen,TABstruct *taboidTMP,int tabSize);

void getTypForTrunc(harray *attr_harray,parray *GetTxRetAll,TYPstruct *typoid,int typoidlen,char *TxRequested,TRUNCstruct *targetTrunc);

int schemaInDefaultSHCS(char *a);

void trim(char* str1, char* str2);

void get_parent_directory(char* path);

static void trim_directory(char* path);

void processAttMod(const char attrTyp[],const char modStr[],char *ret);

char *processAttModInner(char *tokenTyp,char *tokenLen);

void removeSpaces(char *str);

void cleanPadding(const char *buffer, unsigned int *buff_size,int *padding,int *temppadding);

char *xman2Insertxman(char *xman,char *tablename);

void trim_char(char *str, char c);

void prepareRestore();

bool txInDelArrayOrNot(TransactionId Tx,parray *TxTime_parray,int *index,int restoreMode);

void cleanNoTimeTxArray(parray *array);

bool ifTxArrayAllWithTime(parray *array);

int isParameter(char *value,int *type);

int compare_walfile(const void *a, const void *b);

int countFilesBetween(const char* filename1, const char* filename2);

void trimLeadingSpaces(char **str);

bool txInTxArrayOrNot(TransactionId Tx,parray *Tx_parray,int restoreMode);

int attrIsDropped(char *a);

void setEncoding_there(int setting);

void getMetaFromManul(char *sqlPath,char *CUR_DB);

parray *getfilenameParray(char *path2search);

char *hexBuffer2Str(const char *buff, char *hexBuffer,int size);

void printfParam(char *a,char *b);

void mergeToast(char *PATH,char *toastfile);

int* findMinMaxNumbers(const char* path);

char* quotedIfUpper(const char* input);

int get_system_uuid(char *uuid);

int get_mac_address(char *mac_str);

int get_system_uuid(char *uuid);

int get_ip_address(char *ip_str);

int getPGVersion(char *PGDATA);

char *xman2Updatexman(parray *newxman_arr,parray *oldxman_arr,pg_attributeDesc *allDesc,char *tabname);

bool shortCMDMatched(char *cmd);

void elemforTimeINIT(DELstruct *elem);

int compare_prefix(const char *a, int a_len, const char *b, int b_len);

int compare_hex(const char *s1, const char *s2);

bool compareHexStrings(const char *a, const char *b);

bool lsnIsReached(uint64 pre,uint64 suff,char *endLSN);

void loadTbspc(DBstruct *databaseoid,int dbsize,char *PGDATA);

int chunkInfo_compare(const void *a, const void *b);

bool ends_with(const char *str, const char *suffix);

bool is_valid_string(const char *str);

int dropFileRename(dropContext *dc);

int count_effective_lines(const char *filename);

off_t logical_to_physical(off_t logical_offset, struct fiemap *fiemap);

void print_file_blocks(const char *filename,const char *toastfilename);

uint32_t compute_block_hash(const char* block);

int compare_offt(const void *a, const void *b);

int dropFileRenameforToast(char *csvPrefix,off_t currSrtOffset,int currBlks,int currItems);

int dropFileRenameforFinal(dropContext *dc);

void initToastHashforDs(dropContext *dc);

int initPageOffsforDs(dropContext *dc);

int compare_blk(const void *a, const void *b);

int compare_chunkid(const void *a, const void *b);

int compare_tx(const void *a, const void *b);

parray **group_chunks(parray *input, int *num_groups);

parray *list_directories(const char *path);

void cleanDir(const char *path);

bool is_valid_postgres_timestamp(const char *input);

void genCopyForDs(char *tabname);

void printDropScanDirs(parray *dcs);

int is_address_valid(void *addr);

int copyFile(const char *src,const char *dest);

void replaceArchPath();

void trim_whitespace(char *str);

bool has_gibberish(const char *xman);

void getDropScanOids(TABstruct *taboid,char *flag);

systemDropContext* initSystemDropContext(char *flag);

char* read_last_non_empty_line(const char* filename);

void *harray_get(harray* harray, int flag , uint32 val);

bool unwantedCol(char *colname);

char *get_field(char delimiter,const char *str, int a);

int countCommas(const char *str);

int initUnloadHash(char *filename,harray *rechash);

bool chkIdxExist();

void getAttrAlignAndAttlen(char *attr,char *alignStr,char *attlenStr);

int initIdxFileHash(char *filename,harray *idxhash);

void replace_improper_symbols(char* s);

bool isValidString(const char* str);

char *get_symlink_target(const char *symlink_path);

long long getFreeDiskSpace();

int is_file_larger_than(const char *filename, long long n_bytes);

void setup_crash_handlers(void);

#endif /* TOOLS_H */


char* execCMD(const char *command);

void rmLastn(char *str);