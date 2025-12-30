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
 *
 * read.c - Data reading and parsing module for PostgreSQL data files
 */

#include "read.h"
#include "dropscan_fs.h"

void setRestypeNoShow(char *third);
void CHECKWAL(void);
void getSCHFromCLasstxt(char *filename);
int ToastChunkforOid(const char *tuple_data, unsigned int tuple_size, uint32 *chunk_id, Oid *toastoid);
void pgGetTxforArch();
void setRestoreMode_there(int setting);
void setExportMode_there(int setting);
void setResTyp_there(int setting);

static struct timespec start_time;

/**
 * unloadTimer - Simple timing utility for performance measurement
 *
 * @marker: "s" to start timer, "e" to end and print elapsed time
 *
 * Measures elapsed time between start and end calls using monotonic clock.
 */
void unloadTimer(const char* marker) {
    if (marker == NULL) return;

    if (marker[0] == 's') {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
    } else if (marker[0] == 'e') {
        struct timespec end_time;
        clock_gettime(CLOCK_MONOTONIC, &end_time);

        long time_diff_ns = (end_time.tv_sec - start_time.tv_sec) * 1000000000L;
        time_diff_ns += (end_time.tv_nsec - start_time.tv_nsec);

        double time_diff_ms = time_diff_ns / 1000000.0;
        double time_diff_s = time_diff_ns / 1000000000.0;
        #ifdef CN
        printf("%s耗时 %.2f 秒%s\n",COLOR_UNLOAD,time_diff_s,C_RESET);
        #else
        printf("%sExecution Time %.2f seconds%s\n",COLOR_UNLOAD,time_diff_s,C_RESET);
        #endif
    }
}

int solnum;
char initDBPath[512];
char initArchPath[512];
char CURDBFullPath[550]={0};
char waldatadir[1024]={0};
char archivedir[1024]={0};
char diskPath[1024]={0};
parray *pgdataExclude=NULL;

int nWal;
int nArch;
char lastc;
int isDelScanned = 0;
int isDropScanned = 0;

int isTxScanned = 0;
int isPeriodScanned = 0;

int isSingleDB = 0;
int SCANCount = 0;

char pg_filenode[20]="pg_filenode.map";

char manualSrtWal[100];
char manualEndWal[100];

TimestampTz *SrtTime;
TimestampTz *EndTime;

off_t dropScanSrtOff = 0;
int isoMode = 0;
int blkInterval = 5;
int itemspercsv = 100;
harray *dupPages=NULL;
uint32 BIGJUMP_GENIDX;
uint32 BIG_JUMP;
uint32 BIG_JUMP_ACTUAL;

int NBLKS_BIGJUMP;
int JUMP_STEP_IDX;
int CSVINTERVAL_ITEMS;

harray *TxXman_harray = NULL;
parray *TxSaved_paaray = NULL;
parray *GetTxRetAll = NULL;

int archWaldirNum = 0;
int pgwalWaldirNum = 0;
int restoreMode = periodRestore;
int exmode = CSVform;
int pduEncoding = UTF8encoding;
int resTyp = DELETEtyp;
#ifdef CN
char resStr[10]="删除";
#else
char resStr[10]="deleted";
#endif
int dropExist1;

harray *toastTaboid_harray = NULL;
long long ISOFILE_SIZE = 0;

Oid ErrToastId = 0;

char glb_filename[1024];
char glb_attr2Decode[102400];
char glb_bootFileName[100];
char glb_BOOTTYPE[10];
char glb_logPathSucc[1024];
char glb_logPathErr[1024];

static int addNum = 0;

typedef struct
{
  char ATTR_ARRAY[150];
  char sourceFile[30];
  char *SOME_BOOT;
  char *SOME_BOOTTYPE;
}boot2FileParams;

boot2FileParams b2fPArray[4]={
    {SCH_ATTR,"pgSchemaFile",SCHEMA_BOOT,SCHEMA_BOOTTYPE},
    {CLASS_ATTR, "pgClassFile", CLASS_BOOT, CLASS_BOOTTYPE},
    {ATTR_ATTR, "pgAttrFile", ATTR_BOOT, ATTR_BOOTTYPE},
    {TYPE_ATTR, "pgTypeFile", TYP_BOOT, TYPE_BOOTTYPE}
};

char *pgDatabaseFilenode="global/1262";
char *pgClassFilenode="1259";
char *pgAttrFilenode="1249";
char *pgTypeFilenode="1247";
char *pgSchemaFilendoe="2615";

char pgDatabaseFile[MAXPGPATH];
char pgClassFile[MAXPGPATH];
char pgAttrFile[MAXPGPATH];
char pgTypeFile[MAXPGPATH];
char pgSchemaFile[MAXPGPATH];

char *pgDatabaseFilex=NULL;
char *pgClassFilex=NULL;
char *pgAttrFilex=NULL;
char *pgTypeFilex=NULL;
char *pgSchemaFilex=NULL;

#define buffer_size 100
#define DISPLAYCount 20

int	blockStart = -1;
int	blockEnd = -1;
char *delimiter=",";
char *CUR_DB=NULL;
char *CUR_DBDIR=NULL;
char *CUR_SCH=NULL;

TABstruct *taboid;
harray *toastHash;
int tabSize=0;
TABSIZEstruct *tabVol;
SCHstruct *schoid;
int schemasize=0;
DBstruct *databaseoid;
int dosize = 0;
char *bootPadding="  ";
char *space="      ";

decodeFunc attr2Process[MAX_COL_NUM] = {NULL};
commaSeperFunc seperFunc2Use=NULL;

/**
 * str_to_timestamptz_og - Convert string to PostgreSQL TimestampTz
 *
 * @time_str: Time string in format "YYYY-MM-DD HH:MM:SS"
 *
 * Parses a time string and converts it to PostgreSQL's internal
 * TimestampTz format (microseconds since PostgreSQL epoch).
 *
 * Returns: TimestampTz value on success, -1 on parse error, 0 on conversion error
 */
TimestampTz str_to_timestamptz_og(const char *time_str) {
    struct tm tm;
    memset(&tm, 0, sizeof(struct tm));
    if (strptime(time_str, "%Y-%m-%d %H:%M:%S", &tm) == NULL) {
        return -1;
    }

    time_t t = mktime(&tm);
    if (t == -1) {
        printf("Error converting to time_t\n");
        return 0;
    }

    TimestampTz result = (TimestampTz)(t * USECS_PER_SEC);

    result -= (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC;

    return result;
}

#include <sys/ioctl.h>
#include <linux/fs.h>
#include <fcntl.h>
#include <linux/fiemap.h>

/**
 * readItems - Read and decode tuples from PostgreSQL data file
 *
 * @taboid:       Table structure containing metadata
 * @filename:     Path to the data file
 * @attr2Decode:  Comma-separated list of attribute types
 * @bootFileName: Output file name for decoded data
 * @BOOTTYPE:     Boot type identifier
 * @logPathSucc:  Path for success log file
 * @logPathErr:   Path for error log file
 *
 * Reads a PostgreSQL heap data file page by page, decodes each tuple
 * according to the table schema, and writes results to output file.
 *
 * Returns: SUCCESS_RET on success, FAILURE_RET on failure
 */
int readItems(TABstruct *taboid,char *filename,char attr2Decode[],char *bootFileName,char *BOOTTYPE,char *logPathSucc,char *logPathErr)
{
    unsigned int pageSize;
    unsigned int keepDumping=1;
    unsigned int bytesToFormat;
    FILE *bootFile;
    int nItemsSucc=0;
    int nItemsErr=0;
    int nPages=0;
    int nCurPageItems=0;
    int failExistflag=0;
    char filenameFINNAL[1024]="";
    pg_attributeDesc *allDesc=NULL;
    int datafileExist = 0;
    char result[MAXPGPATH];

    FILE *logSucc=fopen(logPathSucc,"a");
    FILE *logErr=fopen(logPathErr,"a");

    if(strcmp(BOOTTYPE,TABLE_BOOTTYPE) == 0){
        int nAttr = atoi(taboid->nattr);
        allDesc = (pg_attributeDesc*)malloc(nAttr*sizeof(pg_attributeDesc));
        dropExist1 = getPgAttrDesc(taboid,allDesc);
    }

    int hundred;
    for(hundred=0;hundred<NUM1G;hundred++){
        if ( hundred == 0 ){
            sprintf(filenameFINNAL,"%s",filename);
        }
        else{
            sprintf(filenameFINNAL,"%s.%d",filename,hundred);
        }
        if (access(filenameFINNAL, F_OK) != -1) {
            keepDumping = 1;
            FILE *fp;
            fp = fopen(filenameFINNAL, "rb");

            #if DROPDEBUG == 1
            int fd = open(filenameFINNAL, O_RDONLY);
            struct fiemap *fiemap;
            char buffer[sizeof(struct fiemap) + sizeof(struct fiemap_extent) * 256];
            fiemap = (struct fiemap *)buffer;
            memset(fiemap, 0, sizeof(struct fiemap));
            fiemap->fm_start = 0;
            fiemap->fm_length = ~0ULL;
            fiemap->fm_flags = 0;
            fiemap->fm_extent_count = 256;

            if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
                perror("ioctl");
                close(fd);
                return;
            }
            struct fiemap_extent *extent = &fiemap->fm_extents[0];
            off_t beginOffset = (long long)extent->fe_physical;
            off_t lastMatchedOffset=0;
            off_t currOffset = 0;
            #endif

            if (!fp)
            {
                char err1[2048];
                #ifdef EN
                sprintf(err1,"\nFAIL TO OPEN TABLE <%s> DATAFILE <%s> ,PLEASE CHECKOUT DATAFILE\n",
                    bootFileName,filenameFINNAL);
                #else
                sprintf(err1,"\n无法打开表 <%s> 的数据文件 <%s> ,请检查数据文件是否存在\n",
                    bootFileName,filenameFINNAL);
                #endif
                fputs(err1,logErr);
                dropExist1=0;
                return FAILURE_RET;
            }
            pageSize = BLCKSZ;

            BlockNumber	currentBlockNo = 0;
            char *block = (char *)malloc(pageSize);
            memset(block,0,pageSize);

            if (!block)
            {
                printf("\nFAILED TO ALLOCATE SIZE OF <%d> BYTES\n",
                    pageSize);
            }

            resetArray2Process(attr2Process);

            memset(result,0,MAXPGPATH);
            if(exmode == CSVform)
                sprintf(result,"%s/%s/%s%s",CUR_DB,CUR_SCH,bootFileName,".csv");
            else if(exmode == SQLform)
                sprintf(result,"%s/%s/%s%s",CUR_DB,CUR_SCH,bootFileName,".sql");
            if (strcmp(BOOTTYPE,TABLE_BOOTTYPE) == 0){
                if (hundred >0){
                    bootFile = fopen(result, "a");
                }
                else{
                    bootFile = fopen(result, "w");
                }
            }
            else{
                bootFile = fopen(bootFileName, "w");
            }
            if (!bootFile)
            {
                char err1[1050];
                sprintf(err1,"\nFailed to open target csv file <%s>, please check\n",
                    result);
                printf("%s",err1);
                fputs(err1,logErr);
                dropExist1=0;
                return FAILURE_RET;
            }
            char *attr2DecodeTMP = (char *)malloc((strlen(attr2Decode)+1)*sizeof(char));
            strcpy(attr2DecodeTMP,attr2Decode);
            int nAttr=0;

            char *attrChars[MAX_COL_NUM];

            for (int i = 0; i < MAX_COL_NUM; i++) {
                attrChars[i] = (char *)malloc(20);
            }
            char temp[50];
            char *token = strtok(attr2DecodeTMP, ",");
            while (token != NULL) {
                if (nAttr >= MAX_COL_NUM) {
                    printf("Exceeded attrChars array capacity\n");
                }
                strncpy(temp, token, sizeof(temp) - 1);
                strcpy(attrChars[nAttr],temp);
                nAttr++;
                token = strtok(NULL, ",");
            }

            int a;
            for (a=0;a<nAttr;a++){
                char ret[100];
                memset(ret,0,100);
                getStdTyp(attrChars[a],ret);
                if(!AddList2Prcess(attr2Process,ret,BOOTTYPE)){
                    dropExist1=0;
                    return FAILURE_RET;
                }
            }
            for (int i = 0; i < MAX_COL_NUM; i++) {
                free(attrChars[i]);
            }

            if(!strcmp(BOOTTYPE,DB_BOOTTYPE)){
                seperFunc2Use=&commaStrWriteIntoFileDB;
            }
            else if(!strcmp(BOOTTYPE,SCHEMA_BOOTTYPE) || !strcmp(BOOTTYPE,TYPE_BOOTTYPE)){
                seperFunc2Use=&commaStrWriteIntoFileSCH_TYP;
            }
            else if(!strcmp(BOOTTYPE,CLASS_BOOTTYPE)){
                seperFunc2Use=&commaStrWriteIntoFileCLASS;
            }
            else if(!strcmp(BOOTTYPE,ATTR_BOOTTYPE)){
                seperFunc2Use=&commaStrWriteIntoFIleAttr;
            }
            else if(!strcmp(BOOTTYPE,TABLE_BOOTTYPE)) {
                seperFunc2Use=commaStrWriteIntoDecodeTab;
            }
            while(keepDumping){

                #if DROPDEBUG == 1
                off_t current_logical = ftello(fp);
                if (current_logical == -1) {
                    perror("ftello");
                    break;
                }
                currOffset = logical_to_physical(current_logical, fiemap);
                #endif

                #if DROPDEBUG == 0
                if(strcmp(BOOTTYPE,TABLE_BOOTTYPE) == 0){
                    infoDecodeLive(bootFileName,nPages,nItemsSucc);
                    fflush(stdout);
                }
                #endif

                bytesToFormat = fread(block, 1, pageSize, fp);
                if (bytesToFormat == 0)
                {
                    keepDumping = 0;
                }
                else{
                    Page page = (Page) block;
                    PageHeader	p = (PageHeader) page;
                    uint16		pagesizeCheck = 0;

                    /*
                    * Don't verify page data unless the page passes basic non-zero test
                    */
                    if (!PageIsNew(page) && strcmp(BOOTTYPE,TABLE_BOOTTYPE) == 0)
                    {
                        pagesizeCheck = p->pd_pagesize_version;
                    }

                    unsigned int x;
                    unsigned int i;
                    unsigned int itemSize;
                    unsigned int itemOffset;
                    unsigned int itemFlags;
                    ItemId		itemId;
                    int	maxOffset;

                    maxOffset = PageGetMaxOffsetNumber(page);

                    if (maxOffset == 0)
                    {
                        #ifdef EN
                        printf("\n\t|-Block %d Empty Page Or Page Corruptted, Skipped\n",nPages);
                        #else
                        printf("\n\t|-块号%d 空页面或页面已损坏 ,已跳过\n",nPages);
                        #endif
                        nPages++;
                        continue;
                    }

                    bool all_visible = PageIsAllVisible(page);
                    for(x= 1 ; x < maxOffset+1 ; x++){

                        itemId = PageGetItemId(page, x);

                        itemFlags = (unsigned int) ItemIdGetFlags(itemId);
                        itemSize = (unsigned int) ItemIdGetLength(itemId);
                        itemOffset = (unsigned int) ItemIdGetOffset(itemId);

                        HeapTupleHeader header = (HeapTupleHeader) &block[itemOffset];

                        TransactionId xmax = HeapTupleHeaderGetRawXmax(header);
                        TransactionId xmin = HeapTupleHeaderGetRawXmin(header);

                        if (itemFlags == LP_NORMAL){
                            unsigned int processed_size = 0;

                            bool		valid;
                            if (all_visible)
                                valid = true;
                            else
                                valid = HeapTupleSatisfiesVisibility(header);

                            if( !valid ){
                                continue;
                            }

                            char *xman=xmanDecode(dropExist1,allDesc,attr2Process,&block[itemOffset],itemSize,BOOTTYPE,logSucc,logErr);
                            datafileExist=1;

                            if(strcmp(BOOTTYPE,DB_BOOTTYPE) == 0 && strcmp(xman,"NoWayOut") != 0){

                                char *target =  get_field('\t',xman,2) ;

                                if(strcmp(target, "template1") == 0 || 
                                    strcmp(target, "template0") == 0 ||
                                    strcmp(target, "security") == 0)
                                {
                                    continue;
                                }

                            }

                            if ( strcmp(xman,"NoWayOut") == 0 ){
                                nItemsErr++;
                                failExistflag = 1;
                                continue;
                            }
                            else{
                                char *xmanret=NULL;
                                if(exmode == CSVform){
                                    xmanret = xman;
                                }
                                else if (exmode == SQLform){
                                    xmanret = xman2Insertxman(xman,bootFileName);
                                }
                                seperFunc2Use(xmanret,bootFile);
                                nItemsSucc++;

                                nCurPageItems++;

                            }
                        }
                    }
                    #if DROPDEBUG == 1
                    FILE *a = fopen("unloaddropscan.txt","a");
                    char content[1000] = {0};
                    printf("Offset:%-10lld data page:%d records count:%d total records:%d byte offset from last page:%-10lld\n",currOffset,nPages,maxOffset,nItemsSucc,currOffset-lastMatchedOffset);
                    sprintf(content,"Offset:%-10lld data page:%d records count:%d total records:%d byte offset from last page:%-10lld\n",currOffset,nPages,maxOffset,nItemsSucc,currOffset-lastMatchedOffset);
                    lastMatchedOffset=currOffset;
                    fputs(content,a);
                    fclose(a);
                    #endif
                    nPages++;
                    currentBlockNo++;
                }
            }

            if (strcmp(BOOTTYPE,TABLE_BOOTTYPE) == 0){
                int nTotal=nItemsErr+nItemsSucc;
                char succ1[1050];
                #ifdef EN
                sprintf(succ1,"<%s>-<%s> Completed\n\t|-%d Pages , %d Records Decoded IN TOTAL.SUCCESS: %d ;FAILED: %d\n\t|-File Path: %s\n",
                                bootFileName,filenameFINNAL,nPages,nTotal,nItemsSucc,nItemsErr,result);
                #else
                sprintf(succ1,"\n\t|-表 %s(%s) 解析完成\n\t|-%d 个数据页 ,共计 %d 条数据. 成功 %d 条; 失败 %d 条\n\t|-文件路径: %s\n\n",
                    bootFileName,filenameFINNAL,nPages,nTotal,nItemsSucc,nItemsErr,result);
                #endif
                infoUnlodResult(bootFileName,filenameFINNAL,nPages,nTotal,nItemsSucc,nItemsErr,result);
                if(failExistflag == 0)
                    fputs(succ1,logSucc);
            }
            free(block);
            fclose(bootFile);
            fclose(fp);
            free(attr2DecodeTMP);
        } else {
            break;
        }
    }
    fclose(logSucc);
    fclose(logErr);
    if(!datafileExist){
        unlink(result);
        dropExist1=0;
        return FAILOPEN_RET;
    }
    if(failExistflag){
        dropExist1=0;
        return FAILURE_RET;
    }
    dropExist1=0;
    return 1;
}

/**
 * execCmd - Execute user command from CLI input
 *
 * @USR_CMD: Command type identifier
 * @former:  First command argument
 * @latter:  Second command argument
 * @third:   Third command argument
 * @fourth:  Fourth command argument
 *
 * Main command dispatcher that routes user input to appropriate handlers.
 */
void execCmd(int USR_CMD,char *former,char *latter,char *third,char *fourth)
{
    if ( USR_CMD == CMD_EXIT ){
        EXIT();
    }
    if ( USR_CMD == CMD_UNKNOWN){
        printUsage();
        return;
    }
    if( USR_CMD == CMD_SHOWTYPE){
        #ifdef CN
        printf("%s\n当前支持的数据类型：%s\n",COLOR_WARNING,C_RESET);
        #else
        printf("%s\nDatatype Supported:%s\n",COLOR_WARNING,C_RESET);
        #endif
        showSupportTypeCom();

        return;
    }

    if (USR_CMD == CMD_BOOTSTRAP){
        setRestypeNoShow("delete");

        sprintf(pgDatabaseFile,"%s%s",initDBPath,pgDatabaseFilenode);
        FILE *dbExist = fopen(pgDatabaseFile,"r");
        if(dbExist){
            fclose(dbExist);
            bootstrap();
        }
        else{
            bootstrap_abnormal();
        }
        strcpy(CUR_DB,"PDU");
        strcpy(CUR_SCH,"public");
        return;
    }

    if( USR_CMD == CMD_DROPSCAN){
        DROP_SCAN(latter,third);
        return;
    }
    if( USR_CMD == CMD_SHOWPARAM ){
        SHOW_PARAM();
        return;
    }
    databaseoid=bootDBStruct(DB_BOOT,1);
    if ( databaseoid == NULL ){
        return;
    }
    dosize = getLineNum(DB_BOOT);
    if( USR_CMD == CMD_USE ){
        useDB(former,latter);
        return;
    }

    if( USR_CMD == CMD_SET ){
        setSCH(former,latter);
        return;
    }

    if( USR_CMD == CMD_SHOW ){
        SHOW(former);
        return;
    }

    if( USR_CMD == CMD_DESC ){
        DESC(former,latter);
        return;
    }

    if( USR_CMD == CMD_UNLOAD ){
        setRestypeNoShow("delete");
        UNLOAD(former,latter,third);
        return;
    }

    if( USR_CMD == CMD_SCAN ){
        SCAN(former,latter);
        return;
    }

    if( USR_CMD == CMD_CHECKWAL ){
        CHECKWAL();
        return;
    }

    if( USR_CMD == CMD_META ){
        META(latter,third);
        return;
    }

    if( USR_CMD == CMD_RESTORE ){
        RESTORE(former,latter,third,fourth);
        return;
    }

    if( USR_CMD == CMD_ADD ){
        ADD_TAB(former,latter,third,fourth);
        return;
    }

    if( USR_CMD == CMD_PARAM ){
        SET_PARAM(former,latter,third,fourth);
        return;
    }

    if( USR_CMD == CMD_RESETPARAM ){
        RESET_PARAM(former,latter,third);
        return;
    }

    if( USR_CMD == CMD_INFO){
        INFO(latter);
    }

}

/**
 * bootDBStruct - Initialize database structure from metadata file
 *
 * @filename:    Path to database metadata file
 * @isCompleted: Flag indicating if initialization is complete
 *
 * Parses database metadata file and builds DBstruct array.
 *
 * Returns: Pointer to DBstruct array
 */
DBstruct* bootDBStruct(char *filename,int isCompleted)
{
    int numLines = 0;
    FILE *file = fileGetLines(filename,&numLines);

    if(file == NULL){
        #ifdef CN
        printf("%s请先使用<b;>命令初始化数据字典%s\n",COLOR_SUCC,C_RESET);
        #else
        printf("%splease bootstrap database info with command <b;>%s\n",COLOR_SUCC,C_RESET);
        #endif
        return NULL;
    }

    DBstruct *databaseoid = (DBstruct *)malloc(numLines * sizeof(DBstruct));
    if (databaseoid == NULL) {
        perror("Memory allocation failed");
        exit(1);
    }
    if ( ! isCompleted ){
        int i;
        for (i = 0; i < numLines; i++) {
            int ret = fscanf(file, "%s\t%s\t%s", databaseoid[i].oid, databaseoid[i].database, databaseoid[i].tbloid);
            if (!(ret != 3 || ret != 4)) {
                perror("Error reading file");
                exit(1);
            }
        }
    }
    else{
        int i;
        for (i = 0; i < numLines; i++) {
            if (fscanf(file, "%s\t%s\t%s\t%s", databaseoid[i].oid, databaseoid[i].database, databaseoid[i].tbloid, databaseoid[i].dbpath) != 4) {
                perror("Error reading file");
                exit(1);
            }
        }
    }

    fclose(file);
    return databaseoid;

}

/**
 * bootAttrStruct - Initialize attribute structure from metadata file
 *
 * @filename: Path to attribute metadata file
 *
 * Parses attribute metadata and builds hash array for fast lookup.
 *
 * Returns: Pointer to harray containing attribute data
 */
harray* bootAttrStruct(char *filename){
    int numLines = 0;
    FILE *file = fileGetLines(filename,&numLines);

    harray *attr_harray=NULL;
    attr_harray = harray_new(HARRAYATTR);

    int i;
    for (i = 0; i < numLines; i++) {
        ATTRstruct *oneattroid = NULL;
        oneattroid = (ATTRstruct *)malloc(sizeof(ATTRstruct));
        if (oneattroid == NULL) {
            perror("malloc failed for oneattroid\n");
            exit(1);
        }
        if (fscanf(file, "%s\t%s\t%s\t%s\t%s\t%s\t%s", oneattroid->relid, oneattroid->attr,
                                                        oneattroid->typid, oneattroid->attlen,
                                                        oneattroid->attrnum, oneattroid->attrmod,
                                                        oneattroid->attalign) != 7) {
            perror("Error reading file");
            exit(1);
        }

        int relidInt = atoi(oneattroid->relid);
        harray_append(attr_harray,HARRAYATTR,oneattroid,relidInt);
    }

    fclose(file);
    return attr_harray;
}

/**
 * bootTabStruct - Initialize table structure from metadata file
 *
 * @filename:    Path to table metadata file
 * @isCompleted: Flag indicating if initialization is complete
 *
 * Parses table metadata file and builds TABstruct array.
 *
 * Returns: Pointer to TABstruct array
 */
TABstruct* bootTabStruct(char *filename,int isCompleted){
    int numLines = 0;
    FILE *file = fileGetLines(filename,&numLines);
    if(file == NULL)
        return NULL;

    TABstruct *taboid = (TABstruct *)malloc(numLines * sizeof(TABstruct));
    if (taboid == NULL) {
        perror("Memory allocation failed");
        exit(1);
    }

    if ( ! isCompleted ){
        int i=0;
        while (!feof(file)){
            fscanf(file, "%s\t%s\t%s\t%s\t%s\t%s\n", taboid[i].oid, taboid[i].tab, taboid[i].nsp, taboid[i].filenode, taboid[i].toastoid, taboid[i].nattr);
            if(toastTaboid_harray != NULL){
                if(strncmp(taboid[i].tab,"pg_toast_",9) == 0){
                    int toastindex = atoi(taboid[i].oid);
                    TABstruct *ontaboid = (TABstruct*)malloc(sizeof(TABstruct));
                    strcpy(ontaboid->oid,taboid[i].oid);
                    strcpy(ontaboid->filenode,taboid[i].filenode);
                    strcpy(ontaboid->nsp,taboid[i].nsp);
                    strcpy(ontaboid->tab,taboid[i].tab);
                    strcpy(ontaboid->nattr,taboid[i].nattr);
                    harray_append(toastTaboid_harray,HARRAYTAB,ontaboid,toastindex);
                }
            }
            i++;
        }
    }
    else{
        int i;
        for (i = 0; i < numLines; i++) {
            fscanf(file, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",taboid[i].oid,taboid[i].filenode,
                                                                        taboid[i].toastoid, taboid[i].toastnode,
                                                                        taboid[i].nsp,taboid[i].tab,
                                                                        taboid[i].attr,taboid[i].typ,
                                                                        taboid[i].nattr,taboid[i].attmod,
                                                                        taboid[i].attlen,taboid[i].attalign);
        }
    }
    fclose(file);
    return taboid;
}

/**
 * bootSCHStruct - Initialize schema structure from metadata file
 *
 * @filename: Path to schema metadata file
 *
 * Parses schema metadata file and builds SCHstruct array.
 *
 * Returns: Pointer to SCHstruct array
 */
SCHstruct* bootSCHStruct(char *filename){
    int numLines = 0;
    FILE *file = fileGetLines(filename,&numLines);
    if(file == NULL){
        ErrorFileNotExist(filename);
        return NULL;
    }
    SCHstruct *schoid = (SCHstruct *)malloc(numLines * sizeof(SCHstruct));
    if (schoid == NULL) {
        perror("Memory allocation failed");
        exit(1);
    }
    int i;
    for (i = 0; i < numLines; i++) {
        if (fscanf(file, "%s\t%s", schoid[i].oid, schoid[i].nspname) != 2) {
            printf("Error Reading File %s\n",filename);
            exit(1);
        }
    }

    fclose(file);
    return schoid;
}

/**
 * bootTYPStruct - Initialize type structure from metadata file
 *
 * @filename: Path to type metadata file
 *
 * Parses type metadata file and builds TYPstruct array.
 *
 * Returns: Pointer to TYPstruct array
 */
TYPstruct* bootTYPStruct(char *filename){
    int numLines = 0;
    FILE *file = fileGetLines(filename,&numLines);
    TYPstruct *typoid = (TYPstruct *)malloc(numLines * sizeof(TYPstruct));
    if (typoid == NULL) {
        perror("Memory allocation failed");
        exit(1);
    }
    int i;
    for (i = 0; i < numLines; i++) {
        if (fscanf(file, "%s\t%s", typoid[i].oid, typoid[i].typname) != 2) {
            printf("Error Reading File %s",filename);
            exit(1);
        }
    }
    fclose(file);
    return typoid;
}

/**
 * bootforDropScan - Initialize metadata for drop/truncate scan
 *
 * @CUR_DB: Current database name
 *
 * Sets up metadata structures needed for scanning dropped tables.
 */
void bootforDropScan(char *CUR_DB)
{
    char *DBClassFile = malloc(100);
    char *DBAttrFile = malloc(100);
    char *DBTypFile = malloc(100);
    char *prefix = "restore/meta";
    sprintf(DBClassFile, "%s/%s", prefix, CLASS_BOOT);
    sprintf(DBAttrFile, "%s/%s", prefix, ATTR_BOOT);
    sprintf(DBTypFile, "%s/meta/%s", CUR_DB, TYP_BOOT);
    int tabSize=getLineNum(DBClassFile);
    TABstruct *taboidDrop;
    taboidDrop=bootTabStruct(DBClassFile,0);

    int attroidsize=getLineNum(DBAttrFile);
    harray *attr_harray = NULL;
    attr_harray = bootAttrStruct(DBAttrFile);
    int typoidsize=getLineNum(DBTypFile);
    TYPstruct *typoid = bootTYPStruct(DBTypFile);
    getAttrUltra(attr_harray,typoid,typoidsize,taboidDrop,tabSize);

    free(DBClassFile);
    free(DBAttrFile);
    free(DBTypFile);
    free(typoid);
    harray_free(attr_harray);
    char filename[100];
    sprintf(filename, "%s/%s/%s_%s", "restore","meta","public",TABLE_BOOT);
    FILE *fp = fopen(filename,"w");

    int i;

    for (i=0;i<tabSize;i++){
        char *tmpstr=NULL;

        if ( strlen(taboidDrop[i].attr) > 0 ){
            tmpstr = (char *)malloc( (sizeof(char))*(sizeof( taboidDrop[i].oid)+sizeof(taboidDrop[i].filenode)+
                        sizeof(taboidDrop[i].nsp)+sizeof(taboidDrop[i].tab)+
                        sizeof(taboidDrop[i].attr)+sizeof(taboidDrop[i].typ)+sizeof(taboidDrop[i].toastoid)));
            if(strlen(taboidDrop[i].typ) == 0 && strlen(taboidDrop[i].attmod) == 0){
                strcpy(taboidDrop[i].typ,"NotFound");
                strcpy(taboidDrop[i].attmod,"NotFound");
            }
            sprintf(tmpstr, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                                                                taboidDrop[i].oid,taboidDrop[i].filenode,
                                                                taboidDrop[i].toastoid,taboidDrop[i].toastoid,
                                                                taboidDrop[i].nsp, taboidDrop[i].tab,
                                                                taboidDrop[i].attr,taboidDrop[i].typ,
                                                                taboidDrop[i].nattr,taboidDrop[i].attmod,
                                                                taboidDrop[i].attlen,taboidDrop[i].attalign);
            fputs(tmpstr, fp);
        }
        free(tmpstr);
    }
    fputs("\r", fp);
    fclose(fp);
    free(taboidDrop);
}

/*
 * bootDropContext - Load DROPSCAN table configuration
 * [Enterprise Edition Feature - Stub Implementation]
 */
parray* bootDropContext()
{
    (void)dropscanDir;
    return NULL;
}

/*
 * bootDropContextforISO - Load DROPSCAN configuration for ISO mode
 * [Enterprise Edition Feature - Stub Implementation]
 */
parray* bootDropContextforISO()
{
    return NULL;
}

/**
 * toastBootstrap - Initialize TOAST table metadata
 *
 * @toastmeta: Path to TOAST metadata file
 * @toastnode: TOAST table node identifier
 *
 * Loads TOAST table chunk information for large value reconstruction.
 */
void toastBootstrap(char *toastmeta,char *toastnode)
{
    char taostFilenameFINNAL[MAXPGPATH]={0};
    char metatoastFilename[100];

    snprintf(metatoastFilename, sizeof(metatoastFilename), "%s/%s", toastmeta, toastnode);
    unlink(metatoastFilename);
    for(int hundred=0;hundred<NUM1G;hundred++){
        if ( hundred == 0 ){
            sprintf(taostFilenameFINNAL,"%s/%s",CUR_DBDIR,toastnode);
        }
        else{
            sprintf(taostFilenameFINNAL,"%s/%s.%d",CUR_DBDIR,toastnode,hundred);
        }
        if (access(taostFilenameFINNAL, F_OK) != -1) {
            FILE *toastRelFp = fopen(taostFilenameFINNAL, "rb");
            if (toastRelFp == NULL) {
            } else {
                FILE *metatoastFp = fopen(metatoastFilename, "a");
                if (metatoastFp == NULL) {
                    perror("Failed to create metatoast file");
                    fclose(toastRelFp);
                } else {
                    unsigned int toastRelBlkSize = determinePageDimension(toastRelFp);
                    fseek(toastRelFp, 0, SEEK_SET);
                    int toastIsEmpty = getToastHash(toastRelFp, toastRelBlkSize, metatoastFp, hundred);

                    fclose(toastRelFp);
                    fclose(metatoastFp);

                    if (toastIsEmpty) {
                        if (unlink(metatoastFilename) != 0) {
                            perror("Failed to delete metatoast file");
                        }
                    }
                }
            }
        }
        else{
            break;
        }
    }
}
/**
 * readFromFilenodeOClass - Read filenode mapping from pg_filenode.map
 *
 * @nodefileType: Type of node file
 * @filename:     Path to the mapping file
 * @readType:     Type of read operation
 *
 * Parses PostgreSQL filenode mapping file to get OID-to-filenode mappings.
 *
 * Returns: SUCCESS_RET on success, FAILURE_RET on failure
 */
int readFromFilenodeOClass(char *nodefileType,char *filename,char *readType)
{
    if ( strcmp(readType,"pg_database") ==0
        || strcmp(readType,"pg_class") ==0)
    {
        FILE *fp;
        char charbuf[RELMAPPER_FILESIZE];
        RelMapFile *map;
        RelMapping *mappings;
        RelMapping m;

        fp = fopen(filename, "rb");
        if (!fp)
        {
#ifdef EN
            printf("\nReading Datafile<%s> FAIL, please Checkout pdu.ini  \n",COLOR_ERROR,filename,C_RESET);
#else
            printf("\n%s数据文件 <%s> 读取失败, 请检查 pdu.ini%s  \n",COLOR_ERROR,filename,C_RESET);
#endif

            exit(1);
        }
        int bytesRead;
        int num_loops;
        bytesRead = fread(charbuf,1,RELMAPPER_FILESIZE,fp);
        map = (RelMapFile *) charbuf;

        mappings = map->mappings;

        num_loops = map->num_mappings;
        char *mapfilenodeStr = NULL;
        mapfilenodeStr = (char *)malloc(buffer_size);

        char *mapfilenodeStr1 = NULL;
        mapfilenodeStr1 = (char *)malloc(buffer_size);

        char *mapfilenodeStr2 = NULL;
        mapfilenodeStr2 = (char *)malloc(buffer_size);

        for (int i=0; i < num_loops; i++) {
            m = mappings[i];

            if( strcmp(nodefileType,"g") ==0 ){
                if ( m.mapoid == 1262){
                    sprintf(mapfilenodeStr, "%s%s%d",initDBPath,"global/",m.mapfilenode);
                    pgDatabaseFilex = mapfilenodeStr;
                    return 1;
                }
            }

            else if (strcmp(nodefileType,"b") ==0)
            {
                if ( m.mapoid == 1259){
                    sprintf(mapfilenodeStr, "%s/%d",CUR_DBDIR,m.mapfilenode);
                    pgClassFilex = mapfilenodeStr;
                    pgClassFilenode = (char*)malloc(sizeof(char)*20);
                    sprintf(pgClassFilenode,"%d",m.mapfilenode);
                }
                else if ( m.mapoid == 1249){
                    sprintf(mapfilenodeStr1, "%s/%d",CUR_DBDIR,m.mapfilenode);
                    pgAttrFilex= mapfilenodeStr1;
                    pgAttrFilenode = (char*)malloc(sizeof(char)*20);
                    sprintf(pgAttrFilenode,"%d",m.mapfilenode);
                }
                else if ( m.mapoid == 1247){
                    sprintf(mapfilenodeStr2, "%s/%d",CUR_DBDIR,m.mapfilenode);
                    pgTypeFilex = mapfilenodeStr2;
                }

            }
	    }

    }
    else if( strcmp(readType,"pg_schema") ==0 ){

        char logPathSucc[100];
        sprintf(logPathSucc,"log/boot_%s",CLASS_BOOTTYPE);
        char logPathErr[100];
        sprintf(logPathErr,"log/boot_%s",CLASS_BOOTTYPE);

        readItems(NULL,pgClassFilex,CLASS_ATTR,CLASS_BOOT,CLASS_BOOTTYPE,logPathSucc,logPathErr);

        getSCHFromCLasstxt(CLASS_BOOT);
    }
    return 1;
}

/**
 * getSCHFromCLasstxt - Extract schema info from class metadata
 *
 * @filename: Path to class metadata file
 *
 * Parses class metadata to build schema-table relationship mapping.
 */
void getSCHFromCLasstxt(char *filename){
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        printf("Unable to open file %s\n",filename);
        return;
    }

    int numLines = 0;
    char ch;
    while ((ch = fgetc(file)) != EOF) {
        if (ch == '\n') {
            numLines++;
        }
        if (ch == '\r'){
            break;
        }
    }

    fseek(file, 0, SEEK_SET);

    char a[64];
    char b[64];
    char c[64];
    char d[64];
    char e[64];
    char f[64];
    char *mapfilenodeStr3 = NULL;
    mapfilenodeStr3 = (char *)malloc(buffer_size);
    int i;
    for (i = 0; i < numLines; i++) {
        if (fscanf(file, "%s %s %s %s %s %s", a, b, c, d, e, f) == 6) {
            if(strcmp(b,"pg_namespace") == 0){
                sprintf(mapfilenodeStr3,"%s/%s",CUR_DBDIR,d);
                pgSchemaFilex = mapfilenodeStr3;
                break;
            };
        }
    }

    fclose(file);
}

/**
 * bootstrap_abnormal - Bootstrap metadata in abnormal recovery mode
 *
 * Initializes metadata structures when normal bootstrap is not possible,
 * typically used for data recovery scenarios.
 */
void bootstrap_abnormal(){

    exmode=CSVform;
    setExportMode_decode(CSVform);
    createDir("log");
    char *fileType="g";
    char *globalFilenodePath=NULL;
    globalFilenodePath = (char *)malloc(buffer_size);
    sprintf(globalFilenodePath, "%s%s", initDBPath, "global/pg_filenode.map");
    if(!readFromFilenodeOClass(fileType,globalFilenodePath,"pg_database")){
        printf("\n<%s>initialization failed，please check the reason  \n",globalFilenodePath);
    }

    infoBootstrap(1,pgDatabaseFilex,"","","",0,"",0,"",0);
    char logPathSucc[100];
    sprintf(logPathSucc,"log/boot_%s",DB_BOOTTYPE);
    char logPathErr[100];
    sprintf(logPathErr,"log/boot_%s",DB_BOOTTYPE);

    setlogLevel(readItemLog);
    readItems(NULL,pgDatabaseFilex,DB_ATTR,DB_BOOT,DB_BOOTTYPE,logPathSucc,logPathErr);

    prepareRestore();

    DBstruct *databaseoid = bootDBStruct(DB_BOOT,0);
    int dosize = 0;
    dosize = getLineNum(DB_BOOT);

    loadTbspc(databaseoid,dosize,initDBPath);
    dboidWithTblIntoFile(databaseoid,dosize,DB_BOOT);

    if(dosize == 1){
        #ifdef CN
        printf("无法解析pg_database，请确认pdu版本与数据库版本是否对应\n");
        #else
        printf("can not parse pg_database,please pdu version and db version are corresponded\n");
        #endif
        exit(1);
    }
    int i;
    for ( i = 0; i < dosize; i++ ){
        if ( (strncmp(databaseoid[i].database,"template",8) != 0) &&
             (strcmp(databaseoid[i].database,"restore") != 0) &&
             (strcmp(databaseoid[i].database,"security") != 0)){
            CUR_DBDIR=databaseoid[i].dbpath;
            strcpy(CUR_DB,databaseoid[i].database);
            infoBootstrap(2,"",CUR_DB,"","",0,"",0,"",0);

            removeDir(CUR_DB);
            createDir(CUR_DB);

            char TmpDBClassFile[MiddleAllocSize]="";
            char DBClassFile[MiddleAllocSize]="";
            char DBAttrFile[MiddleAllocSize]="";
            char DBTypFile[MiddleAllocSize]="";
            char DBSchemaFile[MiddleAllocSize]="";

            char meta[100]="";
            sprintf(meta,"%s/%s",CUR_DB,"meta");
            createDir(meta);

            char toastmeta[100]="";
            sprintf(toastmeta,"%s/%s",CUR_DB,"toastmeta");
            createDir(toastmeta);

            char manual[100]="";
            sprintf(manual,"%s/%s",CUR_DB,"manual");
            createDir(manual);

            fileType="b";
            char *baseFilenodePath=NULL;
            baseFilenodePath = (char *)malloc(buffer_size);
            sprintf(baseFilenodePath,"%s/pg_filenode.map",CUR_DBDIR);
            if(!readFromFilenodeOClass(fileType,baseFilenodePath,"pg_class")){
                ("\n<%s>initialization failed，please check the reason  \n",baseFilenodePath);
                exit(1);
            }

            if(!readFromFilenodeOClass(fileType,pgClassFilex,"pg_schema")){
                printf("\n<%s>initialization failed，please check the reason  \n",pgClassFilex);
                exit(1);
            }
            if (pgSchemaFilex == NULL){
                #ifdef EN
                printf("initialization of pgSchemaFile FAIL\n");
                #else
                printf("初始化 pgSchemaFile 失败\n");
                #endif
                exit(1);
            }

            sprintf(TmpDBClassFile, "%s/%s", meta, CLASS_BOOT_FINAL);
            sprintf(DBClassFile, "%s/%s", meta, CLASS_BOOT);
            sprintf(DBAttrFile, "%s/%s", meta, ATTR_BOOT);
            sprintf(DBTypFile, "%s/%s", meta, TYP_BOOT);
            sprintf(DBSchemaFile, "%s/%s", meta, SCHEMA_BOOT);

            printf("\t%s%s\n\t%s\n\t%s\n\t%s%s\n",COLOR_UNLOAD,pgSchemaFilex,pgClassFilex,pgTypeFilex,pgAttrFilex,C_RESET);
            bootMetaInfo2File(pgSchemaFilex,pgClassFilex,pgTypeFilex,pgAttrFilex);

            SCHstruct *schoidTMP;
            schoidTMP=bootSCHStruct(DBSchemaFile);
            int schemalen=getLineNum(DBSchemaFile);
            int t;

            toastTaboid_harray=harray_new(HARRAYTAB);
            tabSize=getLineNum(DBClassFile);
            TABstruct *taboidTMP;
            taboidTMP=bootTabStruct(DBClassFile,0);

            int attroidsize=getLineNum(DBAttrFile);
            harray *attr_harray = NULL;
            attr_harray = bootAttrStruct(DBAttrFile);

            infoBootstrap(3,"","",pgSchemaFilex,pgClassFilex,tabSize,pgAttrFilex,attroidsize,"",0);

            int typoidsize=getLineNum(DBTypFile);
            TYPstruct *typoid = bootTYPStruct(DBTypFile);
            getAttrUltra(attr_harray,typoid,typoidsize,taboidTMP,tabSize);

            int i1=0;
            int j1=0;
            infoBootstrap(4,"","","","",0,"",0,"",0);

            for ( i1 = 0 ; i1 < schemalen ; i1++){
                char *schoid = schoidTMP[i1].oid;
                strcpy(CUR_SCH,schoidTMP[i1].nspname);
                if(schemaInDefaultSHCS(CUR_SCH)){
                    continue;
                }

                char getSchNumCMD[100];
                sprintf(getSchNumCMD,"cat %s |grep %s|wc -l",DBClassFile,schoid);
                char *schtabObj = execCMD(getSchNumCMD);
                int schtabObjNum = atoi(schtabObj)+20;
                TABstruct *schtaboid = (TABstruct *)malloc(schtabObjNum * sizeof(TABstruct));
                if (schtaboid == NULL) {
                    perror("Failed to allocate memory，need to reduceMAX_TAB_OBJvalue");
                    exit(EXIT_FAILURE);
                }

                int k1=0;

                for (j1=0;j1<tabSize;j1++){
                    if ( ( strcmp( schoid,taboidTMP[j1].nsp ) == 0 ) && atoi(taboidTMP[j1].nattr) < MAX_COL_NUM ){
                        if(k1>=MAX_TAB_OBJ){
                            #ifdef CN
                            printf("%s表对象数量超过%d, 请使用专业版PDU%s\n",COLOR_ERROR,MAX_TAB_OBJ,C_RESET);
                            #else
                            printf("%sTable Number exceeds %d ,please use Professional Edition of PDU%s\n",COLOR_ERROR,MAX_TAB_OBJ,C_RESET);
                            #endif
                            exit(1);
                        }
                        strcpy(schtaboid[k1].oid,taboidTMP[j1].oid);
                        strcpy(schtaboid[k1].tab,taboidTMP[j1].tab);
                        strcpy(schtaboid[k1].nsp,taboidTMP[j1].nsp);
                        strcpy(schtaboid[k1].filenode,taboidTMP[j1].filenode);

                        if(strcmp(taboidTMP[j1].toastoid,"0") != 0){
                            unsigned int toast2searchIndex = hash(toastTaboid_harray,taboidTMP[j1].toastoid,toastTaboid_harray->allocated);
                            Node* node = toastTaboid_harray->table[toast2searchIndex];
                            while (node != NULL) {
                                TABstruct* onatoastoid = (TABstruct*)node->data;
                                if(strcmp(onatoastoid->oid,taboidTMP[j1].toastoid) == 0){
                                    strcpy(schtaboid[k1].toastnode,onatoastoid->filenode);
                                    strcpy(schtaboid[k1].toastoid,onatoastoid->oid);
                                }
                                node = node->next;
                            }
                        }
                        else{
                            strcpy(schtaboid[k1].toastnode,taboidTMP[j1].toastoid);
                            strcpy(schtaboid[k1].toastoid,taboidTMP[j1].toastoid);
                        }

                        if (strcmp(schtaboid[k1].toastnode, "0") != 0) {
                            toastBootstrap(toastmeta,schtaboid[k1].toastnode);
                        }

                        strcpy(schtaboid[k1].nattr,taboidTMP[j1].nattr);
                        strcpy(schtaboid[k1].attr,taboidTMP[j1].attr);
                        strcpy(schtaboid[k1].typ,taboidTMP[j1].typ);
                        strcpy(schtaboid[k1].attmod,taboidTMP[j1].attmod);
                        strcpy(schtaboid[k1].attlen,taboidTMP[j1].attlen);
                        strcpy(schtaboid[k1].attalign,taboidTMP[j1].attalign);
                        k1++;
                    }
                }
                flushFinalCLass(schtaboid,k1);
                free(schtaboid);
                schtaboid=NULL;
            }

            free(schoidTMP);
            schoidTMP=NULL;
            free(taboidTMP);
            taboidTMP=NULL;
            harray_free(attr_harray);
            harray_free(toastTaboid_harray);
            free(typoid);
        }
    }
    free(databaseoid);
    free(taboid);
    taboid=NULL;
    databaseoid=NULL;
    removeDir("log");
}

/**
 * bootstrap - Initialize all metadata structures
 *
 * Main bootstrap function that loads all required metadata including
 * databases, tables, schemas, types, and attributes.
 */
void bootstrap(){

    exmode=CSVform;
    setExportMode_decode(CSVform);
    createDir("log");
    sprintf(pgDatabaseFile,"%s%s",initDBPath,pgDatabaseFilenode);

    infoBootstrap(1,pgDatabaseFile,"","","",0,"",0,"",0);

    char logPathSucc[100];
    sprintf(logPathSucc,"log/boot_%s",DB_BOOTTYPE);
    char logPathErr[100];
    sprintf(logPathErr,"log/boot_%s",DB_BOOTTYPE);

    setlogLevel(readItemLog);
    readItems(NULL,pgDatabaseFile,DB_ATTR,DB_BOOT,DB_BOOTTYPE,logPathSucc,logPathErr);

    prepareRestore();

    DBstruct *databaseoid = bootDBStruct(DB_BOOT,0);
    int dosize = 0;
    dosize = getLineNum(DB_BOOT);

    loadTbspc(databaseoid,dosize,initDBPath);
    dboidWithTblIntoFile(databaseoid,dosize,DB_BOOT);

    if(dosize == 1){
        #ifdef CN
        printf("无法解析pg_database，请确认pdu版本与数据库版本是否对应\n");
        #else
        printf("can not parse pg_database,please pdu version and db version are corresponded\n");
        #endif
        exit(1);
    }
    int i;
    for ( i = 0; i < dosize; i++ ){
        if ( (strncmp(databaseoid[i].database,"template",8) != 0) &&
             (strcmp(databaseoid[i].database,"restore") != 0) &&
             (strcmp(databaseoid[i].database,"security") != 0)){
            CUR_DBDIR=databaseoid[i].dbpath;
            strcpy(CUR_DB,databaseoid[i].database);
            infoBootstrap(2,"",CUR_DB,"","",0,"",0,"",0);

            removeDir(CUR_DB);
            createDir(CUR_DB);

            char TmpDBClassFile[MiddleAllocSize]="";
            char DBClassFile[MiddleAllocSize]="";
            char DBAttrFile[MiddleAllocSize]="";
            char DBTypFile[MiddleAllocSize]="";
            char DBSchemaFile[MiddleAllocSize]="";

            char meta[100]="";
            sprintf(meta,"%s/%s",CUR_DB,"meta");
            createDir(meta);

            char toastmeta[100]="";
            sprintf(toastmeta,"%s/%s",CUR_DB,"toastmeta");
            createDir(toastmeta);

            char manual[100]="";
            sprintf(manual,"%s/%s",CUR_DB,"manual");
            createDir(manual);

            sprintf(TmpDBClassFile, "%s/%s", meta, CLASS_BOOT_FINAL);
            sprintf(DBClassFile, "%s/%s", meta, CLASS_BOOT);
            sprintf(DBAttrFile, "%s/%s", meta, ATTR_BOOT);
            sprintf(DBTypFile, "%s/%s", meta, TYP_BOOT);
            sprintf(DBSchemaFile, "%s/%s", meta, SCHEMA_BOOT);

            sprintf(pgClassFile,"%s/%s",CUR_DBDIR,pgClassFilenode);
            sprintf(pgAttrFile,"%s/%s",CUR_DBDIR,pgAttrFilenode);
            sprintf(pgTypeFile,"%s/%s",CUR_DBDIR,pgTypeFilenode);
            sprintf(pgSchemaFile,"%s/%s",CUR_DBDIR,pgSchemaFilendoe);

            bootMetaInfo2File(pgSchemaFile,pgClassFile,pgTypeFile,pgAttrFile);

            SCHstruct *schoidTMP;
            schoidTMP=bootSCHStruct(DBSchemaFile);
            int schemalen=getLineNum(DBSchemaFile);
            int t;

            toastTaboid_harray=harray_new(HARRAYTAB);
            tabSize=getLineNum(DBClassFile);
            TABstruct *taboidTMP;
            taboidTMP=bootTabStruct(DBClassFile,0);

            int attroidsize=getLineNum(DBAttrFile);
            harray *attr_harray = NULL;
            attr_harray = bootAttrStruct(DBAttrFile);

            infoBootstrap(3,"","",pgSchemaFile,pgClassFile,tabSize,pgAttrFile,attroidsize,"",0);

            int typoidsize=getLineNum(DBTypFile);
            TYPstruct *typoid = bootTYPStruct(DBTypFile);
            getAttrUltra(attr_harray,typoid,typoidsize,taboidTMP,tabSize);

            int i1=0;
            int j1=0;
            infoBootstrap(4,"","","","",0,"",0,"",0);

            for ( i1 = 0 ; i1 < schemalen ; i1++){
                char *schoid = schoidTMP[i1].oid;
                strcpy(CUR_SCH,schoidTMP[i1].nspname);
                if(schemaInDefaultSHCS(CUR_SCH)){
                    continue;
                }

                char getSchNumCMD[100];
                sprintf(getSchNumCMD,"cat %s |grep %s|wc -l",DBClassFile,schoid);
                char *schtabObj = execCMD(getSchNumCMD);
                int schtabObjNum = atoi(schtabObj)+20;
                TABstruct *schtaboid = (TABstruct *)malloc(schtabObjNum * sizeof(TABstruct));
                if (schtaboid == NULL) {
                    perror("Failed to allocate memory，need to reduceMAX_TAB_OBJvalue");
                    exit(EXIT_FAILURE);
                }

                int k1=0;

                for (j1=0;j1<tabSize;j1++){
                    if ( ( strcmp( schoid,taboidTMP[j1].nsp ) == 0 ) && atoi(taboidTMP[j1].nattr) < MAX_COL_NUM ){
                        if(k1>=MAX_TAB_OBJ){
                            #ifdef CN
                            printf("%s表对象数量超过%d, 请使用专业版PDU%s\n",COLOR_ERROR,MAX_TAB_OBJ,C_RESET);
                            #else
                            printf("%sTable Number exceeds %d ,please use Professional Edition of PDU%s\n",COLOR_ERROR,MAX_TAB_OBJ,C_RESET);
                            #endif
                            exit(1);
                        }
                        strcpy(schtaboid[k1].oid,taboidTMP[j1].oid);
                        strcpy(schtaboid[k1].tab,taboidTMP[j1].tab);
                        strcpy(schtaboid[k1].nsp,taboidTMP[j1].nsp);
                        strcpy(schtaboid[k1].filenode,taboidTMP[j1].filenode);

                        if(strcmp(taboidTMP[j1].toastoid,"0") != 0){
                            unsigned int toast2searchIndex = hash(toastTaboid_harray,taboidTMP[j1].toastoid,toastTaboid_harray->allocated);
                            Node* node = toastTaboid_harray->table[toast2searchIndex];
                            while (node != NULL) {
                                TABstruct* onatoastoid = (TABstruct*)node->data;
                                if(strcmp(onatoastoid->oid,taboidTMP[j1].toastoid) == 0){
                                    strcpy(schtaboid[k1].toastnode,onatoastoid->filenode);
                                    strcpy(schtaboid[k1].toastoid,onatoastoid->oid);
                                }
                                node = node->next;
                            }
                        }
                        else{
                            strcpy(schtaboid[k1].toastnode,taboidTMP[j1].toastoid);
                            strcpy(schtaboid[k1].toastoid,taboidTMP[j1].toastoid);
                        }

                        if (strcmp(schtaboid[k1].toastnode, "0") != 0) {
                            toastBootstrap(toastmeta,schtaboid[k1].toastnode);
                        }

                        strcpy(schtaboid[k1].nattr,taboidTMP[j1].nattr);
                        strcpy(schtaboid[k1].attr,taboidTMP[j1].attr);
                        strcpy(schtaboid[k1].typ,taboidTMP[j1].typ);
                        strcpy(schtaboid[k1].attmod,taboidTMP[j1].attmod);
                        strcpy(schtaboid[k1].attlen,taboidTMP[j1].attlen);
                        strcpy(schtaboid[k1].attalign,taboidTMP[j1].attalign);
                        k1++;
                    }
                }
                flushFinalCLass(schtaboid,k1);
                free(schtaboid);
                schtaboid=NULL;
            }

            free(schoidTMP);
            schoidTMP=NULL;
            free(taboidTMP);
            taboidTMP=NULL;
            harray_free(attr_harray);
            harray_free(toastTaboid_harray);
            free(typoid);
        }
    }
    free(databaseoid);
    free(taboid);
    taboid=NULL;
    databaseoid=NULL;
    removeDir("log");
}

/**
 * bootMetaInfo2File - Write metadata to output files
 *
 * @pgSchemaFile: Output path for schema metadata
 * @pgClassFile:  Output path for class metadata
 * @pgTypeFile:   Output path for type metadata
 * @pgAttrFile:   Output path for attribute metadata
 *
 * Exports parsed metadata to text files for persistence.
 */
void bootMetaInfo2File(char *pgSchemaFile,char *pgClassFile,char *pgTypeFile,char *pgAttrFile)
{
    int h;
    for (h=0 ; h<4 ; h++){
        char *sourceFile=NULL;
        if(strcmp(b2fPArray[h].sourceFile,"pgSchemaFile") == 0){
            sourceFile=(char *)malloc((strlen(pgSchemaFile) + 1) * sizeof(char));
            if (sourceFile != NULL){
                strcpy(sourceFile,pgSchemaFile);
            }
        }
        else if (strcmp(b2fPArray[h].sourceFile,"pgClassFile") == 0){
            sourceFile=(char *)malloc((strlen(pgClassFile) + 1) * sizeof(char));
            if (sourceFile != NULL){
                strcpy(sourceFile,pgClassFile);
            }
        }
        else if(strcmp(b2fPArray[h].sourceFile,"pgAttrFile") == 0){
            sourceFile=(char *)malloc((strlen(pgAttrFile) + 1) * sizeof(char));
            if (sourceFile != NULL){
                strcpy(sourceFile,pgAttrFile);
            }
        }
        else if(strcmp(b2fPArray[h].sourceFile,"pgTypeFile") == 0){
            sourceFile=(char *)malloc((strlen(pgTypeFile) + 1) * sizeof(char));
            if (sourceFile != NULL){
                strcpy(sourceFile,pgTypeFile);
            }
        }
        if (sourceFile != NULL) {
            boot2File(b2fPArray[h].ATTR_ARRAY,sourceFile,b2fPArray[h].SOME_BOOT,b2fPArray[h].SOME_BOOTTYPE);
            free(sourceFile);
        }
        else{
            perror("Memory allocation failed");
        }
    }
}

/**
 * boot2File - Write boot data to file
 *
 * @ATTR_ARRAY:    Attribute array data
 * @sourceFile:    Source file path
 * @SOME_BOOT:     Boot identifier
 * @SOME_BOOTTYPE: Boot type identifier
 *
 * Writes bootstrap data to specified output file.
 */
void boot2File(char ATTR_ARRAY[],char *sourceFile, char *SOME_BOOT, char *SOME_BOOTTYPE){
    resetArray2Process(attr2Process);
    char DestFile[MiddleAllocSize];
    sprintf(DestFile, "%s/%s/%s", CUR_DB, "meta",SOME_BOOT);

    char logPathSucc[100];
    sprintf(logPathSucc,"log/boot_%s_Succ",SOME_BOOTTYPE);
    char logPathErr[100];
    sprintf(logPathErr,"log/boot_%s_Err",SOME_BOOTTYPE);
    setlogLevel(readItemLog);
    int ret = readItems(NULL,sourceFile,ATTR_ARRAY,DestFile,SOME_BOOTTYPE,logPathSucc,logPathErr);
    if(ret == FAILOPEN_RET){
        #ifdef CN
        printf("文件<%s>不存在,请检查输入的数据库目录\n",sourceFile);
        #else
        printf("File <%s> does not exist,please check out the input database path\n",sourceFile);
        #endif
        exit(1);
    }
    return;
}

/**
 * flushFinalCLass - Write final class metadata to file
 *
 * @taboid:  Table structure array
 * @tabsize: Number of tables
 *
 * Writes finalized class metadata after processing.
 */
void flushFinalCLass(TABstruct *taboid,int tabsize){
    char filename[100];
    sprintf(filename, "%s/%s/%s_%s", CUR_DB,"meta",CUR_SCH,TABLE_BOOT);
    FILE *fp = fopen(filename,"w");

    int i;

    for (i=0;i<tabsize;i++){
        char *nspoid = taboid[i].nsp;
        char *filenode = taboid[i].filenode;
        char *tmpstr=NULL;

        if ( strlen(taboid[i].attr) > 0 ){
            tmpstr = (char *)malloc( (sizeof(char))*(sizeof( taboid[i].oid)+sizeof(taboid[i].filenode)+
                        sizeof(taboid[i].nsp)+sizeof(taboid[i].tab)+
                        sizeof(taboid[i].attr)+sizeof(taboid[i].typ)+sizeof(taboid[i].toastoid)));
            sprintf(tmpstr, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                                                                taboid[i].oid,taboid[i].filenode,
                                                                taboid[i].toastoid,taboid[i].toastnode,
                                                                taboid[i].nsp, taboid[i].tab,
                                                                taboid[i].attr,taboid[i].typ,
                                                                taboid[i].nattr,taboid[i].attmod,
                                                                taboid[i].attlen,taboid[i].attalign);
            fputs(tmpstr, fp);
        }
        else{
            #ifdef CN
            printf("%s表 %s 缺少列信息%s\n", COLOR_ERROR,taboid[i].tab,C_RESET);
            #else
            printf("%sTable %s lacks of column information%s\n", COLOR_ERROR,taboid[i].tab,C_RESET);
            #endif
        }
        free(tmpstr);
    }
    fputs("\r", fp);
    fclose(fp);

    int tabSizeOuput=getLineNum(filename);

    infoBootstrap(5,"","","","",0,"",0,CUR_SCH,tabSizeOuput);
}

/**
 * useDB - Switch to specified database context
 *
 * @former: Command prefix (unused)
 * @latter: Database name to switch to
 *
 * Changes current working database context for subsequent operations.
 */
void useDB(char *former,char *latter){
    int isRightDB=0;
    int i;
    for ( i = 0; i < dosize; i++ ) {
        if ( strcmp(latter,databaseoid[i].database) == 0 ){
            strcpy(CUR_DB,databaseoid[i].database);
            CUR_DBDIR=databaseoid[i].dbpath;
            isRightDB=1;
        };
    }
    if ( !isRightDB ){
        #ifdef EN
        printf("Unknown database %s\n",latter);
        #else
        printf("未知的数据库 %s\n",latter);
        #endif
    }
    else{
        strcpy(CUR_SCH,"public");
        char DBSchemaFile[MiddleAllocSize];
        sprintf(DBSchemaFile, "%s/%s/%s", CUR_DB, "meta",SCHEMA_BOOT);
        schoid=bootSCHStruct(DBSchemaFile);
        if(schoid == NULL){
            strcpy(CUR_DB,"PDU");
            CUR_DBDIR=NULL;
            return;
        }
        schemasize=getLineNum(DBSchemaFile);
        char DBClassFile[MiddleAllocSize];
        sprintf(DBClassFile, "%s/%s/%s_%s", CUR_DB,"meta",CUR_SCH, TABLE_BOOT);
        memset(CURDBFullPath,0,550);
        if(strcmp(CUR_DB,"restore") == 0){
            sprintf(CURDBFullPath,"%s","restore/datafile");
            initCURDBPath(CURDBFullPath);
            isSingleDB = 0;
        }
        else if(strncmp(CUR_DBDIR,"xman",4) == 0){
            CUR_DBDIR=CUR_DBDIR+4;
            sprintf(CURDBFullPath,"%s",CUR_DBDIR);
            initCURDBPath(CURDBFullPath);
            isSingleDB =1;
        }
        else{
            sprintf(CURDBFullPath,"%s",CUR_DBDIR);
            initCURDBPath(CURDBFullPath);
            isSingleDB = 0;
        }
        if(taboid != NULL){
            free(taboid);
            taboid=NULL;
        }

        taboid=bootTabStruct(DBClassFile,1);
        tabSize=getLineNum(DBClassFile);
        tabVol = NULL;
        tabVol = (TABSIZEstruct *)malloc(tabSize * sizeof(TABSIZEstruct));
        getTabSize(tabVol);

        int i;

        infoSchHeader();
        for ( i = 0; i < schemasize; i++ ) {
            if(schemaInDefaultSHCS(schoid[i].nspname)){
                continue;
            }
            sprintf(DBClassFile, "%s/%s/%s_%s", CUR_DB,"meta",schoid[i].nspname, TABLE_BOOT);
            int schTabSize=getLineNum(DBClassFile);
            printf("%s│    %-23s│  %-10d│%s\n",COLOR_SCHEMA,schoid[i].nspname,schTabSize,C_RESET);
        }
        printf("%s└────────────────────────────────────────┘%s\n",COLOR_SCHEMA,C_RESET);
    }

}

/**
 * setSCH - Set current schema context
 *
 * @former: Command prefix (unused)
 * @latter: Schema name to set
 *
 * Changes current working schema context for subsequent operations.
 */
void setSCH(char *former,char *latter){
    if(strcmp(CUR_DB,"PDU") == 0){
        printf("请先用<use>命令选择当前数据库\n");
        return;
    }
    int isRightSCH=0;
    int i;
    for ( i = 0; i < schemasize; i++ ) {
        if ( strcmp(latter,schoid[i].nspname) == 0 ){
            strcpy(CUR_SCH,schoid[i].nspname);
            isRightSCH=1;
        };
    }
    if ( !isRightSCH ){
        ErrorSchNotExist(latter);
    }
    else{
        char DBClassFile[MiddleAllocSize];
        sprintf(DBClassFile, "%s/%s/%s_%s", CUR_DB,"meta",CUR_SCH, TABLE_BOOT);
        if(taboid != NULL){
            free(taboid);
            taboid=NULL;
        }
        taboid=bootTabStruct(DBClassFile,1);
        tabSize=getLineNum(DBClassFile);
        tabVol = NULL;
        tabVol = (TABSIZEstruct *)malloc(tabSize * sizeof(TABSIZEstruct));
        getTabSize(tabVol);

        int i;
        int end;
        if(tabSize <=DISPLAYCount){
            end = tabSize;
        }
        else{
            end = DISPLAYCount ;
        }
        int volIsDisplayed=0;
        infoTabHeader();
        for ( i = 0; i < end; i++ ) {
            volIsDisplayed=1;
            char *ret = (char *)malloc(20*sizeof(char));
            getFormVol(tabVol[i].vol,ret);
            printf("%s│    %-33s│  %-10s│%s\n",COLOR_TABLE,tabVol[i].tab,ret,C_RESET);
        }
        printf("%s└──────────────────────────────────────────────────┘%s\n",COLOR_TABLE,C_RESET);
        #ifdef EN
        if (volIsDisplayed)
            printf("\tOnly display the first %dth tables in order of tableSize\n",end);
        else
            printf("\t All Empty Tables!\n");
        #else
        if (volIsDisplayed)
            printf("\t仅显示表大小排名前 %d 的表名\n",end);
        else
            printf("\t此模式下都是空表!\n");
        #endif
    }
}

/**
 * tabVolSort - Compare function for table size sorting
 *
 * @a: First table size structure
 * @b: Second table size structure
 *
 * Comparison function for qsort to order tables by size descending.
 *
 * Returns: Negative if a > b, positive if a < b, zero if equal
 */
int tabVolSort(const void *a, const void *b) {
    TABSIZEstruct *structA = (TABSIZEstruct *)a;
    TABSIZEstruct *structB = (TABSIZEstruct *)b;

    long volA = atol(structA->vol);
    long volB = atol(structB->vol);

    if (volB < volA) return -1;
    if (volB > volA) return 1;
    return 0;
}

/**
 * getTabSize - Calculate and populate table sizes
 *
 * @tabVol: Array of table size structures to populate
 *
 * Calculates actual disk size for each table in the current schema.
 */
void getTabSize(TABSIZEstruct *tabVol){
    int i;
    int j;

    for( i = 0 ;i< tabSize ; i++ ){

        uint64 totalSize = 0;
        char tabPath[600]="";
        char tabToastPath[600]="";
        for( j = 0; j < 500; j++){
            if (j == 0){
                sprintf(tabPath,"%s/%s",CURDBFullPath,taboid[i].filenode);
            }
            else{
                sprintf(tabPath,"%s/%s.%d",CURDBFullPath,taboid[i].filenode,j);
            }
            struct stat file_stat;
            if (stat(tabPath, &file_stat) != -1) {
                totalSize = totalSize + file_stat.st_size;
            }
            else{
                break;
            }
        }
        for( j = 0; j < 500; j++){
            if (j == 0){
                sprintf(tabToastPath,"%s/%s",CURDBFullPath,taboid[i].toastoid);
            }
            else{
                sprintf(tabToastPath,"%s/%s.%d",CURDBFullPath,taboid[i].toastoid,j);
            }
            struct stat file_stat;
            if (stat(tabToastPath, &file_stat) != -1) {
                totalSize = totalSize + file_stat.st_size;
            }
            else{
                break;
            }
        }

        sprintf(tabVol[i].vol,"%llu",totalSize);
        sprintf(tabVol[i].tab,"%s",taboid[i].tab);
    }
    qsort(tabVol, tabSize, sizeof(TABSIZEstruct), tabVolSort);
}

/**
 * getFormVol - Format volume size for display
 *
 * @vol: Raw volume value string
 * @ret: Output buffer for formatted string
 *
 * Converts raw byte count to human-readable format (KB, MB, GB).
 */
void getFormVol(char *vol,char *ret){
    if(strcmp(vol,"0") == 0){
        sprintf(ret,"0");
        return;
    }
    double numvol = atof(vol);
    double value;
    value = numvol / (1024*1024*1024);
    if (value >= 1 && value <= 1024) {
        sprintf(ret,"%.2f GB",value);
        return;
    }

    value = numvol / (1024*1024);
    if (value >= 1 && value <= 1024) {
        sprintf(ret,"%.2f MB",value);
        return;
    }

    value = numvol / 1024;
    if (value >= 1 && value <= 1024) {
        sprintf(ret,"%.2f KB",value);
        return;
    }

    sprintf(ret,"%lu Bytes",numvol);
    return;
}

/**
 * SHOW - Display database objects
 *
 * @former: Object type to show (databases, schemas, tables, etc.)
 *
 * Lists database objects based on the specified type.
 */
void SHOW(char *former){
    if ( strcmp(former,"\\l") == 0){
        infoDBHeader();
        int i;
        for ( i = 0; i < dosize; i++ ) {
            if(strcmp(databaseoid[i].database,"restore") == 0)
                printf("%s│    %s(PDU)   │%s\n",COLOR_DB,databaseoid[i].database,C_RESET);
            else{
                printf("%s│    %-12s   │%s\n",COLOR_DB,databaseoid[i].database,C_RESET);
            }
        }
        printf("%s└───────────────────┘%s\n\n",COLOR_DB,C_RESET);
    }
    else if( strcmp(former,"\\dt") == 0 ){
        int volIsDisplayed=0;
        if ( !taboid ){
            warningUseDBFirst();
            return;
        }
        else{
            infoTabHeader();
            int i;
            int end;
            int dspcnt = 0;
            if(tabSize <=DISPLAYCount){
                end = tabSize;
            }
            else{
                end = DISPLAYCount ;
            }
            for ( i = 0; i < tabSize; i++ ) {
                volIsDisplayed=1;
                char *ret = (char *)malloc(20*sizeof(char));
                getFormVol(tabVol[i].vol,ret);
                printf("%s│    %-33s│  %-10s│%s\n",COLOR_TABLE,tabVol[i].tab,ret,C_RESET);
                dspcnt++;

            }
            printf("%s└──────────────────────────────────────────────────┘%s\n\n",COLOR_TABLE,C_RESET);

            #ifdef EN
            if (volIsDisplayed)
                printf("\t%d tables in total\n",dspcnt);
            else
                printf("\t All Empty Tables!\n");
            #else
            if (volIsDisplayed)
                printf("\t共计 %d 张表\n",dspcnt);
            else
                printf("\t此模式下都是空表!\n");
            #endif
        }
    }
    else if ( strcmp(former,"\\dn") == 0 ){
        int nSchDisplayed=0;
        if ( !schoid ){
            warningUseDBFirst();

            return;
        }
        else{
            char DBClassFile[MiddleAllocSize];
            sprintf(DBClassFile, "%s/%s/%s_%s", CUR_DB,"meta",CUR_SCH, TABLE_BOOT);
            int i;

            infoSchHeader();
            for ( i = 0; i < schemasize; i++ ) {
                if(schemaInDefaultSHCS(schoid[i].nspname)){
                    continue;
                }
                sprintf(DBClassFile, "%s/%s/%s_%s", CUR_DB,"meta",schoid[i].nspname, TABLE_BOOT);
                int schTabSize=getLineNum(DBClassFile);
                printf("%s│    %-23s│  %-10d│%s\n",COLOR_SCHEMA,schoid[i].nspname,schTabSize,C_RESET);
                nSchDisplayed++;
            }
            printf("%s└────────────────────────────────────────┘%s\n",COLOR_SCHEMA,C_RESET);
        }
            printf("\n");
            printf("%s%d rows selected\n",space,nSchDisplayed);

    }
    else{
        printUsage();
    }

}

/**
 * DESC - Describe table structure
 *
 * @former: Command modifier
 * @latter: Table name to describe
 *
 * Displays detailed column information for specified table.
 */
void DESC(char *former,char *latter){
    if ( !taboid ){
        warningUseDBFirst();

        return;
    }
    else if (latter == NULL){
        ErrorTabNotExist(latter);
    }
    else
    {
        int Dmatched=0;
        int i;
        for ( i = 0; i < tabSize; i++ ) {
            if( strcmp( taboid[i].tab,latter) == 0 ){
                char attr[10240]="";
                strcpy(attr,taboid[i].attr);
                char typ[10240]="";
                strcpy(typ,taboid[i].typ);
                char attmod[10240]="";
                strcpy(attmod,taboid[i].attmod);
                char descTyp[5];
                strcpy(descTyp,former);

                if(strcmp(descTyp,"\\d+") == 0){
                    char *ddltabname = quotedIfUpper(taboid[i].tab);

                    infoDescHeader(1);
                    printf("   CREATE TABLE %s (\n",ddltabname);
                    getAttrTypForm(attr,typ,attmod);
                    printf("   );\n");
                    printf("%s┌──────────────────────────────────────────────────────────────┐%s\n",COLOR_TABLE,C_RESET);
                    printf("%s│                                                              │%s\n",COLOR_TABLE,C_RESET);
                    printf("%s└──────────────────────────────────────────────────────────────┘%s\n",COLOR_TABLE,C_RESET);

                }
                else if(strcmp(descTyp,"\\d") == 0){
                    char res[10240]="";
                    getAttrTypSimple(res,typ);
                    infoDescHeader(0);
                    printf("%s\n",res);
                }
                Dmatched=1;
            }
        }

        if ( !Dmatched ){
            ErrorTabNotExist(latter);
        }
    }

}

#if defined(__linux__)
/**
 * allocate_start_end_to_parray - Load WAL file list from directory
 *
 * @directory1: Path to WAL directory
 * @array:      Output array for WAL file names
 *
 * Scans directory for WAL files and populates array.
 *
 * Returns: Number of WAL files found
 */
int allocate_start_end_to_parray(char *directory1,WALFILE *array) {
    DIR *dir1;
    struct dirent *entry1;
    int arraySize=0;
    dir1 = opendir(directory1);
    if (dir1 == NULL) {
        ErrorArchPathNotExist(directory1);
        exit(1);
    }
    else{
        while ((entry1 = readdir(dir1)) != NULL) {

            if (entry1->d_type == 8) {
                char fullPath[1024];
                struct stat file_stat;

                sprintf(fullPath,"%s/%s",directory1,entry1->d_name);
                if (stat(fullPath, &file_stat) == -1) {
                    perror("FAIL to get file status");
                    continue;
                }

                if (file_stat.st_size > 1048576 && IsXLogFileName(entry1->d_name)){
                    strcpy(array[arraySize].walnames,entry1->d_name);
                    arraySize++;
                }
            }
        }
    }

    qsort(array,arraySize,sizeof(WALFILE), compare_walfile);

    closedir(dir1);
    return arraySize;
}
#elif defined(_WIN32)
/**
 * allocate_start_end_to_parray - Load WAL file list from directory
 *
 * @directory1: Path to WAL directory
 * @array:      Output array for WAL file names
 *
 * Scans directory for WAL files and populates array.
 *
 * Returns: Number of WAL files found
 */
int allocate_start_end_to_parray(char *directory1, WALFILE *array) {
    WIN32_FIND_DATA findData;
    HANDLE hFind = INVALID_HANDLE_VALUE;
    int arraySize = 0;

    char searchPath[1024];
    sprintf(searchPath, "%s\\*", directory1);

    hFind = FindFirstFile(searchPath, &findData);
    if (hFind == INVALID_HANDLE_VALUE) {
        ErrorArchPathNotExist(directory1);
        exit(1);
    } else {
        do {
            if (!(findData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
                char fullPath[1024];
                BY_HANDLE_FILE_INFORMATION file_info;

                sprintf(fullPath, "%s\\%s", directory1, findData.cFileName);

                HANDLE hFile = CreateFile(fullPath, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
                if (hFile == INVALID_HANDLE_VALUE) {
                    perror("FAIL to get file status");
                    continue;
                }

                if (!GetFileInformationByHandle(hFile, &file_info)) {
                    CloseHandle(hFile);
                    perror("FAIL to get file status");
                    continue;
                }

                CloseHandle(hFile);

                ULARGE_INTEGER fileSize;
                fileSize.HighPart = file_info.nFileSizeHigh;
                fileSize.LowPart = file_info.nFileSizeLow;

                if (fileSize.QuadPart > 10485760 && IsXLogFileName(findData.cFileName)) {
                    strcpy(array[arraySize].walnames, findData.cFileName);
                    arraySize++;
                }
            }
        } while (FindNextFile(hFind, &findData) != 0);
    }

    FindClose(hFind);

    qsort(array, arraySize, sizeof(WALFILE), compare_walfile);

    return arraySize;
}
#endif

/**
 * initWalScan - Initialize WAL scanning parameters
 *
 * @flag:                Scan mode flag
 * @archDirFiles_array:  Archive directory file list
 * @walDirFiles_array:   WAL directory file list
 *
 * Sets up WAL file lists for transaction scanning.
 */
void initWalScan(int flag,WALFILE *archDirFiles_array,WALFILE *walDirFiles_array){

    nWal = 0;
    TxXman_harray = NULL;
    TxSaved_paaray = NULL;

    TxXman_harray = harray_new(HARRAYDEL);
    TxSaved_paaray = parray_new();

    if(flag == SCANINIT){
        GetTxRetAll = parray_new();
    }
    sprintf(archivedir,"%s",initArchPath);
    sprintf(waldatadir,"%s%s",initDBPath,"pg_wal");

    archWaldirNum = allocate_start_end_to_parray(archivedir,archDirFiles_array);
    pgwalWaldirNum = allocate_start_end_to_parray(waldatadir,walDirFiles_array);

}

/**
 * printLsnT - Print LSN timing information
 *
 * Outputs LSN and timing data for debugging purposes.
 */
void printLsnT()
{
    int elemNumber = parray_num(GetTxRetAll);
    if(elemNumber != 0){
        DELstruct *elem = parray_get(GetTxRetAll,0);

        char ret1[60];
        char ret2[60];

        sprintf(ret1,"         %s",elem->startLSNforTOAST);
        sprintf(ret2,"         %s",elem->endLSNforTOAST);

        printfParam("startlsnt",ret1);
        printfParam("endlsnt",ret2);
    }
    else{
        printfParam("startlsnt"," ");
        printfParam("endlsnt"," ");
    }
}

/**
 * execGetTx - Execute transaction retrieval from WAL
 *
 * @GetTxRetAll:    Output array for transaction results
 * @archDirFiles:   Archive directory files
 * @walDirFiles:    WAL directory files
 * @flag:           Operation flag
 *
 * Scans WAL files to extract transaction information.
 *
 * Returns: Number of transactions found
 */
int execGetTx(parray *GetTxRetAll,WALFILE *archDirFiles,WALFILE *walDirFiles,int flag,
              char datafile[50],char oldDatafile[50],
              char toastfile[50],char oldToastfile[50],
              char tabname[50],char typ[10240],
              TransactionId txRequested,TABstruct *taboid){
    char *datadir=NULL;
    parray *TxForRestore=NULL;
    TxForRestore = parray_new();
    char *startwal;

    char start_walfilename[100];
    char end_walfilename[100];
    memset(start_walfilename,0,100);
    memset(end_walfilename,0,100);

    char start_archfilename[100];
    char end_archfilename[100];
    memset(start_archfilename,0,100);
    memset(end_archfilename,0,100);

    char datafile2rd[50];
    char oldDatafile2rd[50];
    char toastfile2rd[50];
    strcpy(datafile2rd,datafile);
    strcpy(oldDatafile2rd,oldDatafile);
    strcpy(toastfile2rd,toastfile);

    parray *GetTxRetFromWal=NULL;
    parray *GetTxRetFromArch=NULL;

    if(strlen(manualSrtWal) > 0 && strlen(manualEndWal) == 0){
        if(IsXLogFileName(manualSrtWal)){
            strcpy(start_archfilename,manualSrtWal);
            strcpy(end_archfilename,archDirFiles[archWaldirNum-1].walnames);
        }
        else{
            ErrorWalname();
            return FAILURE_RET;
        }
    }
    else if(strlen(manualSrtWal) == 0 && strlen(manualEndWal) > 0){
        if(IsXLogFileName(manualEndWal)){
            strcpy(end_archfilename,manualEndWal);
            strcpy(start_archfilename,archDirFiles[0].walnames);
        }
        else{
            ErrorWalname();
            return FAILURE_RET;
        }
    }
    else if(strlen(manualSrtWal) > 0 && strlen(manualEndWal) > 0){
        if(IsXLogFileName(manualEndWal) && IsXLogFileName(manualSrtWal)){
            strcpy(start_archfilename,manualSrtWal);
            strcpy(end_archfilename,manualEndWal);
        }
        else{
            ErrorWalname();
            return FAILURE_RET;
        }
    }

    else{
        strcpy(start_archfilename,archDirFiles[0].walnames);
        strcpy(end_archfilename,archDirFiles[archWaldirNum-1].walnames);
    }

    if(!IsXLogFileName(start_archfilename) || !IsXLogFileName(end_archfilename)){
        ErrorArchivePath(initArchPath);
        exit(1);
    }

    infoWalRange(start_archfilename,end_archfilename);

    if(flag == DELRESTORE && restoreMode == periodRestore){
        for (int u=0;u<parray_num(GetTxRetAll);u++){
            DELstruct *elem=parray_get(GetTxRetAll,u);
            startwal = elem->startwal;
            parray_append(TxForRestore,elem);
        }
    }
    else if(flag == DELRESTORE && restoreMode == TxRestore){
        int matched=0;
        for (int u=0;u<parray_num(GetTxRetAll);u++){
            DELstruct *elem=parray_get(GetTxRetAll,u);
            if(elem->tx == txRequested){
                startwal = elem->startwal;
                parray_append(TxForRestore,elem);
                matched=1;
            }
        }
        if (!matched){
            #ifdef CN
            printf("不存在的事务号<%d>\n",txRequested);
            #else
            printf("unknown Tx number<%d>\n",txRequested);
            #endif
            return 0;
        }
    }
    else if (flag == DEL && restoreMode == TxRestore){
        #ifdef CN
        char *item="▌ 事务恢复模式 [按事务号分组显示]";
        #else
        char *item="▌ Tx Restore Mode [Displayed by Tx groups]";
        #endif
        infoRestoreMode(item);
    }
    else if (flag == DEL && restoreMode == periodRestore){
        #ifdef CN
        char *item="▌ 时间区间恢复模式 [按时间区间内全部显示]";
        #else
        char *item="▌ Time Range Restore Mode [Displayed all within Time Range]";
        #endif
        infoRestoreMode(item);
    }

    int walDiff=countFilesBetween(start_archfilename,end_archfilename);
    if(walDiff+2 > 250){
        #ifdef CN
        printf("%s当前的WAL文件区间的文件个数为%d,超过250个 ,建议设置参数startwal与endwal ,本次不执行%s\n",COLOR_WARNING,walDiff+2,C_RESET);
        #else
        printf("%sWAL file number in current file range is %d, which is over 250, parameter <startwal> and <endwal> are recommended ,quit executing this time%s\n",COLOR_WARNING,walDiff,C_RESET);
        #endif
        return FAILURE_RET;
    }
    strcpy(start_walfilename,walDirFiles[0].walnames);
    strcpy(end_walfilename,walDirFiles[pgwalWaldirNum-1].walnames);

    if(flag == DELRESTORE){
        int startWalDiff=countFilesBetween(start_archfilename,startwal);

        if(startWalDiff >5){
            char choice;
            int c;
            while (1) {
                #ifdef CN
                printf("%s当前startwal设置和建议startwal之间差距较大\n是否确认执行？y/n  %s",COLOR_VERSION,C_RESET);
                #else
                printf("%sCurrent startwal setting has significant deviation from recommended value\ncausing degraded recovery efficiency. Confirm execution? (y/n)  %s", COLOR_VERSION, C_RESET);
                #endif
                if (scanf(" %c", &choice) != 1) {
                    while ((c = getchar()) != '\n' && c != EOF);
                    continue;
                }
                while ((c = getchar()) != '\n' && c != EOF);
                if (choice == 'y' || choice == 'Y') {
                    break;
                } else if (choice == 'n' || choice == 'N') {
                    return FAILURE_RET;
                }
            }
        }
        else if(startWalDiff == -1 && strcmp(start_archfilename,startwal) != 0){
            printf("%s当前的ParameterstartwalValue小于建议startwal，Recovery必定failed，退出本次Recovery，请重设startwal%s\n",COLOR_ERROR,C_RESET);
            return FAILURE_RET;
        }
    }

    resetArray2Process(attr2Process);
    if(strcmp(typ,"xman") != 0){
        char *attr2DecodeTMP = (char *)malloc((strlen(typ)+1)*sizeof(char));
        strcpy(attr2DecodeTMP,typ);
        int nAttr=0;
        char *attrChars[MAX_COL_NUM];
        for (int i = 0; i < MAX_COL_NUM; i++) {
            attrChars[i] = (char *)malloc(20);
        }
        char temp[50];
        char *token = strtok(attr2DecodeTMP, ",");
        while (token != NULL) {
            if (nAttr >= 1024) {
                printf("ExceededattrCharsarray capacity\n");
            }
            strncpy(temp, token, sizeof(temp) - 1);
            strcpy(attrChars[nAttr],temp);
            nAttr++;
            token = strtok(NULL, ",");
        }

        int a;
        for (a=0;a<nAttr;a++){
            char ret[100];
            memset(ret,0,100);
            getStdTyp(attrChars[a],ret);
            if(!AddList2Prcess(attr2Process,ret,TABLE_BOOTTYPE)){
                return FAILURE_RET;
            }
        }
        for (int i = 0; i < MAX_COL_NUM; i++) {
            free(attrChars[i]);
        }
    }

    if(flag == DELRESTORE){
        pgGetTxforArch(&GetTxRetFromArch,SrtTime,EndTime,archDirFiles,archWaldirNum,
                start_archfilename,end_archfilename,archivedir,
                flag,"0","0",toastfile,oldToastfile,tabname,
                TxForRestore,1,attr2Process,taboid);
        char DBDIRcopy[MAXPGPATH]={0};
        strcpy(DBDIRcopy,CUR_DBDIR);
        strcpy(CUR_DBDIR,"restore/datafile");
        toastBootstrap("restore/toastmeta",taboid->toastnode);
        strcpy(CUR_DBDIR,DBDIRcopy);
        int toastInitRet = initToastHash("restore",taboid->toastnode);
        initToastId(taboid->toastnode);
        setToastHash(toastHash);
    }
    pgGetTxforArch(&GetTxRetFromArch,SrtTime,EndTime,archDirFiles,archWaldirNum,
            start_archfilename,end_archfilename,archivedir,
            flag,datafile,oldDatafile,toastfile,oldToastfile,tabname,
            TxForRestore,1,attr2Process,taboid);

    if(GetTxRetFromArch != NULL && parray_num(GetTxRetFromArch) > 0 && flag != DELRESTORE){
        int x;
        for (x = 0; x < parray_num(GetTxRetFromArch); x++) {
            if(flag == DEL){
                DELstruct *elem = parray_get(GetTxRetFromArch,x);
                parray_append(GetTxRetAll,elem);
            }
            else{
                TRUNCstruct *elem = parray_get(GetTxRetFromArch,x);
                parray_append(GetTxRetAll,elem);
            }
        }
    }
    if(flag != DELRESTORE)
        infoTimeRange((char *)timestamptz_to_str_og(*SrtTime),(char *)timestamptz_to_str_og(*EndTime));

    if (GetTxRetFromArch != NULL) {
        parray_free(GetTxRetFromArch);
    }
    if (GetTxRetFromWal != NULL) {
        parray_free(GetTxRetFromWal);
    }

    return SUCCESS_RET;
}

/**
 * SCAN - Scan for deleted/truncated table records
 *
 * @former: Scan type (table name or 'manual')
 * @latter: Additional scan parameter
 *
 * Scans WAL logs to find DELETE/TRUNCATE operations for data recovery.
 */
void SCAN(char *former,char *latter)
{
    WALFILE *walDirFiles_array = (WALFILE*)malloc(sizeof(WALFILE)*2048);
    WALFILE *archDirFiles_array = (WALFILE*)malloc(sizeof(WALFILE)*2048);

    initWalScan(SCANINIT,archDirFiles_array,walDirFiles_array);
    unloadTimer("start");
    if ( !taboid ){
        warningUseDBFirst();

        return;
    }
    isDropScanned = 0;
    isDelScanned = 0;

    int matched = 0;
    int i;
    int alreadyWritten = 0;
    if( strcmp(latter,"drop")==0 ){
        if(strcmp(CUR_DB,"restore") == 0 || strcmp(CUR_DB,"PDU") == 0){
            #ifdef CN
            printf("请在需要恢复表的所在库下进行scan操作\n");
            #else
            printf("you must scan under the corresponding database\n");
            #endif
            return;
        }

        #ifdef EN
        printf("\n%sScanning %s%s%s %sRecords ...%s\n",C_WHITE2,COLOR_ERROR,"DROP/TRUNCATE",C_RESET,C_WHITE2,C_RESET);
        #else
        printf("\n%s正在扫描%s%s%s%s记录...%s\n",C_WHITE2,COLOR_ERROR,"DROP/TRUNCATE",C_RESET,C_WHITE2,C_RESET);
        #endif

        if(!execGetTx(GetTxRetAll,archDirFiles_array,walDirFiles_array,DROP,"0","0","0","0","tabname","xman",0,NULL)){
            return;
        }
        free(walDirFiles_array);
        free(archDirFiles_array);
        walDirFiles_array = NULL;
        archDirFiles_array = NULL;

        bootforDropScan(CUR_DB);

        if(parray_num(GetTxRetAll) != 0){

            for(int h=0;h < parray_num(GetTxRetAll);h++){
                dropElem *elem = parray_get(GetTxRetAll,h);
                infoScanDropResult(elem);
            }
            isDropScanned = 1;
        }
        else{
            #ifdef CN
            printf("%s现有WAL日志中未检测到drop/truncate事务。%s\n",COLOR_WARNING,C_RESET);
            #else
            printf("%sNO drop/truncate transaction detected from the given wal files.%s\n",COLOR_WARNING,C_RESET);
            #endif
        }
    }
    else if( strcmp(latter,"manual")==0 ){
        char sqlPath[500];
        sprintf(sqlPath,"%s/manual",CUR_DB);
        parray *filenamesFromManual=getfilenameParray(sqlPath);
        if(parray_num(filenamesFromManual) == 0){
            #ifdef CN
            printf("<%s/manual>路径下未找到需要扫描的文件\n",CUR_DB);
            #else
            printf("No file found in <%s/manual>\n",CUR_DB);
            #endif
            return;
        }

        for(int i=0;i<parray_num(filenamesFromManual);i++){
            char fullsqlPath[600];
            char *file = parray_get(filenamesFromManual,i);
            sprintf(fullsqlPath,"%s/%s",sqlPath,file);
            getMetaFromManul(fullsqlPath,CUR_DB);
        }

    }
    else{

        for ( i = 0; i < tabSize; i++ ) {
            if( strcmp( taboid[i].tab,latter) == 0 ){
                char pgFilePath[1024]="";
                sprintf(pgFilePath, "%s/%s", CUR_DBDIR,taboid[i].filenode);
                #ifdef EN
                printf("\n%sScanning %s%s%s %sRecords for table<%s>...%s\n\n",C_WHITE2,COLOR_ERROR,resStr,C_RESET,C_WHITE2,taboid[i].tab,C_RESET);
                #else
                printf("\n%s正在扫描表<%s>的%s%s%s记录...%s\n\n",C_WHITE2,taboid[i].tab,COLOR_ERROR,resStr,C_RESET,C_RESET);
                #endif

                if(! execGetTx(GetTxRetAll,archDirFiles_array,walDirFiles_array,DEL,taboid[i].filenode,taboid[i].oid,taboid[i].toastnode,taboid[i].toastoid,taboid[i].tab,"xman",0,&taboid[i])){
                    return;
                }
                free(walDirFiles_array);
                free(archDirFiles_array);
                walDirFiles_array = NULL;
                archDirFiles_array = NULL;

/*--------------------------------------------------------------------------
| 时间戳：2000-01-01 08:00:00.000000 CST | Transaction号：15698 | 待Recoveryrecords数：1012 |
--------------------------------------------------------------------------
--------------------------------------------------------------------------
| 时间戳：2000-01-01 08:00:00.000000 CST | Transaction号：15698 | 待Recoveryrecords数：43477 |
--------------------------------------------------------------------------
--------------------------------------------------------------------------
| 时间戳：2000-01-01 08:00:00.000000 CST | Transaction号：15698 | 待Recoveryrecords数：43267 |
--------------------------------------------------------------------------
--------------------------------------------------------------------------
| 时间戳：2000-01-01 08:00:00.000000 CST | Transaction号：15698 | 待Recoveryrecords数：43475 |
--------------------------------------------------------------------------
--------------------------------------------------------------------------
| 时间戳：2025-02-24 16:54:43.196302 CST | Transaction号：15698 | 待Recoveryrecords数：36048 |
--------------------------------------------------------------------------*/

                if(parray_num(GetTxRetAll) > 1){
                    for (int x = 0; x < parray_num(GetTxRetAll); x++) {
                        DELstruct *elem = parray_get(GetTxRetAll,x);
                        if(elem->txtime != 0 && parray_num(GetTxRetAll) > 1){
                            for (int f = 0; f < parray_num(GetTxRetAll); f++){
                                DELstruct *elemInner = parray_get(GetTxRetAll,f);
                                if(elem->tx == elemInner->tx && elem->txtime != elemInner->txtime){
                                    elem->delCount = elem->delCount+elemInner->delCount;
                                }
                            }
                        }
                    }
                }

                while(! ifTxArrayAllWithTime(GetTxRetAll) ){
                    cleanNoTimeTxArray(GetTxRetAll);
                }

                if(restoreMode == TxRestore){
                    *SrtTime=0;
                    *EndTime=0;
                }

                if(parray_num(GetTxRetAll) == 0){
                    #ifdef CN
                    printf("\n%s现有wal日志中未发现表<%s>的%s记录%s\n\n",COLOR_WARNING,taboid[i].tab,resStr,C_RESET);
                    #else
                    printf("\n%sNO %s records detected for table <%s> from the given wal files.%s\n\n",COLOR_WARNING,resStr,taboid[i].tab,C_RESET);
                    #endif
                    return;
                }

                if(restoreMode == TxRestore){
                    for (int j = 0; j < parray_num(GetTxRetAll); j++) {
                        DELstruct *elem = parray_get(GetTxRetAll,j);
                        strcpy(elem->tabname,taboid[i].tab);
                        strcpy(elem->datafile,taboid[i].filenode);
                        strcpy(elem->oldDatafile,taboid[i].oid);
                        strcpy(elem->toast,taboid[i].toastnode);
                        strcpy(elem->oldToast,taboid[i].toastoid);
                        strcpy(elem->typ,taboid[i].typ);
                        elem->taboid = &taboid[i];
                        infoTxScanResult(elem,resStr);
                    }
                    isTxScanned = 1;
                    isPeriodScanned = 0;
                }
                else if (restoreMode == periodRestore){
                    DELstruct *elem = parray_get(GetTxRetAll,0);
                    strcpy(elem->tabname,taboid[i].tab);
                    strcpy(elem->datafile,taboid[i].filenode);
                    strcpy(elem->oldDatafile,taboid[i].oid);
                    strcpy(elem->toast,taboid[i].toastnode);
                    strcpy(elem->oldToast,taboid[i].toastoid);
                    strcpy(elem->typ,taboid[i].typ);
                    elem->taboid = &taboid[i];
                    infoTimeScanResult(elem,resStr,SrtTime,EndTime);
                    isTxScanned = 0;
                    isPeriodScanned = 1;
                }
                InfoStartwalMeaning();
                isDelScanned = 1;
                matched = 1;
                break;
            }
        }
        if(matched != 1){
            ErrorTabNotExist(latter);
        }
    }
    unloadTimer("end");
}

/**
 * RESTORE - Restore deleted table data
 *
 * @former: Restore mode
 * @latter: Target specification
 * @third:  Additional parameter
 * @fourth: Additional parameter
 *
 * Recovers deleted data from WAL logs and writes to output files.
 */
void RESTORE(char *former,char *latter,char *third,char *fourth)
{
    if ( !taboid && strcmp(latter,"db") != 0){
        warningUseDBFirst();
        return;
    }

    if ( strcmp("restore",CUR_DB) == 0){
        #ifdef CN
        printf("请在需要恢复表的对应库下进行restore操作\n");
        #else
        printf("you can only execute <restore> under the corresponding database\n");
        #endif
        return;
    }

    char logPathSucc[100];
    char logPathErr[100];
    createDir("log");
    sprintf(logPathSucc,"log/%s_%s_%s_%s_%s",CUR_DB,CUR_SCH,"restore",latter,"succ.txt");
    sprintf(logPathErr,"log/%s_%s_%s_%s_%s",CUR_DB,CUR_SCH,"restore",latter,"err.txt");
    FILE *logSucc=fopen(logPathSucc,"w");
    FILE *logErr=fopen(logPathErr,"w");

    char *txRequested = (char*)malloc(sizeof(char)*sizeof(latter));
    strcpy(txRequested,third);
    trim_char(txRequested,' ');

    if(TxSaved_paaray == NULL && strcmp(latter,"tab") != 0 && strcmp(latter,"db") != 0){
        #ifdef CN
        printf("%s请先执行SCAN操作%s\n",COLOR_WARNING,C_RESET);
        #else
        printf("%splease SCAN first%s\n",COLOR_WARNING,C_RESET);
        #endif
        return;
    }
    unloadTimer("start");
    if(isDropScanned == 0 && strcmp(latter,"db") != 0){
        if(restoreMode == TxRestore && isTxScanned == 0){
            #ifdef CN
            printf("%s时间区间恢复模式下的扫描结果无法用于事务号恢复，请重新扫描%s\n",COLOR_WARNING,C_RESET);
            #else
            printf("%sScan result under Period restore mode can not be restore under Tx restore mode; please SCAN again%s\n",COLOR_WARNING,C_RESET);
            #endif
            return;
        }
        else if(restoreMode == periodRestore && isPeriodScanned == 0){
            #ifdef CN
            printf("%s事务号恢复模式下的扫描结果无法用于时间区间恢复，请重新扫描%s\n",COLOR_WARNING,C_RESET);
            #else
            printf("%sScan result under Tx restore mode can not be restore under Period restore mode; please SCAN again%s\n",COLOR_WARNING,C_RESET);
            #endif
            return;
        }
    }

    if(strcmp(latter,"del") == 0 || strcmp(latter,"upd") == 0){

        if(strcmp(latter,"upd") == 0 && resTyp == DELETEtyp){
            ErrorUpdDelWrong(1);
            return;
        }
        else if(strcmp(latter,"del") == 0 && resTyp == UPDATEtyp){
            ErrorUpdDelWrong(0);
            return;
        }

        if(restoreMode == periodRestore && strcmp(third,"all") != 0){
            ErrUnknownParam(third);
            return;
        }
        else if(restoreMode == TxRestore && strcmp(third,"all") == 0){
            #ifdef CN
            printf("%s事务号恢复模式下请指定事务号进行恢复%s\n",COLOR_WARNING,C_RESET);
            #else
            printf("%splease use Tx number to restore under Tx restore mode%s\n",COLOR_WARNING,C_RESET);
            #endif
            return;
        }

        if(isDelScanned == 0){
            #ifdef CN
            printf("%s针对单表的scan未执行%s\n",COLOR_WARNING,C_RESET);
            #else
            printf("%s<scan> command for delete restore has not been executed yet%s\n",COLOR_WARNING,C_RESET);
            #endif
            return;
        }

        WALFILE *walDirFiles_array = (WALFILE*)malloc(sizeof(WALFILE)*2048);
        WALFILE *archDirFiles_array = (WALFILE*)malloc(sizeof(WALFILE)*2048);

        initWalScan(RESTOREINIT,archDirFiles_array,walDirFiles_array);

        TransactionId txForDel=atoi(txRequested);
        if ( !taboid ){
            warningUseDBFirst();

            return;
        }
        cleanDir("restore/.fpw");
        int matched = 0;

        initCURDBPath("restore/datafile");
        initCURDBPathforDB(CURDBFullPath);
        DELstruct *elem=parray_get(GetTxRetAll,0);

        if(! execGetTx(GetTxRetAll,archDirFiles_array,walDirFiles_array,DELRESTORE,
                        elem->datafile,elem->oldDatafile,elem->toast,elem->oldToast,elem->tabname,elem->typ,txForDel,elem->taboid)){
            return;
        }
        initCURDBPath(CURDBFullPath);
        free(walDirFiles_array);
        free(archDirFiles_array);
        walDirFiles_array = NULL;
        archDirFiles_array = NULL;

        if(restoreMode == TxRestore){
            *SrtTime=0;
            *EndTime=0;
        }
        unloadTimer("end");
        return;
    }
    else if (strcmp(latter,"db") == 0){
        printf("\n");
        #ifdef CN
        printf("%s╔══════════════════════════════════════════════════════════════════╗%s\n", C_BLUE2, C_RESET);
        printf("%s║                 Restore DB - 专业版/企业版功能                    ║%s\n", C_BLUE2, C_RESET);
        printf("%s╠══════════════════════════════════════════════════════════════════╣%s\n", C_BLUE2, C_RESET);
        printf("%s║  Restore DB 功能可以在只剩下一个数据库目录的情况下                  ║%s\n", C_BLUE2, C_RESET);
        printf("%s║  获取数据字典并导出数据。(如仅剩下：base/dbid这一个目录)            ║%s\n", C_BLUE2, C_RESET);
        printf("%s║                                                                  ║%s\n", C_BLUE2, C_RESET);
        printf("%s║  了解更多信息，请访问: %s%s\n", C_BLUE2, PDU_ENTERPRISE_URL, C_RESET);
        printf("%s╚══════════════════════════════════════════════════════════════════╝%s\n", C_BLUE2, C_RESET);
        #else
        printf("%s╔══════════════════════════════════════════════════════════════════╗%s\n", C_BLUE2, C_RESET);
        printf("%s║                Restore DB - Pro/Enterprise feature               ║%s\n", C_BLUE2, C_RESET);
        printf("%s╠══════════════════════════════════════════════════════════════════╣%s\n", C_BLUE2, C_RESET);
        printf("%s║  Restore DB can retrieve the data dictionary and export data     ║%s\n", C_BLUE2, C_RESET);
        printf("%s║  when only a single database directory remains (e.g., base/dbid).║%s\n", C_BLUE2, C_RESET);
        printf("%s║                                                                  ║%s\n", C_BLUE2, C_RESET);
        printf("%s║  Learn more: %s%s\n", C_BLUE2, PDU_ENTERPRISE_URL, C_RESET);
        printf("%s╚══════════════════════════════════════════════════════════════════╝%s\n", C_BLUE2, C_RESET);
        #endif
        printf("\n");
    }
    else{
        ErrUnknownParam(latter);
    }
}

/**
 * ADD_TAB - Add table to manual recovery list
 *
 * @former: Command prefix
 * @latter: Table OID
 * @third:  Table name
 * @fourth: Additional info
 *
 * Adds a table definition for manual data recovery mode.
 */
void ADD_TAB(char *former,char *latter,char *third,char *fourth){
    if(strcmp(CUR_DB,"restore") != 0){
        #ifdef CN
        printf("%s仅在restore库下可进行add操作%s\n",COLOR_ERROR,C_RESET);
        #else
        printf("%s<add> operation is only permitted under <restore> database%s\n",COLOR_ERROR,C_RESET);
        #endif
        return;
    }

    char addFilePath[16+strlen(latter)];
    sprintf(addFilePath,"restore/datafile/%s",latter);
    FILE *addfp =fopen(addFilePath,"rb");
    if(addfp == NULL){
        #ifdef CN
        printf("%srestore/datafile下并不存在数据文件%s，添加失败%s\n",COLOR_ERROR,latter,C_RESET);
        #else
        printf("%sThere is not datafile <%s> in path [restore/datafile]，adding datafile Failed%s\n",COLOR_ERROR,latter,C_RESET);
        #endif
        return;
    }

    char *pgPublicPath="restore/meta/public_tables.txt";
    FILE *pgPublicFile = fopen(pgPublicPath,"a");

    char *item=(char*)malloc(10240);

    int nattr = countCommas(fourth)+1;
    char *alignStr = malloc(nattr*10);
    char *attlenStr = malloc(nattr*10);
    memset(attlenStr,0,nattr*10);
    memset(alignStr,0,nattr*10);
    memset(item,0,10240);
    getAttrAlignAndAttlen(fourth,alignStr,attlenStr);
    sprintf(item,"%s\t%s\tTOASTOID\tTOASTNODE\t2200\t%s\tATTR\t%s\t%d\tMOD\t%s\t%s\n",latter,latter,third,fourth,nattr,attlenStr,alignStr);

    fputs(item,pgPublicFile);
    fclose(pgPublicFile);

    if(taboid != NULL){
        free(taboid);
        taboid=NULL;
    }
    taboid=bootTabStruct(pgPublicPath,1);
    tabSize=getLineNum(pgPublicPath);
    tabVol = NULL;
    tabVol = (TABSIZEstruct *)malloc(tabSize * sizeof(TABSIZEstruct));
    getTabSize(tabVol);
    #ifdef CN
    printf("%s添加完成 ,请用\\dt;查看可unload的表%s\n",COLOR_SUCC,C_RESET);
    #else
    printf("%sAdding complete, please use \\dt; command to show tabs%s\n",COLOR_SUCC,C_RESET);
    #endif
}

/**
 * UNLOAD - Export table data to file
 *
 * @former: Export type (tab, sch, ddl, copy)
 * @latter: Object name
 * @third:  Additional parameter
 *
 * Exports PostgreSQL data to CSV or SQL format.
 */
void UNLOAD(char *former,char *latter,char *third){
    if ( !taboid ){
        warningUseDBFirst();
        return;
    }

    createDir("log");

    if (strcmp(latter,"tab") == 0){
        if(unloadTAB(third) != 1){
            ErrorTabNotExist(third);
        }
    }
    else if(strcmp(latter,"sch") == 0)
    {
        if(unloadSCH(third) != 1){
            ErrorSchNotExist(third);
        }
    }
    else if(strcmp(latter,"db") == 0){
        if(unloadDB(third) != 1){
            #ifdef EN
            printf("Unknown database <%s>\n",third);
            #else
            printf("不存在的数据库 <%s>\n",latter);
            #endif
        }
    }
    else if(strcmp(latter,"ddl") == 0){
        unloadSCHDDL();
    }
    else if(strcmp(latter,"copy") == 0){
        unloadCOPY(CUR_SCH);
    }
    else{
        ErrUnknownParam(latter);
    }
}

/**
 * unloadTAB - Export single table data
 *
 * @tabname: Name of table to export
 *
 * Reads and exports all data from specified table.
 *
 * Returns: SUCCESS_RET on success, FAILURE_RET on failure
 */
int unloadTAB(char *tabname){
    int i;
    char schPath[100];
    int readRet;
    sprintf(schPath,"%s/%s",CUR_DB,CUR_SCH);
    createDir(schPath);

    for ( i = 0; i < tabSize; i++ ) {
        if( strcmp( taboid[i].tab,tabname) == 0 ){
            char pgFilePath[600]="";
            if(strcmp(CUR_DB,"restore") == 0){
                sprintf(pgFilePath, "%s/%s", CURDBFullPath,taboid[i].filenode);
                char toastmeta[100]="";
                sprintf(toastmeta,"%s/%s",CUR_DB,"toastmeta");
                toastBootstrap(toastmeta,taboid[i].toastnode);

            }
            else if(isSingleDB){
                sprintf(pgFilePath, "%s/%s", CUR_DBDIR,taboid[i].filenode);
            }
            else{
                sprintf(pgFilePath, "%s/%s",CUR_DBDIR,taboid[i].filenode);
            }

            char logPathSucc[100];
            sprintf(logPathSucc,"log/%s_%s_%s_%s_%s",CUR_DB,CUR_SCH,"unload",tabname,"succ.txt");
            char logPathErr[100];
            sprintf(logPathErr,"log/%s_%s_%s_%s_%s",CUR_DB,CUR_SCH,"unload",tabname,"err.txt");

            initToastId(taboid[i].toastnode);
            int toastInitRet = initToastHash(CUR_DB,taboid[i].toastnode);
            unloadTimer("start");

            setlogLevel(xmanDecodeLog);
            setToastHash(toastHash);
            readRet = readItems(&taboid[i],pgFilePath,taboid[i].typ,taboid[i].tab,TABLE_BOOTTYPE,logPathSucc,logPathErr);
            if(toastHash != NULL){
                harray_free(toastHash);
            }

            if (readRet == FAILURE_RET){
                if(toastInitRet == FAILURE_RET){
                }
                #ifdef EN
                printf("%sFAIL PARSING TABLE<%s>,please check log %s%s\n",COLOR_WARNING,taboid[i].tab,logPathErr,C_RESET);
                #else
                printf("%s表 <%s> 存在解析失败数据,请查看日志 %s%s\n",COLOR_WARNING,taboid[i].tab,logPathErr,C_RESET);
                #endif
            }
            else if(readRet == FAILOPEN_RET){

            }

            unloadTimer("end");
            return 1;
        }
    }
    return 0;
}

/**
 * unloadSCH - Export all tables in schema
 *
 * @schemaname: Name of schema to export
 *
 * Exports data from all tables in the specified schema.
 *
 * Returns: SUCCESS_RET on success, FAILURE_RET on failure
 */
int unloadSCH(char *schemaname){
    int i;
    if(strcmp(CUR_DB,"restore") == 0){
        #ifdef CN
        printf("restore库不支持此操作\n");
        #else
        printf("Such operation not supported under restore database\n");
        #endif
        return 0;
    }
    for(int j = 0;j<schemasize;j++){
        if( strcmp(schemaname,schoid[j].nspname) == 0 ){
            harray *unloadHash = harray_new(HARRAYINT);
            char schPath[100];
            sprintf(schPath,"%s/%s",CUR_DB,schemaname);
            createDir(schPath);

            char sch2copy[100];
            strcpy(sch2copy,CUR_SCH);
            strcpy(CUR_SCH,schemaname);

            char rec[100]={0};
            sprintf(rec,"%s/%s/.rec",CUR_DB,CUR_SCH);
            FILE *recExist = fopen(rec,"r");
            FILE *recFp = NULL;
            char *recMode = NULL;
            if(recExist){
                fclose(recExist);
                initUnloadHash(rec,unloadHash);
            }

            char logPathSucc[100];
            sprintf(logPathSucc,"log/%s_%s_%s_%s",CUR_DB,"unload_schema",CUR_SCH,"succ.txt");
            char logPathErr[100];
            sprintf(logPathErr,"log/%s_%s_%s_%s",CUR_DB,"unload_schema",CUR_SCH,"err.txt");
            unlink(logPathSucc);
            unlink(logPathErr);

            char DBClassFile[MiddleAllocSize];
            sprintf(DBClassFile, "%s/%s/%s_%s", CUR_DB,"meta",CUR_SCH, TABLE_BOOT);
            if(taboid != NULL){
                free(taboid);
                taboid=NULL;
            }
            taboid=bootTabStruct(DBClassFile,1);
            tabSize=getLineNum(DBClassFile);
            int nErr=0;
            int nNodata=0;
            unloadTimer("start");
            for ( i = 0; i < tabSize; i++ ) {
                recFp = fopen(rec,"a");
                int filenodeOid = atoi(taboid[i].filenode);
                if(harray_search(unloadHash,HARRAYINT,filenodeOid)){
                    continue;
                }
                int readRet;
                char pgFilePath[600]="";
                if(isSingleDB){
                    sprintf(pgFilePath, "%s/%s", CUR_DBDIR,taboid[i].filenode);
                }else{
                    sprintf(pgFilePath, "%s/%s",CUR_DBDIR,taboid[i].filenode);
                }
                initToastId(taboid[i].toastnode);
                int toastInitRet = initToastHash(CUR_DB,taboid[i].toastnode);

                setToastHash(toastHash);
                setlogLevel(readItemLog);
                readRet = readItems(&taboid[i],pgFilePath,taboid[i].typ,taboid[i].tab,TABLE_BOOTTYPE,logPathSucc,logPathErr);
                if(toastHash != NULL){
                    harray_free(toastHash);
                }

                if (readRet == FAILURE_RET){
                    if(toastInitRet == FAILURE_RET){
                        ErrorToastNoExist((Oid)atoi(taboid[i].toastnode));
                    }
                    char err3[1024];
                    FILE *logErr = fopen(logPathErr,"a");
                    #ifdef EN
                    sprintf(err3,"FAIL PARSING TABLE <%s>,DATAFILE<%s>\n",taboid[i].tab,pgFilePath);
                    #else
                    sprintf(err3,"表 <%s> 解析失败,对应的数据文件路径为 <%s>\n",taboid[i].tab,pgFilePath);
                    #endif
                    nErr++;
                    fputs(err3,logErr);
                    fclose(logErr);
                    fclose(recFp);
                }
                else if(readRet == FAILOPEN_RET){
                    nNodata++;
                    fputs(taboid[i].filenode,recFp);
                    fputs("\n",recFp);
                    fclose(recFp);
                }
                else if(readRet == SUCCESS_RET){
                    fputs(taboid[i].filenode,recFp);
                    fputs("\n",recFp);
                    fclose(recFp);
                }

            }
            FILE *logSucc = fopen(logPathSucc,"a");
            unloadTimer("end");
            infoUSchSucc(schemaname,tabSize,nNodata,nErr,logPathErr,logPathSucc);
            char succ2[500];
            #ifdef EN
            sprintf(succ2,"\n\nSchema <%s> %d tables in total。Success: %d, Empty table: %d, Failure: %d \nLog Path\n\t|-Succ Log：%s\n\t|-Fail Log：%s\n",schemaname,tabSize,tabSize-nErr-nNodata,nNodata,nErr,logPathErr,logPathSucc);
            #else
            sprintf(succ2,"\n\n模式<%s>共 %d 张表。成功：%d, 无数据：%d, 失败：%d \n日志路径\n\t|-成功日志：%s\n\t|-失败日志：%s\n",schemaname,tabSize,tabSize-nErr-nNodata,nNodata,nErr,logPathErr,logPathSucc);
            #endif
            fputs(succ2,logSucc);
            fclose(logSucc);
            unloadCOPY(schemaname);
            unloadSCHDDL();
            strcpy(CUR_SCH,sch2copy);
            return 1;
        }
    }
    return 0;
}

/**
 * unloadDB - Export entire database
 *
 * @databasename: Name of database to export
 *
 * Exports data from all tables in all schemas of the database.
 *
 * Returns: SUCCESS_RET on success, FAILURE_RET on failure
 */
int unloadDB(char *databasename){
    for(int i=0;i<dosize;i++){
        if(strcmp(databasename,databaseoid[i].database) == 0){
            char CUR_DB_copy[100];
            char CUR_SCH_copy[100];
            strcpy(CUR_DB_copy,CUR_DB);
            strcpy(CUR_SCH_copy,CUR_SCH);

            strcpy(CUR_DB,databasename);

            char DBSchemaFile[MiddleAllocSize];
            sprintf(DBSchemaFile, "%s/%s/%s", CUR_DB, "meta",SCHEMA_BOOT);
            schoid=bootSCHStruct(DBSchemaFile);
            schemasize=getLineNum(DBSchemaFile);
            for(int j=0;j<schemasize;j++){
                if(schemaInDefaultSHCS(schoid[j].nspname)){
                    continue;
                }
                strcpy(CUR_SCH,schoid[j].nspname);
                char schPath[100];
                sprintf(schPath,"%s/%s",CUR_DB,CUR_SCH);
                createDir(schPath);

                char logPathSucc[100];
                sprintf(logPathSucc,"log/%s_%s_%s","unload_db",CUR_DB,"succ.txt");
                char logPathErr[100];
                sprintf(logPathErr,"log/%s_%s_%s","unload_db",CUR_DB,"err.txt");
                unlink(logPathSucc);
                unlink(logPathErr);

                char DBClassFile[MiddleAllocSize];
                sprintf(DBClassFile, "%s/%s/%s_%s", CUR_DB,"meta",CUR_SCH, TABLE_BOOT);
                if(taboid != NULL){
                    free(taboid);
                    taboid=NULL;
                }
                taboid=bootTabStruct(DBClassFile,1);
                tabSize=getLineNum(DBClassFile);
                int nErr=0;
                int nNodata=0;
                for ( i = 0; i < tabSize; i++ ) {
                    char pgFilePath[600]="";
                    sprintf(pgFilePath, "%s%s%s/%s",CUR_DBDIR,taboid[i].filenode);
                    initToastId(taboid[i].toastnode);
                    setlogLevel(readItemLog);
                    int readRet = readItems(&taboid[i],pgFilePath,taboid[i].typ,taboid[i].tab,TABLE_BOOTTYPE,logPathSucc,logPathErr);
                    if (readRet == FAILURE_RET){
                        char err3[1024];
                        FILE *logErr = fopen(logPathErr,"a");
                        sprintf(err3,"FAIL PARSING TABLE <%s>,DATAFILE<%s>\n",taboid[i].tab,pgFilePath);
                        nErr++;
                        fputs(err3,logErr);
                        fclose(logErr);
                    }
                    else if(readRet == FAILOPEN_RET){
                        nNodata++;
                    }
                }
                FILE *logSucc = fopen(logPathSucc,"a");

                char succ2[100];
                #ifdef EN
                sprintf(succ2,"\n\nSCHEMA <%s> CONTAINS %d TABLES ,SUCCESS NUMBER:%d ,NODATA NUMBER:%d ,FAILED NUMBER: %d  \nLOG DIR:log/%s \n",CUR_SCH,tabSize,tabSize-nErr,nNodata,nErr,logPathErr);
                #else
                sprintf(succ2,"\n\n模式<%s>共 %d 张表。成功：%d, 无数据：%d, 失败 %d \n日志路径:%s \n",CUR_SCH,tabSize,tabSize-nErr,nNodata,nErr,logPathErr);
                #endif
                printf("%s",succ2);
                fputs(succ2,logSucc);
                fclose(logSucc);
                unloadCOPY(CUR_SCH);
                unloadSCHDDL();

            }
            strcpy(CUR_DB,CUR_DB_copy);
            strcpy(CUR_SCH,CUR_SCH_copy);
            return 1;
        }
    }
    return 0;
}

/**
 * unloadCOPY - Generate COPY statements for data import
 *
 * @schemaname: Schema name for COPY statements
 *
 * Creates PostgreSQL COPY command script for bulk data loading.
 */
void unloadCOPY(char *schemaname){
    char COPY[100]="";
    sprintf(COPY,"%s/COPY",CUR_DB);
    createDir(COPY);

    char copyFilename[150];
    memset(copyFilename,0,100);
    sprintf(copyFilename,"%s/%s_copy.sql",COPY,schemaname);
    FILE *copyfp = fopen(copyFilename,"w");

    if(copyfp == NULL){
        printf("COPYFile<%s>创建failed ,未能生成COPY命令\n",copyFilename);
        return;
    }

    char setSch[100]="";
    sprintf(setSch,"set search_path to %s;\n",schemaname);
    fputs(setSch,copyfp);

    char csvpath[100];
    sprintf(csvpath,"%s/%s",CUR_DB,schemaname);

    int PATHSIZE=1024;
    char currPath[PATHSIZE];
    char tabName[PATHSIZE];
    char fullPath[2*PATHSIZE];
    char filenames[MAX_FILES][MAX_FILENAME_LENGTH];
    int file_count = 0;

#if defined(_WIN32)
    DWORD len =GetModuleFileName(NULL, currPath, 10240);
    if (len > 0 && len < 10240) {
        currPath[len] = '\0';
        for (DWORD i = len - 1; i > 0; i--) {
            if (currPath[i] == '\\') {
                currPath[i] = '\0';
                break;
            }
        }
    }
    sprintf(fullPath,"%s/%s/*",currPath,csvpath);
    WIN32_FIND_DATA findFileData;
    HANDLE hFind = FindFirstFile(fullPath, &findFileData);

    if (hFind != INVALID_HANDLE_VALUE) {
        do {
            if (!(findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
                strncpy(filenames[file_count], findFileData.cFileName, MAX_FILENAME_LENGTH - 1);
                filenames[file_count][MAX_FILENAME_LENGTH - 1] = '\0';
                file_count++;
            }
        } while (FindNextFile(hFind, &findFileData) != 0);

        FindClose(hFind);
    }
    size_t len1 = strlen(fullPath);
    fullPath[len1-1]='\0';

#elif defined(__linux__)
    char path[1024];
    memset(path,0,1024);
    ssize_t len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    get_parent_directory(path);
    sprintf(fullPath,"%s/%s",path,csvpath);
    DIR *dir1;
    struct dirent *entry1;
    int arraySize=0;
    dir1 = opendir(csvpath);
    if (dir1 == NULL) {
        #ifdef EN
        printf("directory <%s> does not exist,please <unload> first\n",csvpath);
        #else
        printf("路径<%s>不存在 ,请先执行unload命令导出数据\n",csvpath);
        #endif
        return;
    }
    else{
        while ((entry1 = readdir(dir1)) != NULL) {
            if (entry1->d_type == 8) {
                if(strcmp(entry1->d_name,".rec") == 0)
                    continue;
                strcpy(filenames[file_count],entry1->d_name);
                file_count++;
            }
        }
    }
#endif

    for (int i = 0; i < file_count; i++) {
        char str2write[1000]="";
        char *dot = strrchr(filenames[i], '.');
        if (dot != NULL) {
            size_t len = dot - filenames[i];
            strncpy(tabName, filenames[i], len);
            tabName[len] = '\0';
        }
        char *tabNameProcessed = quotedIfUpper(tabName);
        sprintf(str2write,"COPY %s FROM '%s/%s';\n",tabNameProcessed,fullPath,filenames[i]);
        fputs(str2write,copyfp);
    }

    fclose(copyfp);
    infoUCopySucc(copyFilename,file_count);
}

/**
 * unloadSCHDDL - Export schema DDL statements
 *
 * Generates CREATE TABLE statements for current schema.
 */
void unloadSCHDDL()
{
    char SCHDDL[100]="";
    sprintf(SCHDDL,"%s/DDL",CUR_DB);
    createDir(SCHDDL);

    char ddlFilename[200]="";
    sprintf(ddlFilename,"%s/%s_ddl.sql",SCHDDL,CUR_SCH);
    FILE *ddl = fopen(ddlFilename,"w");

    if(ddl == NULL){
        printf("DDLFile<%s>创建failed ,未能生成DDL命令\n",ddlFilename);
        return;
    }

    char msg_setpath[100]="";
    sprintf(msg_setpath,"CREATE SCHEMA %s;\nset search_path to %s;\n",CUR_SCH,CUR_SCH);
    fputs(msg_setpath,ddl);

    int i;
    for ( i = 0; i < tabSize; i++ ) {
        char msg_begin[200]="";
        char *ddltabname = quotedIfUpper(taboid[i].tab);
        sprintf(msg_begin,"CREATE TABLE %s(\n",ddltabname);

        fputs(msg_begin,ddl);

        char attr[10240]="";
        strcpy(attr,taboid[i].attr);
        char typ[10240]="";
        strcpy(typ,taboid[i].typ);
        char attmod[10240]="";
        strcpy(attmod,taboid[i].attmod);

        ata2DDL(attr,typ,attmod,ddl);

        char msg_end[200]="";
        sprintf(msg_end,");\n\n");
        fputs(msg_end,ddl);
    }
    infoUddlSucc(ddlFilename,tabSize);
    fclose(ddl);
}

/**
 * EXIT - Clean up and exit program
 *
 * Performs cleanup operations and terminates the program.
 */
void EXIT(){
    exit(1);
}

void loadParam()
{
    FILE *configFile;
    char line[1024];
    configFile = fopen("pdu.ini", "r");
    if (configFile == NULL) {
        printf("No pdu.ini FOUND\n");
        return;
    }
    char PGDATA_EXCLUDE[MAXPGPATH]={0};
    while (fgets(line, sizeof(line), configFile)) {
        line[strcspn(line, "\n")] = '\0';

        if (strncmp(line, "PGDATA=", 7) == 0) {
            sscanf(line + 7, "%s", initDBPath);
        }

        if (strncmp(line, "ARCHIVE_DEST=", 13) == 0) {
            sscanf(line + 13, "%s", initArchPath);
        }

        if (strncmp(line, "DISK_PATH=", 10) == 0) {
            sscanf(line + 10, "%s", diskPath);
        }

        if (strncmp(line, "BLOCK_INTERVAL=", 15) == 0) {
            char *val=malloc(10);
            sscanf(line + 15, "%s", val);
            blkInterval = atoi(val);
            if(blkInterval < 1){
                blkInterval = 5;
            }
        }

        if(strncmp(line,"PGDATA_EXCLUDE=",15) == 0){
            sscanf(line + 15,"%s",PGDATA_EXCLUDE);
        }
    }

    DIR *dir;
    if ((dir = opendir(initDBPath)) == NULL) {
        #ifdef EN
        printf("%sNO SUCH PGDATA PATH AS : %s%s \n",COLOR_ERROR,initDBPath,C_RESET);
        #else
        printf("%s此PGDATA路径不存在: %s%s\n",COLOR_ERROR,initDBPath,C_RESET);
        #endif
        exit(1);
    }

    pgdataExclude = parray_new();
    char *ret  = get_symlink_target(initDBPath);
    parray_append(pgdataExclude,ret);
    if(strlen(PGDATA_EXCLUDE) != 0){
        int cnt = countCommas(PGDATA_EXCLUDE);
        for (size_t i = 0; i < cnt+1; i++)
        {
            char *path = get_field(',',PGDATA_EXCLUDE,i+1);
            char *path_ret  = get_symlink_target(path);
            if(path_ret == NULL){
                #ifdef EN
                printf("%sNO SUCH PGDATA_EXCLUDE PATH AS : %s%s \n",COLOR_ERROR,path,C_RESET);
                #else
                printf("%s此PGDATA_EXCLUDE路径不存在: %s%s\n",COLOR_ERROR,path,C_RESET);
                #endif
                exit(1);
            }
            parray_append(pgdataExclude,path_ret);
        }
    }
    fclose(configFile);

    size_t lenDB = strlen(initDBPath);
    size_t lenArch = strlen(initArchPath);

    if(lenDB == 0){
        printf("%sNo PGDATA Found in pdu.ini, Please Check pdu.ini%s\n",COLOR_ERROR,C_RESET);
        exit(1);
    }

    if (initDBPath[lenDB - 1] != '/') {
        initDBPath[lenDB] = '/';
        initDBPath[lenDB + 1] = '\0';
    }

    if (initArchPath[lenArch - 1] == '/') {
        initArchPath[lenArch-1] = '\0';
    }

}
int getInit(){

    loadParam();

    if(getPGVersion(initDBPath) == FAILURE_RET){
        exit(1);
    }

    CUR_SCH = malloc(100);
    CUR_DB = malloc(100);
    strcpy(CUR_SCH,"public");
    strcpy(CUR_DB,"PDU");
    SrtTime=(TimestampTz*)malloc(sizeof(TimestampTz));
    EndTime=(TimestampTz*)malloc(sizeof(TimestampTz));
    *SrtTime=0;
    *EndTime=0;
    setRestoreMode_there(periodRestore);
    setExportMode_decode(CSVform);
    setExportMode_there(CSVform);
    cprDeclaration();
    char dfcmd[MAXPGPATH]={0};
    sprintf(dfcmd,"df -h %s | awk 'NR>1 {print $1}'",initDBPath);
    char *retdev = execCMD(dfcmd);
    if(strcmp(retdev,diskPath) != 0 && strlen(diskPath) > 0){
        #ifdef CN
        printf("%s注意：PGDATA所属的磁盘与DISK_PATH设置的磁盘不一致，执行ds idx时有效块无法被正确排除\n%s",COLOR_WARNING,C_RESET);
        #else
        printf("%sWarning: The disk where PGDATA resides is inconsistent with the disk set by DISK_PATH. Valid blocks cannot be properly excluded when executing 'ds idx'\n%s",COLOR_WARNING,C_RESET);
        #endif
    }
    return 1;
}

char* getinput() {
    printf("%s%s.%s=#%s ",COLOR_CMD,CUR_DB,CUR_SCH,C_RESET);
    char output[102400]="";
    char c;
    size_t input_len = 0;

    while (1) {

        c = getchar();

        while (c == '\n' && lastc != ';'){
            printf("%s%s.%s=#%s ",COLOR_CMD,CUR_DB,CUR_SCH,C_RESET);
            c = getchar();
        }
        lastc = c;
        output[input_len] = c;
        input_len++;

        if(input_len>10240){
            return "UNKNOWN";
        }
        else if (c == ';') {
            input_len--;
            output[input_len] = '\0';
            char *output1=(char*)malloc(10240*sizeof(char));
            trim(output,output1);
            trimLeadingSpaces(&output1);
            return output1;
        }
    }
}

int getToastHash(FILE *fp,unsigned int blockSize,FILE *destfp,int hundred)
{
    unsigned int keepDumping=1;
    unsigned int bytesToFormat;
    int toastIsEmpty=1;
    FILE *bootFile;
    int nItems=0;
    int nPages=0;
    keepDumping = 1;
	int result = -1;
    BlockNumber	currentBlockNo = 0;
    char *block = (char *)malloc(blockSize);
    if (!block)
    {
        printf("\nFAILED TO ALLOCATE SIZE OF <%d> BYTES \n",
            blockSize);
    }

    while(keepDumping){

        bytesToFormat = fread(block, 1, blockSize, fp);
        nPages++;
        if (bytesToFormat == 0)
        {
            keepDumping = 0;
        }
        else{
            Page page = (Page) block;

            unsigned int x;
            unsigned int i;
            unsigned int itemSize;
            unsigned int itemOffset;
            unsigned int itemFlags;
            ItemId		itemId;
            int	maxOffset;

            maxOffset = PageGetMaxOffsetNumber(page);

            if (maxOffset == 0)
            {
				continue;
            }
            for(x= 1 ; x < maxOffset+1 ; x++){
                itemId = PageGetItemId(page, x);
                itemFlags = (unsigned int) ItemIdGetFlags(itemId);
                itemSize = (unsigned int) ItemIdGetLength(itemId);
                itemOffset = (unsigned int) ItemIdGetOffset(itemId);

				uint32			chunkId=10086;
				uint32			toastOid=0;
				unsigned int	chunkSize = 0;
                if(itemFlags == LP_NORMAL){
                    int ret = ToastChunkforOid(&block[itemOffset], itemSize, &chunkId,&toastOid);
                    if(ret == -1)
                        break;
                    char destInfo[100]={0};

                    if(toastOid > 0 && chunkId >= 0)
                    {
                        sprintf(destInfo,"%d\t%d\t%d\t%d\t%d\n",toastOid,chunkId,currentBlockNo,itemOffset,hundred);
                        fputs(destInfo,destfp);
                    }
                    toastIsEmpty=0;
                }
			}
		}
        currentBlockNo++;
	}
    free(block);
    return toastIsEmpty;
}

static int DecodeToastOid(const char *buffer,unsigned int buff_size,unsigned int *processed_size,Oid *result)
{
	const char	   *new_buffer = (const char *) INTALIGN(buffer);
	unsigned int	delta = (unsigned int)((uintptr_t)new_buffer - (uintptr_t)buffer);

	if (buff_size < delta)
		return -1;

	buff_size -= delta;
	buffer = new_buffer;

    if (buff_size < delta + sizeof(Oid))
        return -1;

	*result = *(Oid *)buffer;
	*processed_size = sizeof(Oid) + delta;

	return 0;
}

int ToastChunkforOid(const char *tuple_data,unsigned int tuple_size,uint32 *chunk_id,Oid *toastoid)
{
	HeapTupleHeader		header = (HeapTupleHeader)tuple_data;
	const char	   *data = tuple_data + header->t_hoff;
	unsigned int	size = tuple_size - header->t_hoff;
	unsigned int	processed_size = 0;
	int				ret;
	Oid toastread=0;
	ret = DecodeToastOid(data, size, &processed_size, &toastread);

    if(ret == -1)
        return -1;

	*toastoid = toastread;
	size -= processed_size;
	data += processed_size;

	ret = DecodeToastOid(data, size, &processed_size, chunk_id);

    if(*chunk_id > 10000)
        return -1;

}

int initToastHash(char *CUR_DB,char *toastnode)
{
    char metaToastPath[100];
    sprintf(metaToastPath,"%s/toastmeta/%s",CUR_DB,toastnode);
    int numLines = 0;
    FILE *file = fileGetLines(metaToastPath,&numLines);
    toastHash = NULL;
    if(file == NULL){
        return FAILURE_RET;
    }

    toastHash = harray_new(HARRAYTOAST);

    int i;
    for (i = 0; i < numLines; i++) {
        chunkInfo *elem = malloc(sizeof(chunkInfo));
        char chunkId[20];
        char toastOid[20];
        char blk[20];
        char toff[20];
        char suffix[20];
        if (fscanf(file, "%s\t%s\t%s\t%s\t%s\n",toastOid,chunkId,blk,toff,suffix) != 5) {
            printf("Error Reading File %s\n",metaToastPath);
            exit(1);
        }
        elem->toid=atoi(toastOid);
        elem->chunkid=atoi(chunkId);
        elem->blk=atoi(blk);
        elem->toff=atoi(toff);
        elem->suffix=atoi(suffix);
        harray_append(toastHash,HARRAYTOAST,elem,elem->toid);
    }

    fclose(file);
    return SUCCESS_RET;
}

void INFO(char *latter){
    int tabMatched = 0;
    if ( !taboid ){
        warningUseDBFirst();
        return;
    }
    else if (latter == NULL){
        ErrorTabNotExist(latter);
    }
    else{
        for ( int i = 0; i < tabSize; i++ ) {
            if( strcmp( taboid[i].tab,latter) == 0 ){
                char fulltabpath[MAXPGPATH]={0};
                char fulltoastpath[MAXPGPATH]={0};
                sprintf(fulltabpath,"%s/%s",CUR_DBDIR,taboid[i].filenode);
                sprintf(fulltoastpath,"%s/%s",CUR_DBDIR,taboid[i].toastnode);
                print_file_blocks(fulltabpath,fulltoastpath);
                tabMatched = 1;
            }
        }
        if( !tabMatched ){
            ErrorTabNotExist(latter);
        }
    }
}

bool HeapTupleSatisfiesVisibility(HeapTupleHeader tuple)
{
    TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);
    TransactionId xmin = HeapTupleHeaderGetRawXmin(tuple);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;
    }

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (xmax>0)
	{
        return false;
	}
    return true;
}

void META(char *type,char *objname){
    if( strcmp(type,"sch") != 0 && strcmp(type,"tab") != 0 ){
        ErrUnknownParam(type);
        return;
    }
    if(!taboid){
        warningUseDBFirst();
        return;
    }
    if (fclose(fopen("restore/tab.config", "w")) == EOF) perror("Error clearing file");
    FILE *fp = fopen("restore/tab.config","a");
    int cnt = 0;
    int matched = 0;
    if(strcmp(type,"sch") == 0){

        for(int j = 0;j<schemasize;j++){
            if( strcmp(objname,schoid[j].nspname) == 0 ){
                matched = 1;
                char sch2copy[100];
                strcpy(sch2copy,CUR_SCH);
                strcpy(CUR_SCH,objname);

                char DBClassFile[MiddleAllocSize];
                sprintf(DBClassFile, "%s/%s/%s_%s", CUR_DB,"meta",CUR_SCH, TABLE_BOOT);
                if(taboid != NULL){
                    free(taboid);
                    taboid=NULL;
                }
                taboid=bootTabStruct(DBClassFile,1);
                tabSize=getLineNum(DBClassFile);
                for (int j = 0; j < tabSize; j++)
                {
                    TABstruct elem = taboid[j];
                    char *str = (char*)malloc(20+strlen(elem.tab)+strlen(elem.typ));
                    sprintf(str,"%s %s\n",elem.tab,elem.typ);
                    fputs(str,fp);
                }
                strcpy(CUR_SCH,sch2copy);
            }
        }

        cnt = tabSize;
    }
    else if(strcmp(type,"tab") == 0){
        int commaCnt = countCommas(objname);
        for (int i = 0; i < commaCnt+1; i++)
        {
            char *tab = get_field(',',objname,i+1);
            for (int j = 0; j < tabSize; j++)
            {
                TABstruct elem = taboid[j];
                if(strcmp(elem.tab,tab) == 0){
                    char *str = (char*)malloc(20+strlen(elem.tab)+strlen(elem.typ));
                    sprintf(str,"%s %s",elem.tab,elem.typ);
                    printf("%s%s%s\n",COLOR_UNLOAD,str,C_RESET);
                    fputs(str,fp);
                    matched = 1;
                    break;
                }
            }
        }
        cnt = commaCnt+1;
    }
    if(strcmp(type,"sch") == 0 && matched == 0){
        ErrorSchNotExist(objname);
    }
    else if(strcmp(type,"tab") == 0 && matched == 0){
        #ifdef CN
        printf("%s输入的表名均不存在%s\n",COLOR_ERROR,C_RESET);
        #else
        printf("%sNone of the input table names exist%s\n",COLOR_ERROR,C_RESET);
        #endif
    }
    else{
        #ifdef CN
        printf("%s完成，共导入%d个可dropscan的表对象\n%s",COLOR_SUCC,cnt,C_RESET);
        #else
        printf("%sCompleted, imported %d dropscan-capable table objects in total\n%s",COLOR_SUCC,cnt,C_RESET);
        #endif
    }

    fclose(fp);

}

void CHECKWAL()
{
    char waldir[MAXPGPATH] = {0};
    XLogSegNo	segno;
    int WalSegSz;
    int r;
    cleanDir("restore/ckwal");
    WALFILE *walDirFiles_array = (WALFILE*)malloc(sizeof(WALFILE)*2048);
    WALFILE *archDirFiles_array = (WALFILE*)malloc(sizeof(WALFILE)*2048);
    XLogLongPageHeader longhdr = NULL;
    XLogDumpPrivate private;
	memset(&private, 0, sizeof(XLogDumpPrivate));
	private.timeline = 1;
	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;
	private.endptr_reached = false;
    initWalScan(RESTOREINIT,archDirFiles_array,walDirFiles_array);
    PGAlignedXLogBlock buf;

    int isMess = 0;
    for(int j=0;j<archWaldirNum;j++){

        char destPath[MAXPGPATH]={0};
        char		fpath[MAXPGPATH];
        snprintf(fpath, MAXPGPATH, "%s/%s", initArchPath, archDirFiles_array[j].walnames);
        FILE *fp = fopen(fpath,"rb");
        r = fread(buf.data,1,BLCKSZ,fp);
        if (r == XLOG_BLCKSZ)
        {
            longhdr = (XLogLongPageHeader) buf.data;
            WalSegSz = longhdr->xlp_seg_size;
            XLogFromFileName(archDirFiles_array[j].walnames, &private.timeline, &segno, WalSegSz);
            private.startptr = longhdr->std.xlp_pageaddr;
            private.endptr = private.startptr + WalSegSz*2;
        }
        XLByteToSeg(private.startptr, segno, WalSegSz);
        char ptrfName[50]={0};
        XLogFileName(ptrfName, private.timeline, segno, WalSegSz);

        if(strcmp(ptrfName,archDirFiles_array[j].walnames) != 0){
            isMess = 1;
        }
        if(isMess){
            sprintf(waldir,"restore/ckwal");
            sprintf(destPath,"%s/%s",waldir,ptrfName);
            int ret = copyFile(fpath,destPath);
            if(ret == EXIT_FAILURE){
                return;
            }
            printf("%s%s -> %s%s\n",C_PURPLE2,archDirFiles_array[j].walnames,ptrfName,C_RESET);
        }
        else{
            sprintf(waldir,"restore/ckwal");
            sprintf(destPath,"%s/%s",waldir,archDirFiles_array[j].walnames);
            int ret = copyFile(fpath,destPath);
            if(ret == EXIT_FAILURE){
                return;
            }
            printf("%s%s -> %s%s\n",C_PURPLE2,archDirFiles_array[j].walnames,ptrfName,C_RESET);
        }

    }
    if(!isMess){
        cleanDir("restore/ckwal");
    }
    free(walDirFiles_array);
    free(archDirFiles_array);
    walDirFiles_array = NULL;
    archDirFiles_array = NULL;
    harray_free(TxXman_harray);
    parray_free(TxSaved_paaray);
    TxXman_harray = NULL;
    TxSaved_paaray = NULL;
}

void setSrtWalname(char *third)
{
    if(IsXLogFileName(third)){
        strcpy(manualSrtWal,third);
        SHOW_PARAM();
    }
    else{
        ErrorWalname();
    }
}

void setEndWalname(char *third)
{
    if(IsXLogFileName(third)){
        strcpy(manualEndWal,third);
        SHOW_PARAM();
    }
    else{
        ErrorWalname();
    }
}

void setDropScanOff(char *third)
{
    off_t number = atoll(third);
    if(number % 4096 == 0){
        dropScanSrtOff = number;
        SHOW_PARAM();
    }
    else{
        ErrorDropScanOff();
    }
}

void setIdxMode(char *third)
{
    if(strcmp(third,"on") != 0 && strcmp(third,"off") != 0)
    {
        #ifdef CN
        printf("%s请设置为on/off%s\n",COLOR_WARNING,C_RESET);
        #else
        printf("%sOnly on/off can be set%s\n",COLOR_WARNING,C_RESET);
        #endif
    }
    else{
        isoMode = strcmp(third,"on") == 0 ? 1 : 0;
        SHOW_PARAM();
    }
}

void setBlkIntval(char *third)
{
    int val = atoi(third);
    if(val < 1 || val > 1024)
    {
        #ifdef CN
        printf("%s非法数值%s\n",COLOR_WARNING,C_RESET);
        #else
        printf("%sInvalid Values%s\n",COLOR_WARNING,C_RESET);
        #endif
    }
    else{
        blkInterval = val;
        SHOW_PARAM();
    }
}

void setItmsPerCsv(char *third)
{
    int val = atoi(third);
    if( val < 100 )
    {
        #ifdef CN
        printf("%s非法数值%s\n",COLOR_WARNING,C_RESET);
        #else
        printf("%sInvalid Values%s\n",COLOR_WARNING,C_RESET);
        #endif
    }
    else{
        itemspercsv = val;
        SHOW_PARAM();
    }
}

void setTime(char *third,char *fourth,int flag){
    if(restoreMode == TxRestore){
        #ifdef CN
        printf("%s事务号 恢复模式下不设置起始时间\n%s",COLOR_ERROR,C_RESET);
        #else
        printf("%sstarttime and endtime are not set under Tx restore mode.\n",COLOR_ERROR,C_RESET);
        #endif
        return;
    }

    char time[100];
    sprintf(time,"%s %s",third,fourth);
    if(flag == 0){
        *SrtTime=str_to_timestamptz_og(time);
        if(*SrtTime == -1){
            printf("InputParameter不符合YYYY-MM-DD HH:MM:SS的规范\n");
        }
        else
            SHOW_PARAM();
    }
    else if(flag == 1){
        *EndTime=str_to_timestamptz_og(time);
        if(*EndTime == -1){
            printf("InputParameter不符合YYYY-MM-DD HH:MM:SS的规范\n");
        }
        else
            SHOW_PARAM();
    }
}

void setExmode(char *third){
    int setting;
    if(strcmp(third,"csv") == 0){
        setting=CSVform;
    }
    else if (strcmp(third,"sql") == 0){
        setting=SQLform;
    }
    else if (strcmp(third,"db") == 0){
        setting=DBform;
    }
    else{
        #ifdef CN
        printf("未知的导出模式<%s>\n",third);
        #else
        printf("unknown export mode <%s>\n",third);
        #endif
        return;
    }
    exmode=setting;
    setExportMode_there(setting);
    setExportMode_decode(setting);
    SHOW_PARAM();
}

void setResmode(char *third){
    if(strcmp(third,"tx") == 0){
        restoreMode=TxRestore;
        setRestoreMode_there(TxRestore);
    }
    else if (strcmp(third,"time") == 0){
        restoreMode=periodRestore;
        setRestoreMode_there(periodRestore);
    }
    else{
        #ifdef CN
        printf("未知的恢复模式<%s>\n",third);
        #else
        printf("unknown restore mode <%s>\n",third);
        #endif
        return;
    }
    SHOW_PARAM();
}

void setEncoding(char *third){
    if(strcmp(third,"utf8") == 0){
        pduEncoding = UTF8encoding;
        setEncoding_there(UTF8encoding);
    }
    else if (strcmp(third,"gbk") == 0){
        pduEncoding = GBKencoding;
        setEncoding_there(GBKencoding);
    }
    else{
        #ifdef CN
        printf("未知的字符集<%s>\n",third);
        #else
        printf("unknown encoding <%s>\n",third);
        #endif
        return;
    }
    SHOW_PARAM();
}

void setEndLsnT(char *third)
{
    int elemNumber = parray_num(GetTxRetAll);
    if(elemNumber != 0){
        for(int i=0;i<elemNumber;i++){
            DELstruct *elem = parray_get(GetTxRetAll,i);
            memset(elem->endLSNforTOAST,0,50);
            strncpy(elem->endLSNforTOAST,third,50);
        }
        SHOW_PARAM();
    }
    else{
        printf("%sNOT SCANNED YET%s\n",COLOR_WARNING,C_RESET);
    }
}

void setStartLsnT(char *third)
{
    int elemNumber = parray_num(GetTxRetAll);
    if(elemNumber != 0){
        for(int i=0;i<elemNumber;i++){
            DELstruct *elem = parray_get(GetTxRetAll,i);
            memset(elem->startLSNforTOAST,0,50);
            strncpy(elem->startLSNforTOAST,third,50);
        }
        SHOW_PARAM();
    }
    else{
        printf("%sNOT SCANNED YET%s\n",COLOR_WARNING,C_RESET);
    }
}

void setRestype(char *third){
    if(strcmp(third,"update") == 0){
        resTyp = UPDATEtyp;
        setResTyp_there(UPDATEtyp);
        setResTyp_decode(UPDATEtyp);
        memset(resStr,0,10);
        #ifdef CN
        sprintf(resStr,"%s","更新");
        #else
        sprintf(resStr,"%s","updated");
        #endif
    }
    else if (strcmp(third,"delete") == 0){
        resTyp = DELETEtyp;
        setResTyp_there(DELETEtyp);
        setResTyp_decode(DELETEtyp);
        memset(resStr,0,10);
        #ifdef CN
        sprintf(resStr,"%s","删除");
        #else
        sprintf(resStr,"%s","deleted");
        #endif
    }
    else{
        #ifdef CN
        printf("未知的恢复类型<%s>\n",third);
        #else
        printf("unknown restore type <%s>\n",third);
        #endif
        return;
    }
    SHOW_PARAM();
}

void setRestypeNoShow(char *third)
{
    if(strcmp(third,"update") == 0){
        resTyp = UPDATEtyp;
        setResTyp_there(UPDATEtyp);
        setResTyp_decode(UPDATEtyp);
        memset(resStr,0,10);
        #ifdef CN
        sprintf(resStr,"%s","更新");
        #else
        sprintf(resStr,"%s","updated");
        #endif
    }
    else if (strcmp(third,"delete") == 0){
        resTyp = DELETEtyp;
        setResTyp_there(DELETEtyp);
        setResTyp_decode(DELETEtyp);
        memset(resStr,0,10);
        #ifdef CN
        sprintf(resStr,"%s","删除");
        #else
        sprintf(resStr,"%s","deleted");
        #endif
    }
    else{
        #ifdef CN
        printf("未知的恢复类型<%s>\n",third);
        #else
        printf("unknown restore type <%s>\n",third);
        #endif
        return;
    }
}

void SHOW_PARAM()
{
    infoParamHeader();

    char walret1[60];
    char walret2[60];

    sprintf(walret1,"   %s",manualSrtWal);
    sprintf(walret2,"   %s",manualEndWal);
    printfParam("startwal",walret1);
    printfParam("endwal",walret2);

    printLsnT();

    if(*SrtTime == 0)
        printfParam("starttime"," ");
    else
        printfParam("starttime",(char *)timestamptz_to_str_og(*SrtTime));
    if(*EndTime == 0)
        printfParam("endtime"," ");
    else
        printfParam("endtime",(char *)timestamptz_to_str_og(*EndTime));

    if(restoreMode == TxRestore)
        printfParam("resmode(Data Restore Mode)","              TX");
    else if (restoreMode == periodRestore)
        printfParam("resmode(Data Restore Mode)","              TIME");

    if(exmode == CSVform)
        printfParam("exmode(Data Export Mode)      ","              CSV");
    else if (exmode == SQLform)
        printfParam("exmode(Data Export Mode)      ","              SQL");
    else if (exmode == DBform)
        printfParam("exmode(Data Export Mode)      ","              DB");

    if(pduEncoding == UTF8encoding )
        printfParam("encoding","              UTF8");
    else if (pduEncoding == GBKencoding )
        printfParam("encoding","              GBK");

    if(resTyp == DELETEtyp)
        printfParam("restype(Data Restore Type)","              DELETE");
    else if(resTyp == UPDATEtyp)
        printfParam("restype(Data Restore Type)","              UPDATE");

    printf("%s\t  ----------------------DropScan----------------------%s\n",COLOR_helpRestore,C_RESET);

    char dsoffstr[50]={0};
    sprintf(dsoffstr,"              %lld",dropScanSrtOff);
    printfParam("dsoff(DropScan startOffset)",dsoffstr);

    char blkintvalStr[10]={0};
    sprintf(blkintvalStr,"                        %d",blkInterval);
    printfParam("blkiter(Block Intervals)",dsoffstr);

    char itmsPerCsvStr[10]={0};
    sprintf(itmsPerCsvStr,"              %d",itemspercsv);
    printfParam("itmpcsv(Items Per Csv)",itmsPerCsvStr);
    char *isoModeStr= isoMode ? "              on":"              off";
    printfParam("isomode",isoModeStr);
    printf("%s└─────────────────────────────────────────────────────────────────┘%s\n",COLOR_PARAM,C_RESET);

}

void SET_PARAM(char *former,char *latter,char *third,char *fourth)
{
    int paramTyp;
    if(isParameter(latter,&paramTyp)){
        switch (paramTyp)
        {
        case 0:
            setSrtWalname(third);
            break;
        case 1:
            setEndWalname(third);
            break;
        case 2:
            setResmode(third);
            break;
        case 3:
            setTime(third,fourth,0);
            break;
        case 4:
            setTime(third,fourth,1);
            break;
        case 5:
            setExmode(third);
            break;
        case 6:
            setEncoding(third);
            break;
        case 7:
            setRestype(third);
            break;
        case 8:
            setEndLsnT(third);
            break;
        case 9:
            setStartLsnT(third);
            break;
        case 10:
            setIdxMode(third);
            break;
        case 11:
            setDropScanOff(third);
            break;
        case 12:
            setBlkIntval(third);
            break;
        case 13:
            setItmsPerCsv(third);
            break;
        default:
            break;
        }
    }
    else{
        ErrUnknownParam(latter);
        return;
    }
}

void RESET_ALL(){

    strcpy(manualSrtWal,"");
    strcpy(manualEndWal,"");

    restoreMode=periodRestore;
    setRestoreMode_there(periodRestore);

    *SrtTime=0;
    *EndTime=0;

    exmode=CSVform;
    setExportMode_decode(CSVform);
    setExportMode_there(CSVform);

    pduEncoding = UTF8encoding;
    setEncoding_there(UTF8encoding);

    resTyp = DELETEtyp;
    setResTyp_there(DELETEtyp);
    setResTyp_decode(DELETEtyp);
    memset(resStr,0,10);
    #ifdef CN
    sprintf(resStr,"%s","删除");
    #else
    sprintf(resStr,"%s","deleted");
    #endif
    dropScanSrtOff = 0;
    isoMode = 0;

    SHOW_PARAM();

}

void RESET_PARAM(char *former,char *latter,char *third)
{

    if(strcmp(latter,"all") == 0){
        RESET_ALL();
        return;
    }

    if(strcmp(third,"") != 0){
        ErrUnknownParam(third);
        return;
    }

    int paramTyp;
    if(isParameter(latter,&paramTyp)){
        switch (paramTyp)
        {
        case 0:
            strcpy(manualSrtWal,"");
            break;
        case 1:
            strcpy(manualEndWal,"");
            break;
        case 2:
            restoreMode=periodRestore;
            setRestoreMode_there(periodRestore);
            break;
        case 3:
            *SrtTime=0;
            break;
        case 4:
            *EndTime=0;
            break;
        case 5:
            exmode=CSVform;
            setExportMode_decode(CSVform);
            setExportMode_there(CSVform);
            break;
        case 6:
            pduEncoding = UTF8encoding;
            setEncoding_there(UTF8encoding);
            break;
        case 7:
            resTyp = DELETEtyp;
            setResTyp_there(DELETEtyp);
            setResTyp_decode(DELETEtyp);
            memset(resStr,0,10);
            #ifdef CN
            sprintf(resStr,"%s","删除");
            #else
            sprintf(resStr,"%s","deleted");
            #endif
        case 10:
            dropScanSrtOff = 0;
            break;
        case 11:
            isoMode = 0;
            break;
        default:
            break;
        }
        SHOW_PARAM();
    }
    else{
        ErrUnknownParam(latter);
        return;
    }

}

/*
 * dcProc - Process data page for DROPSCAN recovery
 * [Enterprise Edition Feature - Stub Implementation]
 */
int dcProc(int itemNum, dropContext *dc, char *page, int *consecutiveUnmatched, int *scanState, off_t *currOffset, int isIdx)
{
    (void)itemNum; (void)dc; (void)page;
    (void)consecutiveUnmatched; (void)scanState;
    (void)currOffset; (void)isIdx;
    return NOCALLBACK;
}

/*
 * dcProcforToast - Process TOAST data page for DROPSCAN recovery
 * [Enterprise Edition Feature - Stub Implementation]
 */
int dcProcforToast(int itemNum, dropContext *dc, char *page, int *consecutiveUnmatched, int *scanState, off_t *currOffset, int isIdx)
{
    (void)itemNum; (void)dc; (void)page;
    (void)consecutiveUnmatched; (void)scanState;
    (void)currOffset; (void)isIdx;
    return NOCALLBACK;
}

/*
 * dcProcforToastISO - Process TOAST data page for ISO mode DROPSCAN
 * [Enterprise Edition Feature - Stub Implementation]
 */
int dcProcforToastISO(int itemNum, dropContext *dc, char *page, int *consecutiveUnmatched, int *scanState, off_t *currOffset, int isIdx)
{
    (void)itemNum; (void)dc; (void)page;
    (void)consecutiveUnmatched; (void)scanState;
    (void)currOffset; (void)isIdx;
    return NOCALLBACK;
}

/*
 * dcProcIso - Process data page for ISO mode DROPSCAN recovery
 * [Enterprise Edition Feature - Stub Implementation]
 */
int dcProcIso(int itemNum, dropContext *dc, char *page, int *consecutiveUnmatched, int *scanState, off_t *currOffset, int isIdx)
{
    (void)itemNum; (void)dc; (void)page;
    (void)consecutiveUnmatched; (void)scanState;
    (void)currOffset; (void)isIdx;
    return NOCALLBACK;
}

/*
 * dropScanNoIdx - Disk scan without index (deprecated)
 * [Enterprise Edition Feature - Stub Implementation]
 */
void dropScanNoIdx(parray *dcs, int isToastRound)
{
    (void)dcs;
    (void)isToastRound;
}

/*
 * dcProcFinal - Final pass processing for DROPSCAN repair
 * [Enterprise Edition Feature - Stub Implementation]
 */
int dcProcFinal(int itemNum, dropContext *dc, char *page)
{
    (void)itemNum; (void)dc; (void)page;
    return NOCALLBACK;
}

/*
 * dropScanGenIdxThrds - Generate disk index using multiple threads
 *
 * NOTE: This is a stub implementation. The full functionality is
 * available in the Enterprise Edition.
 */
void dropScanGenIdxThrds(char *latter,harray *idxHash) {
    (void)latter;
    (void)idxHash;
}

/*
 * dropScanToast - Scan and recover TOAST table data
 *
 * NOTE: This is a stub implementation. The full functionality is
 * available in the Enterprise Edition.
 */
void dropScanToast(char *latter)
{
    (void)latter;
}

/*
 * dsForDatafileforIDX - Prepare datafile scan using disk index
 *
 * NOTE: This is a stub implementation. The full functionality is
 * available in the Enterprise Edition.
 */
parray *dsForDatafileforIDX(){
    return NULL;
}

/*
 * dsForDatafileISO - Prepare datafile scan from ISO image
 *
 * NOTE: This is a stub implementation. The full functionality is
 * available in the Enterprise Edition.
 */
parray *dsForDatafileISO(){
    return NULL;
}

/*
 * dsForToastforISO - Prepare TOAST scan from ISO image
 *
 * NOTE: This is a stub implementation. The full functionality is
 * available in the Enterprise Edition.
 */
void dsForToastforISO(parray *dcs)
{
    (void)dcs;
}

/*
 * dsForToastforIDX - Prepare TOAST scan using disk index
 *
 * NOTE: This is a stub implementation. The full functionality is
 * available in the Enterprise Edition.
 */
void dsForToastforIDX(parray *dcs)
{
    (void)dcs;
}

/*
 * dropScanIdx - Scan disk using pre-built index
 *
 * NOTE: This is a stub implementation. The full functionality is
 * available in the Enterprise Edition.
 */
void dropScanIdx(parray *dcs,int isToastRound)
{
    (void)dcs;
    (void)isToastRound;
}

/*
 * dropScanISO - Scan ISO image file for dropped table data
 *
 * NOTE: This is a stub implementation. The full functionality is
 * available in the Enterprise Edition.
 */
void dropScanISO(parray *dcs,int isToastRound)
{
    (void)dcs;
    (void)isToastRound;
}

/*
 * DROP_SCAN - Main entry point for DROPSCAN feature
 *
 * The DROPSCAN feature enables recovery of data from dropped PostgreSQL tables
 * by scanning disk sectors for orphaned data pages.
 *
 * NOTE: This is a stub implementation. The full DROPSCAN functionality
 * is available in the Enterprise Edition.
 *
 * For the full DROPSCAN functionality, please visit:
 * https://www.example.com/pdu
 */
void DROP_SCAN(char *latter,char *third) {
    (void)latter;
    (void)third;

    printf("\n");
    #ifdef CN
    printf("%s╔══════════════════════════════════════════════════════════════════╗%s\n", C_BLUE2, C_RESET);
    printf("%s║                 DROPSCAN - 专业版/企业版功能                      ║%s\n", C_BLUE2, C_RESET);
    printf("%s╠══════════════════════════════════════════════════════════════════╣%s\n", C_BLUE2, C_RESET);
    printf("%s║  DROPSCAN 磁盘扫描恢复功能可从已DROP的表中恢复数据。             ║%s\n", C_BLUE2, C_RESET);
    printf("%s║                                                                  ║%s\n", C_BLUE2, C_RESET);
    printf("%s║  此功能在专业版和企业版中提供，包含以下能力：                    ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan idx    - 生成磁盘数据页索引                        ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan        - 扫描恢复已DROP表的数据                    ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan iso    - 从镜像文件扫描恢复                        ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan repair - 修复TOAST表数据                           ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan clean  - 清理扫描结果目录                          ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan copy   - 生成COPY导入命令                          ║%s\n", C_BLUE2, C_RESET);
    printf("%s║                                                                  ║%s\n", C_BLUE2, C_RESET);
    printf("%s║  了解更多信息，请访问: %s%s\n", C_BLUE2, PDU_ENTERPRISE_URL, C_RESET);
    printf("%s╚══════════════════════════════════════════════════════════════════╝%s\n", C_BLUE2, C_RESET);
    #else
    printf("%s╔══════════════════════════════════════════════════════════════════╗%s\n", C_BLUE2, C_RESET);
    printf("%s║             DROPSCAN - Pro/Enterprise Feature                    ║%s\n", C_BLUE2, C_RESET);
    printf("%s╠══════════════════════════════════════════════════════════════════╣%s\n", C_BLUE2, C_RESET);
    printf("%s║  DROPSCAN enables recovery of data from dropped PostgreSQL       ║%s\n", C_BLUE2, C_RESET);
    printf("%s║  tables by scanning disk sectors.                                ║%s\n", C_BLUE2, C_RESET);
    printf("%s║                                                                  ║%s\n", C_BLUE2, C_RESET);
    printf("%s║  This feature is available in the Pro and Enterprise Edition:    ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan idx    - Generate disk page index                  ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan        - Scan and recover dropped table data       ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan iso    - Recover from ISO image file               ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan repair - Repair TOAST table data                   ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan clean  - Clean scan result directories             ║%s\n", C_BLUE2, C_RESET);
    printf("%s║    • dropscan copy   - Generate COPY import commands             ║%s\n", C_BLUE2, C_RESET);
    printf("%s║                                                                  ║%s\n", C_BLUE2, C_RESET);
    printf("%s║  For more information, please visit: %s%s\n", C_BLUE2, PDU_ENTERPRISE_URL, C_RESET);
    printf("%s╚══════════════════════════════════════════════════════════════════╝%s\n", C_BLUE2, C_RESET);
    #endif
    printf("\n");
}
/*
.toast中的File说明
1、dbf_fsm 主table中failed记录在dbf_tab中的位置，第一个Value是页号，第二个Value是元组号
2、dbf_tab 主table中包含toast的faileddata page集合
3、.toastoid 主table中解析failed的toast数据对应的toastoid
4、dbf 在ds toast操作中Scan出来的toasttable数据
5、dbf_idx toasttable数据的hashIndex，包括toastOid,chunkId,blk,toff,suffix四个Value
*/

/*
 * dropscanIsoDict - Build dictionary from ISO image for DROPSCAN
 *
 * NOTE: This is a stub implementation. The full functionality is
 * available in the Enterprise Edition.
 */
void dropscanIsoDict(char *phase)
{
    (void)phase;
}