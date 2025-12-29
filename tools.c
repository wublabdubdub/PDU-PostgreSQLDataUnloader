/*
 * PDU - PostgreSQL Data Unloader
 * Copyright (c) 2024-2025 ZhangChen
 *
 * Licensed under the Business Source License 1.1 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://mariadb.com/bsl11/
 *
 * Change Date: 2029-01-01
 * Change License: Apache License, Version 2.0
 *
 * See the License for the specific language governing permissions.
 */

#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <iconv.h>
#include <sys/socket.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <ifaddrs.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <ctype.h>
#include <stdbool.h>
#include <linux/fiemap.h>
#include <signal.h>
#include <execinfo.h>
#include <sys/resource.h>
#include "tools.h"
#include "basic.h"
#include <sys/statvfs.h>

#include <unistd.h>
#include <stdlib.h>
#include <string.h>

/**
 * crash_signal_handler - Signal handler for fatal signals
 *
 * @signo:    Signal number received
 * @info:     Signal information structure
 * @ucontext: User context (unused)
 *
 * Dumps a backtrace to stderr and exits when a fatal signal is received.
 * Avoids recursive dumps by calling _exit() instead of exit().
 */
static void crash_signal_handler(int signo, siginfo_t *info, void *ucontext)
{
	(void) ucontext;
	(void) info;

	const char prefix[] = "Fatal signal received, dumping backtrace:\n";
	write(STDERR_FILENO, prefix, sizeof(prefix) - 1);

	void *buffer[32];
	int frames = backtrace(buffer, 32);
	if (frames > 0)
		backtrace_symbols_fd(buffer, frames, STDERR_FILENO);

	_exit(128 + signo);
}

/**
 * setup_crash_handlers - Install signal handlers for fatal signals
 *
 * Sets up signal handlers for SIGSEGV, SIGABRT, SIGFPE, SIGILL, and SIGBUS
 * to dump backtraces on crashes. Also disables core dumps.
 */
void setup_crash_handlers(void)
{
	struct rlimit core_limit = {0, 0};
	setrlimit(RLIMIT_CORE, &core_limit);

	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_sigaction = crash_signal_handler;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_SIGINFO | SA_RESETHAND;

	int signals[] = {SIGSEGV, SIGABRT, SIGFPE, SIGILL, SIGBUS};
	for (size_t i = 0; i < sizeof(signals) / sizeof(signals[0]); i++)
		sigaction(signals[i], &sa, NULL);
}

void resetArray2Process(decodeFuncs *array2Process);
int AddList2Prcess(decodeFuncs *array2Process, char *type, char *BOOTTYPE);
void infoPhysicalBlkHeader(char *filename, uint32 st_size, int flag);
void infoPhysycalBlkContect(long long a, long long b, long long c, int i);
void infoUnlodResult(char *tabname, char *oidpath, int nPages, int nTotal, int nItemsSucc, int nItemsErr, char *csvpath);

#if defined(_WIN32)
    #include <windows.h>
#elif defined(__linux__)
    #include <unistd.h>
    #include <limits.h>
    #include <stdio.h>
    #include <stdlib.h>
    #include <dirent.h>
    #include <sys/resource.h>
#endif

#if defined(__i386__) || defined(__x86_64__)
#define EndOfFile -1
#elif defined(__aarch64__) || defined(__arm__)
#define EndOfFile 255
#endif

#define FIEMAP_FLAG_DEVICE_ORDER 0

int oldblk = 0;
int pduEncodingThere = UTF8encoding;

#define dropPrefix "........pg.dropped"

char *getVersionNum(char initDBPath[1024])
{
    FILE *file;
    char *version = NULL;
    char buffer[15];
    char versionfilepath[1050];
    sprintf(versionfilepath,"%s%s",initDBPath,"PG_VERSION");
    file = fopen(versionfilepath, "r");
    if (file == NULL) {
        perror("Failed to openFile");
        return "NoWayOut";
    }

    if (fgets(buffer, 15, file) != NULL) {
        version = strdup(buffer);
        if (version == NULL) {
            perror("Memory allocation failed");
            fclose(file);
            return "NoWayOut";
        }

        size_t len = strlen(version);
        if (len > 0 && version[len - 1] == '\n') {
            version[len - 1] = '\0';
        }
    } else {
        fprintf(stderr, "Failed to get Postgres version\n");
        fclose(file);
        return "NoWayOut";
    }

    fclose(file);

    return version;

}

int getLineNum(char *filename)
{
    FILE *file = fopen(filename, "r");
    int numLines = 0;
    char ch;
    while ((ch = fgetc(file)) != EndOfFile) {
        if (ch == '\n') {
            numLines++;
        }
        if (ch == '\r'){
            break;
        }
    }
    return numLines;
}

void trimLastValue(const char* str1, char* str2)
{
    int len1 = strlen(str1);
    int i;
    int j=0;
    for (i = 0; i < len1; i++) {
        if (i != len1 -1) {
            str2[j++] = str1[i];
        }
    }
    str2[j] = '\0';
}

void getStdTyp(char *str,char *ret)
{
    int size = sizeof(typmap_table)/sizeof(typmap_table[0]);
    int i;
    for ( i=0;i<size;i++ ){
        if ( strcmp(typmap_table[i].oriTyp,str) == 0 ){
            strcpy(ret, typmap_table[i].stdTyp);
            return;
        }
    }
    strcpy(ret, str);
}

/**
 * createDir - Create directory if not exists
 *
 * @path: Directory path to create
 *
 * Creates directory with standard permissions.
 *
 * Returns: 0 on success, -1 on failure
 */
int createDir(char *dirname)
{
    if( opendir(dirname) == NULL ){
        #if defined(_WIN32)
        mkdir(dirname);
        #elif defined(__linux__)
        mkdir(dirname,0755);
        #endif
        return 0;
    }
}

int removeEntry(const char *path) {
    struct stat statbuf;
    if (stat(path, &statbuf) == -1) {
        return -1;
    }

    if (S_ISDIR(statbuf.st_mode)) {
        DIR *dir;
        struct dirent *entry;
        char full_path[1024];

        dir = opendir(path);
        if (dir == NULL) {
            return -1;
        }

        while ((entry = readdir(dir)) != NULL) {
            snprintf(full_path, sizeof(full_path), "%s/%s", path, entry->d_name);

            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;

            if (removeEntry(full_path) == -1) {
                closedir(dir);
                return -1;
            }
        }

        closedir(dir);

        if (rmdir(path) == -1) {
            return -1;
        }
    } else {
        if (unlink(path) == -1) {
            return -1;
        }
    }

    return 0;
}

/**
 * removeDir - Recursively remove directory
 *
 * @path: Directory path to remove
 *
 * Removes directory and all its contents.
 *
 * Returns: 0 on success, -1 on failure
 */
int removeDir(const char *path) {
    return removeEntry(path);
}

void cleanDir(const char *path){
    removeDir(path);
    createDir((char *)path);
}

void getAttrTypForm(char attr[10240],char typ[10240],char attmod[10240])
{
    char attrArr[1665][100];
    int i=0;
    char typArr[1665][100];
    int j=0;
    char modArr[1665][100];
    int k=0;

    char *tokenAttr=strtok(attr, ",");
    while (tokenAttr != NULL){
        if(strcmp(tokenAttr,"dropped") == 0){
            tokenAttr = strtok(NULL, ",");
            continue;
        }
        strcpy(attrArr[i],tokenAttr);
        tokenAttr = strtok(NULL, ",");
        i++;
    }

    char *tokenTyp=strtok(typ, ",");
    while (tokenTyp != NULL){
        strcpy(typArr[j],tokenTyp);
        tokenTyp = strtok(NULL, ",");
        j++;
    }

    char *tokenMod=strtok(attmod, ",");
    while (tokenMod != NULL){
        strcpy(modArr[k],tokenMod);
        tokenMod = strtok(NULL, ",");
        k++;
    }

    if ( j == i && i == k){
        int x;
        for( x=0;x<i;x++){
            char *ddlcolname = quotedIfUpper(attrArr[x]);
            if (strcmp(typArr[x],"numeric") == 0 && strcmp(modArr[x],"()") != 0){
                char a[10]="";
                char b[10]="";
                char *token=strtok(modArr[x],".");
                strcpy(a,token);
                token=strtok(NULL,".");
                strcpy(b,token);
                if(x != i-1){
                    printf("\t%-25s%s%s,%s,\n",ddlcolname,typArr[x],a,b);
                }
                else{
                    printf("\t%-25s%s%s,%s\n",ddlcolname,typArr[x],a,b);
                }
            }
            else{
                if(x != i-1){
                    if(strcmp(modArr[x],"()") == 0){
                        printf("\t%-25s%s,\n",ddlcolname,typArr[x]);
                    }
                    else{
                        printf("\t%-25s%s%s,\n",ddlcolname,typArr[x],modArr[x]);
                    }
                }
                else{
                    if(strcmp(modArr[x],"()") == 0){
                        printf("\t%-25s%s\n",ddlcolname,typArr[x]);
                    }
                    else{
                        printf("\t%-25s%s%s\n",ddlcolname,typArr[x],modArr[x]);
                    }
                }
            }
        }
    }
}

void getAttrTypSimple(char res[10240],char typ[10240])
{

    char typArr[1665][100];
    int j=0;

    char *tokenTyp=strtok(typ, ",");
    while (tokenTyp != NULL){
        strcpy(typArr[j],tokenTyp);
        tokenTyp = strtok(NULL, ",");
        j++;
    }

    for(int x=0;x<j;x++){
        if(x != j-1){
            sprintf(res,"%s%s,",res,typArr[x]);
        }
        else{
            sprintf(res,"%s%s",res,typArr[x]);
        }
    }
}

void ata2DDL(char attr[10240],char typ[10240],char attmod[10240],FILE *ddl)
{
    char attrArr[1665][100];
    int i=0;
    char typArr[1665][100];
    int j=0;
    char modArr[1665][100];
    int k=0;

    char tmp[500]="";

    char *tokenAttr=strtok(attr, ",");
    while (tokenAttr != NULL){
        if(strcmp(tokenAttr,"dropped") == 0){
            tokenAttr = strtok(NULL, ",");
            continue;
        }
        strcpy(attrArr[i],tokenAttr);
        tokenAttr = strtok(NULL, ",");
        i++;
    }

    char *tokenTyp=strtok(typ, ",");
    while (tokenTyp != NULL){
        strcpy(typArr[j],tokenTyp);
        tokenTyp = strtok(NULL, ",");
        j++;
    }

    char *tokenMod=strtok(attmod, ",");
    while (tokenMod != NULL){
        strcpy(modArr[k],tokenMod);
        tokenMod = strtok(NULL, ",");
        k++;
    }

    if ( j == i && i == k){
        int x;
        for( x=0;x<i;x++){
            char *ddlcolname = quotedIfUpper(attrArr[x]);
            if(strcmp(modArr[x],"()") == 0){
                if(x == i-1){
                    sprintf(tmp,"\t%s %s\n",ddlcolname,typArr[x]);
                }
                else{
                    sprintf(tmp,"\t%s %s,\n",ddlcolname,typArr[x]);
                }
            }
            else{
                if (strcmp(typArr[x],"numeric") == 0){
                    char a[10]="";
                    char b[10]="";
                    char *token=strtok(modArr[x],".");
                    strcpy(a,token);
                    while (token != NULL){
                        token=strtok(NULL,".");
                        if(token != NULL){
                            strcpy(b,token);
                        }
                    }
                    if(x == i-1){
                        sprintf(tmp,"\t%s %s%s,%s\n",ddlcolname,typArr[x],a,b);
                    }
                    else{
                        sprintf(tmp,"\t%s %s%s,%s,\n",ddlcolname,typArr[x],a,b);
                    }
                }

                else if (strcmp(typArr[x],"varchar") == 0 || strcmp(typArr[x],"_varchar") == 0 ){
                    if(x == i-1){
                        sprintf(tmp,"\t%s %s\n",ddlcolname,typArr[x]);
                    }
                    else{
                        sprintf(tmp,"\t%s %s,\n",ddlcolname,typArr[x]);
                    }
                }
                else{
                   if(x == i-1){
                        sprintf(tmp,"\t%s %s%s\n",ddlcolname,typArr[x],modArr[x]);
                    }
                    else{
                        sprintf(tmp,"\t%s %s%s,\n",ddlcolname,typArr[x],modArr[x]);
                    }
                }
            }
            fputs(tmp,ddl);
        }
    }
}

void commaStrWriteIntoFileDB(char *str,FILE *file)
{
    #if PG_VERSION_NUM >= 17
	int oidnum=1;
	int datnamenum=2;
	int tbloidnum=12;
    #elif PG_VERSION_NUM <= 16 && PG_VERSION_NUM >= 15
	int oidnum=1;
	int datnamenum=2;
	int tbloidnum=11;
	#elif PG_VERSION_NUM == 14
	int oidnum=1;
	int datnamenum=2;
	int tbloidnum=13;
	#endif

    char tmpstr1[MiddleAllocSize]="";

    int colcount=1;
    char *token=strtok(str, "\t");
    while (token != NULL){
        char tmpstr2[100]="";
        strcpy(tmpstr2,token);
        removeSpaces(tmpstr2);
        if (colcount == oidnum){
            sprintf(tmpstr1, "%s%s", tmpstr2, "\t");
        }
        else if (colcount == datnamenum){
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\t");
        }
        else if (colcount == tbloidnum){
			strcat(tmpstr1,tmpstr2);
			strcat(tmpstr1,"\n");
            fputs(tmpstr1, file);
        }
        token = strtok(NULL, "\t");
        colcount++;
    }
}

void commaStrWriteIntoFileSCH_TYP(char *str,FILE *file)
{
    char tmpstr[MiddleAllocSize];
    char *token=strtok(str, "\t");

    sprintf(tmpstr, "%s%s", token, "\t");
    fputs(tmpstr, file);

    token = strtok(NULL, ",");
    sprintf(tmpstr, "%s", token);
    fputs(tmpstr, file);

    fputs("\n", file);
}

void AttrXman2AttrStrcut(char *src,ATTRstruct *oneattr)
{
    int colcount=1;
    char *token=strtok(src, "\t");
    while (token != NULL){
        char tmpstr2[MiddleAllocSize]="";
        strcpy(tmpstr2,token);
        removeSpaces(tmpstr2);
        if (colcount == 1)
        {
            strcpy(oneattr->relid,tmpstr2);
        }

        else if (colcount == 2){
            strcpy(oneattr->attr,tmpstr2);
        }

        else if (colcount == 3){
            strcpy(oneattr->typid,tmpstr2);
        }
        else if (colcount == 6){
            strcpy(oneattr->attrnum,tmpstr2);
            return;
        }
        colcount++;
        token = strtok(NULL, "\t");
    }
}

void setEncoding_there(int setting){
	pduEncodingThere = setting;
}

void OutputGBKString(const char *gbk_str, FILE *file) {
    iconv_t cd = iconv_open("UTF-8", "GBK");
    if (cd == (iconv_t)-1) {
        perror("iconv_open");
        return;
    }

    size_t in_len = strlen(gbk_str);
    size_t out_len = in_len * 4;
    char *out_buf = malloc(out_len);
    char *in_ptr = (char*)gbk_str;
    char *out_ptr = out_buf;

    if (iconv(cd, &in_ptr, &in_len, &out_ptr, &out_len) == (size_t)-1) {
        perror("iconv");
    } else {
        *out_ptr = '\0';
        fprintf(file, "%s", out_buf);
    }

    free(out_buf);
    iconv_close(cd);
}

void commaStrWriteIntoDecodeTab(char *str,FILE *file)
{
    strcat(str, "\n");

    if(pduEncodingThere == GBKencoding){
        OutputGBKString(str, file);
    }
    else{
        fputs(str, file);
    }

}

void genCopy(char *csvpath,FILE *copyfp){
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
    char path[10240];
    ssize_t len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    sprintf(fullPath,"%s/%s",path,csvpath);
    DIR *dir1;
    struct dirent *entry1;
    int arraySize=0;
    dir1 = opendir(csvpath);
    if (dir1 == NULL) {
        printf("Error opening directory<%s>",csvpath);
    }
    else{
        while ((entry1 = readdir(dir1)) != NULL) {

            if (entry1->d_type == 8) {
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
        sprintf(str2write,"COPY %s FROM '%s%s';\n",tabName,fullPath,filenames[i]);
        fputs(str2write,copyfp);
    }
}

void addQuotesToString(char *str)
{
    int len = strlen(str);
    int i;

    char *newStr = malloc(len + 3);
    if (newStr != NULL) {
        newStr[0] = '\'';
        strncpy(newStr + 1, str, len);
        newStr[len + 1] = '\'';
        newStr[len + 2] = '\0';
        strcpy(str, newStr);
        free(newStr);
    } else {
        printf("Memory allocation FAIL\n");
    }

}
#include <netpacket/packet.h>

int get_mac_address(char *mac_str) {
    struct ifaddrs *ifaddr, *ifa;
    int family;

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        return 1;
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        family = ifa->ifa_addr->sa_family;

        if (family == AF_PACKET && strcmp(ifa->ifa_name, "lo") != 0) {
            struct sockaddr_ll *sll = (struct sockaddr_ll *)ifa->ifa_addr;
            if (sll->sll_halen == 6) {
                sprintf(mac_str, "%02x:%02x:%02x:%02x:%02x:%02x",
                        sll->sll_addr[0], sll->sll_addr[1], sll->sll_addr[2],
                        sll->sll_addr[3], sll->sll_addr[4], sll->sll_addr[5]);
                freeifaddrs(ifaddr);
                return 0;
            }
        }
    }

    freeifaddrs(ifaddr);
    return 1;
}

int get_ip_address(char *ip_str) {
    struct ifaddrs *ifaddr, *ifa;
    int found = 0;

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        return 1;
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        int family = ifa->ifa_addr->sa_family;

        if (family == AF_INET && (ifa->ifa_flags & IFF_UP) && !(ifa->ifa_flags & IFF_LOOPBACK)) {
            struct sockaddr_in *sai = (struct sockaddr_in *)ifa->ifa_addr;
            char ip[INET_ADDRSTRLEN];

            if (inet_ntop(AF_INET, &sai->sin_addr, ip, sizeof(ip))) {
                if (strcmp(ip, "0.0.0.0") != 0) {
                    sprintf(ip_str,"%s-%s-%s-",ip,ip,ip);
                    found = 1;
                    break;
                }
            } else {
                perror("inet_ntop");
            }
        }
    }

    freeifaddrs(ifaddr);
    return found ? 0 : 1;
}

int get_system_uuid(char *uuid) {
    FILE *fp = popen("blkid -o value -s UUID $(df --output=source / | tail -1)", "r");
    if(!fp) return -1;

    if(fgets(uuid, 100, fp) == NULL) {
        pclose(fp);
        return -1;
    }

    uuid[strcspn(uuid, "\n")] = 0;
    pclose(fp);
    return 0;
}

int attrInDefaultATTR(char *a){
    const char *list[] = {
        "xmax",
        "cmax",
        "xmin",
        "cmin",
        "tableoid",
        "ctid",
        "rowid",
        "xc_node_id"
    };
    int list_size = sizeof(list) / sizeof(list[0]);
    int i;
    for (i = 0; i < list_size; i++) {
        if (strcmp(a, list[i]) == 0) {
            return 1;
        }
    }
    return 0;
}

int attrIsDropped(char *a)
{
    if(strncmp(a,dropPrefix,18) == 0){
        return 1;
    }
    return 0;
}

FILE* fileGetLines(char *filename,int *numlines)
{
    int charappreaed = 0;
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        return NULL;
    }
    char ch;
    while ((ch = fgetc(file)) != EndOfFile) {
        charappreaed = 1;
        if (ch == '\n') {
            *numlines = *numlines +1;
        }
        if (ch == '\r'){
            break;
        }
    }

    if(*numlines == 0 && charappreaed == 1)
        *numlines = *numlines +1;

    fseek(file, 0, SEEK_SET);
    return file;
}

int compare(const void *a, const void *b) {
    ATTRstruct *entryA = (ATTRstruct *)a;
    ATTRstruct *entryB = (ATTRstruct *)b;
    int numA = atoi(entryA->attrnum);
    int numB = atoi(entryB->attrnum);

    if (numA < 0 && numB >= 0) return 1;
    if (numA >= 0 && numB < 0) return -1;
    return numA - numB;
}

unsigned int hash(harray* harray, char *val, int allocatedVal)
{
    unsigned int hash = 0;

    for (int i = 0; i < 50; i++) {
        hash = (hash * 31 + val[i]) % allocatedVal;
        if (val[i] == '\0') break;
    }
    return hash;
}

harray *harray_new(int flag)
{
	harray *a = pgut_new(harray);

	a->table = NULL;
	a->used = 0;
	a->allocated = 0;

	harray_expand(a,flag, 1024);

	return a;
}

void harray_append(harray* harray, int flag, void *elem, uint64 val)
{
    if (harray->used + 1 > harray->allocated)
        harray_expand(harray, flag,harray->allocated * 2);

    char valStr[1000];
    sprintf(valStr,"%lld",val);

    unsigned int index = hash(harray,valStr,harray->allocated);
    Node* newNode = (Node*)malloc(sizeof(Node));

    if ( flag == HARRAYATTR ){
        newNode->data = (ATTRstruct *)elem;
    }
    else if ( flag == HARRAYDEL ){
        newNode->data = (DELstruct *)elem;
    }
    else if ( flag == HARRAYTAB ){
        newNode->data = (TABstruct *)elem;
    }
    else if ( flag == HARRAYINT){
        uint32 *copy = malloc(sizeof(uint32));
        *copy = val;
        newNode->data = copy;
    }
    else if ( flag == HARRAYLLINT){
        uint64 *copy = malloc(sizeof(uint64));
        *copy = val;
        newNode->data = copy;
    }
    else if ( flag == HARRAYTOAST ){
        newNode->data = (chunkInfo *)elem;
    }

    newNode->next = harray->table[index];
    harray->table[index] = newNode;
    harray->used++;
}

void harray_expand(harray *array,int flag, size_t newsize) {
    if (newsize <= array->allocated)
        return;

    Node** new_table = (Node**)calloc(newsize, sizeof(Node*));
    if (!new_table) {
        fprintf(stderr, "Memory allocation failed\n");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < array->allocated; i++) {
        Node* current = array->table[i];
        while (current) {
            unsigned int new_index;
            Node* next = current->next;
            if(flag == HARRAYATTR){
                new_index = hash(array, ((ATTRstruct*)current->data)->relid,newsize);
            }
            else if( flag == HARRAYTOAST){
                chunkInfo *a = (chunkInfo*)current->data;
                char val2pass[1000];
                sprintf(val2pass,"%d",a->toid);
                new_index = hash(array, val2pass,newsize);
            }
            else if ( flag == HARRAYINT){
                uint32 *a = (uint32 *)current->data;
                char val2pass[100]={0};
                sprintf(val2pass, "%" PRIu32, *a);
                new_index = hash(array, val2pass,newsize);
            }
            else if ( flag == HARRAYLLINT){
                uint64 *a = (uint64 *)current->data;
                char val2pass[100]={0};
                sprintf(val2pass, "%" PRIu64, *a);
                new_index = hash(array, val2pass,newsize);
            }
            else if ( flag == HARRAYDEL){
                DELstruct *a = (DELstruct*)current->data;
                char val2pass[1000];
                sprintf(val2pass,"%d",a->tx);
                new_index = hash(array, val2pass,newsize);
            }
            else if( flag == HARRAYTAB ){
                TABstruct *a = (TABstruct*)current->data;
                new_index = hash(array, a->oid,newsize);
            }
            current->next = new_table[new_index];
            new_table[new_index] = current;
            current = next;
        }
    }

    free(array->table);

    array->table = new_table;
    array->allocated = newsize;
}

int harray_search(harray* harray, int flag , uint64 val)
{
    char valStr[1000];
    sprintf(valStr,"%lld",val);
    unsigned int index = hash(harray, valStr,harray->allocated);
    Node* node = harray->table[index];
    int found=0;
    if(flag == HARRAYATTR){
        ATTRstruct* attrPtr = (ATTRstruct*)node->data;
        while (node != NULL) {
            if (strcmp(attrPtr->relid, valStr) == 0)
                found = 1;
            node = node->next;
        }
    }
    else if(flag == HARRAYINT){
        while (node != NULL) {
            uint32 *blk = (uint32*)(node->data);
            if (*blk==val){
                found =1;
                break;
            }
            node = node->next;
        }
    }
    else if(flag == HARRAYLLINT){
        while (node != NULL) {
            uint64 *blk = (uint64*)(node->data);
            if (*blk==val){
                found =1;
                break;
            }
            node = node->next;
        }
    }
    else if(flag == HARRAYDEL){
        while (node != NULL) {
            DELstruct* delPtr = (DELstruct*)node->data;
            if (delPtr->tx == val){
                found = 1;
                break;
            }
            node = node->next;
        }
    }
    return found;
}

void *harray_get(harray* harray, int flag , uint32 val)
{
    char valStr[1000];
    sprintf(valStr,"%lld",val);
    unsigned int index = hash(harray, valStr,harray->allocated);
    Node* node = harray->table[index];
    int found=0;
    if(flag == HARRAYDEL){
        while (node != NULL) {
            DELstruct* delPtr = (DELstruct*)node->data;
            if (delPtr->tx==val){
                return node->data;
            }
            node = node->next;
        }
    }
    return NULL;
}

size_t harray_num(const harray *array)
{
	return array!= NULL ? array->used : (size_t) 0;
}

void harray_free(harray* harray)
{
    for (int i = 0; i < harray_num(harray); i++) {
        Node* current = harray->table[i];
        while (current != NULL) {
            Node* next = current->next;
            free(current->data);
            free(current);
            current = next;
        }
    }
    free(harray->table);
    free(harray);
}

void getAttrUltra(harray *attr_harray,TYPstruct *typoid,int typoidlen,TABstruct *taboidTMP,int tabSize)
{
    char typStr[10240]="";
    char attrStr[10240]="";
    char modStr[2048]="";
    char lenStr[1024]="";
    char alignStr[1024]="";
    ATTRstruct *tempArray = NULL;

    int g=0;
    for( g=0 ; g < tabSize ; g++ ){
        int attrAppendCount=0;
        tempArray=malloc(10240 * sizeof(ATTRstruct));
        char *relid = taboidTMP[g].oid;

        unsigned int index = hash(attr_harray, relid, attr_harray->allocated);
        Node* node = attr_harray->table[index];

        while (node != NULL) {
            ATTRstruct* attrPtr = (ATTRstruct*)node->data;
            char *attr=attrPtr->attr;

            if (strcmp(attrPtr->relid, relid) == 0 && attrAppendCount < atoi(taboidTMP[g].nattr) && !attrInDefaultATTR(attr)){
                memcpy(&tempArray[attrAppendCount], attrPtr, sizeof(ATTRstruct));
                attrAppendCount++;
            }
            node = node->next;

            if ( attrAppendCount == atoi(taboidTMP[g].nattr) ){
                int x;
                qsort(tempArray, attrAppendCount, sizeof(ATTRstruct), compare);
                for (x=0; x<attrAppendCount; x++){

                    int isdrop = 0;
                    if(strncmp(tempArray[x].attr,dropPrefix,18) == 0)
                        isdrop =1;
                    if(isdrop){
                        memset(tempArray[x].attr,0,50);
                        sprintf(tempArray[x].attr,"dropped");
                    }

                    if(x == attrAppendCount -1){
                        strcat(attrStr,tempArray[x].attr);
                        strcat(typStr,tempArray[x].typid);
                        if(!isdrop)
                            strcat(modStr,tempArray[x].attrmod);
                        strcat(lenStr,tempArray[x].attlen);
                        strcat(alignStr,tempArray[x].attalign);
                    }
                    else if (x != 0){
                        strcat(attrStr,tempArray[x].attr);
                        strcat(attrStr,",");
                        strcat(typStr,tempArray[x].typid);
                        strcat(typStr,",");
                        if(!isdrop)
                            strcat(modStr,tempArray[x].attrmod);
                            strcat(modStr,",");
                        strcat(lenStr,tempArray[x].attlen);
                        strcat(lenStr,",");
                        strcat(alignStr,tempArray[x].attalign);
                        strcat(alignStr,",");

                    }
                    else{
                        sprintf(attrStr, "%s%s",tempArray[x].attr,",");
                        sprintf(typStr, "%s%s",tempArray[x].typid,",");
                        if(!isdrop)
                            sprintf(modStr, "%s%s",tempArray[x].attrmod,",");
                        sprintf(lenStr, "%s%s",tempArray[x].attlen,",");
                        sprintf(alignStr, "%s%s",tempArray[x].attalign,",");
                    }

                }

                char typStrArray[1024][10]={};
                char attrTyp[2048]="";
                int a=0;
                char *token = strtok(typStr, ",");
                while( token != NULL ) {
                    strcpy(typStrArray[a++],token);
                    token = strtok(NULL, ",");
                }
                int y;
                for ( y =0 ; y< a; y++){
                    int x;
                    for ( x = 0; x < typoidlen; x++ ){
                        if ( strcmp(typStrArray[y],typoid[x].oid ) == 0 ){
                            char *typnameret=NULL;
                            typnameret=(char *)malloc(sizeof(typoid[x].typname));
                            strcpy(typnameret,typoid[x].typname);
                            if( y == a-1 ){
                                sprintf(attrTyp, "%s%s",attrTyp,typnameret);
                                free(typnameret);
                                break;
                            }
                            else if ( y == 0 ){
                                sprintf(attrTyp, "%s%s",typnameret,",");
                                free(typnameret);
                                break;
                            }
                            else{
                                sprintf(attrTyp, "%s%s%s",attrTyp,typnameret,",");
                                free(typnameret);
                                break;
                            }
                        }
                    }
                }
                char *lenStrProced =NULL;
                lenStrProced = (char *)malloc(sizeof(char) * 65536);
                memset(lenStrProced,0,65536);
                processAttMod(attrTyp,modStr,lenStrProced);
                strcpy(taboidTMP[g].attr,attrStr);
                strcpy(taboidTMP[g].typ,attrTyp);
                strcpy(taboidTMP[g].attmod,lenStrProced);
                strcpy(taboidTMP[g].attlen,lenStr);
                strcpy(taboidTMP[g].attalign,alignStr);

                free(tempArray);
                attrAppendCount=0;
                free(lenStrProced);
                memset(typStr, 0, sizeof(typStr));
                memset(attrStr, 0, sizeof(attrStr));
                memset(modStr, 0, sizeof(modStr));
                memset(attrTyp, 0, sizeof(attrTyp));
                memset(lenStr, 0, sizeof(lenStr));
                memset(alignStr, 0, sizeof(alignStr));

                break;
            }
        }
    }
}

int getPgAttrDesc(TABstruct *taboid,pg_attributeDesc *allDesc)
{
    int dropExist = 0;
    int i=0;
    char attname[sizeof(taboid->attr)]={0};
    char atttyp[sizeof(taboid->typ)]={0};
    char attlen[sizeof(taboid->attlen)]={0};
    char attalign[sizeof(taboid->attalign)]={0};
    char attalignby[5]={0};

    strcpy(attname,taboid->attr);
    char *token = strtok(attname, ",");
    while (token != NULL) {
        if(strcmp(token,"dropped") == 0){
            dropExist=1;
        }
        memset(allDesc[i].attname,0,100);
        strcpy(allDesc[i].attname,token);
        token = strtok(NULL, ",");
        i++;
    }

    i=0;
    strcpy(atttyp,taboid->typ);
    char *token1 = strtok(atttyp, ",");
    while (token1 != NULL) {
        memset(allDesc[i].atttyp,0,100);
        while(strcmp(allDesc[i].attname,"dropped") == 0){
            i++;
        }
        strcpy(allDesc[i].atttyp,token1);
        token1 = strtok(NULL, ",");
        i++;
    }

    i=0;
    strcpy(attlen,taboid->attlen);
    char *token11 = strtok(attlen, ",");
    while (token11 != NULL) {
        memset(allDesc[i].attlen,0,10);
        strcpy(allDesc[i].attlen,token11);
        token11 = strtok(NULL, ",");
        i++;
    }

    i=0;
    strcpy(attalign,taboid->attalign);
    char *token2 = strtok(attalign, ",");
    while (token2 != NULL) {
        memset(allDesc[i].attalign,0,2);
        strcpy(allDesc[i].attalign,token2);
        token2 = strtok(NULL, ",");
        i++;
    }

    //         case TYPALIGN_INT:
    //         case TYPALIGN_CHAR:
    //         case TYPALIGN_DOUBLE:
    //         case TYPALIGN_SHORT:
    //         default:

    return dropExist;
}

void getTypForTrunc(harray *attr_harray,parray *GetTxRetAll,TYPstruct *typoid,int typoidlen,char *TxRequested,TRUNCstruct *targetTrunc)
{
    char typStr[10240]="";
    char attrStr[65536]="";
    ATTRstruct *tempArray = NULL;

    for(int y=0;y<parray_num(GetTxRetAll);y++){
        TRUNCstruct *elem=parray_get(GetTxRetAll,y);
        if(elem->tx == atoi(TxRequested)){
            targetTrunc->tx = elem->tx;
            targetTrunc->datafile = elem->datafile;
            targetTrunc->toast = elem->toast;
            targetTrunc->txtime = elem->txtime;
            break;
        }
    }
    int attrAppendCount=0;
    tempArray=malloc(500 * sizeof(ATTRstruct));

    unsigned int index = hash(attr_harray, TxRequested, attr_harray->allocated);
    Node* node = attr_harray->table[index];

    while (node != NULL) {
        ATTRstruct* onattroid = (ATTRstruct*)node->data;
        char *rel=onattroid->relid;
        char *attr=onattroid->attr;
        if ( atoi(rel)== targetTrunc->datafile && !attrInDefaultATTR(attr)){
            memcpy(&tempArray[attrAppendCount], onattroid, sizeof(ATTRstruct));
            attrAppendCount++;
        }
        node = node->next;
    }
    int x;
    qsort(tempArray, attrAppendCount, sizeof(ATTRstruct), compare);
    for (x=0; x<attrAppendCount; x++){
        if(x == attrAppendCount -1){
            strcat(attrStr,tempArray[x].attr);
            strcat(typStr,tempArray[x].typid);
        }
        else if (x != 0){
            strcat(attrStr,tempArray[x].attr);
            strcat(attrStr,",");
            strcat(typStr,tempArray[x].typid);
            strcat(typStr,",");
        }
        else{
            sprintf(attrStr, "%s%s",tempArray[x].attr,",");
            sprintf(typStr, "%s%s",tempArray[x].typid,",");
        }
    }
    char typStrArray[1024][10]={};
    char attrTyp[2048]="";
    int a=0;
    char *token = strtok(typStr, ",");
    while( token != NULL ) {
        strcpy(typStrArray[a++],token);
        token = strtok(NULL, ",");
    }
    int y;
    for ( y =0 ; y< a; y++){
        int x;
        for ( x = 0; x < typoidlen; x++ ){
            if ( strcmp(typStrArray[y],typoid[x].oid ) == 0 ){
                char *typnameret=NULL;
                typnameret=(char *)malloc(sizeof(typoid[x].typname));
                strcpy(typnameret,typoid[x].typname);
                if( y == a-1 ){
                    strcat(attrTyp,typnameret);
                    free(typnameret);
                    break;
                }
                else if ( y == 0 ){
                    sprintf(attrTyp, "%s%s",typnameret,",");
                    free(typnameret);
                    break;
                }
                else{
                    strcat(attrTyp,typnameret);
                    strcat(attrTyp,",");
                    free(typnameret);
                    break;
                }
            }
        }
    }
    targetTrunc->attribute=attrTyp;
    free(tempArray);
    attrAppendCount=0;
    memset(typStr, 0, sizeof(typStr));
    memset(attrStr, 0, sizeof(attrStr));

}

void processAttMod(const char attrTyp[],const char modStr[],char *ret)
{

    char *attrTypx = strdup(attrTyp);
    char *modStrx = strdup(modStr);

    char *tokenTyp;
    char *tokenLen;

    const char *delim = ",";

    parray *modarray = NULL;
    parray *typarray = NULL;
    modarray = parray_new();
    typarray = parray_new();

    tokenLen = strtok(modStrx, delim);
    while (tokenLen != NULL){
        char *x = malloc(10);
        memset(x,0,10);
        strcpy(x,tokenLen);
        parray_append(modarray,x);
        tokenLen = strtok(NULL, delim);
    }

    tokenTyp = strtok(attrTypx, delim);
    while (tokenTyp != NULL){
        char *y = malloc(100);
        memset(y,0,100);
        strcpy(y,tokenTyp);
        parray_append(typarray,y);
        tokenTyp = strtok(NULL, delim);
    }

    int i;
    for(i=0;i<parray_num(typarray);i++){
        char *Typ = parray_get(typarray,i);
        char *Len = parray_get(modarray,i);
        char *tmp=processAttModInner(Typ,Len);

        if(i == 0){
            sprintf(ret,"%s",tmp);
        }
        else{
            sprintf(ret,"%s,%s",ret,tmp);
        }
    }
    parray_free(modarray);
    parray_free(typarray);
}

char *processAttModInner(char *tokenTyp,char *tokenLen)
{
    static char tmp[50]="";
    int numLen = atoi(tokenLen);
    if(numLen == -1){
        sprintf(tmp,"()");
    }
    else if(strcmp(tokenTyp,"varchar") == 0 ||
            strcmp(tokenTyp,"char") == 0 ||
            strcmp(tokenTyp,"bpchar") == 0){
        numLen = numLen -4;
        sprintf(tmp,"(%d)",numLen);
    }
    else if(strcmp(tokenTyp,"numeric") == 0){
        int x=(numLen-4)/65536;
        int y=(numLen-4)%65536;
        sprintf(tmp,"(%d.%d)",x,y);
    }
    else if(strcmp(tokenTyp,"timestamp") == 0 || strcmp(tokenTyp,"timestamptz") == 0){
        sprintf(tmp,"(%d)",numLen);
    }
    return tmp;
}

int isParameter(char *value,int *type){
    const char *list[] = {
        "startwal",
        "endwal",
        "resmode",
        "starttime",
        "endtime",
        "exmode",
        "encoding",
        "restype",
        "endlsnt",
        "startlsnt",
        "isomode",
        "dsoff",
        "blkiter",
        "itmpcsv"
    };
    int list_size = sizeof(list) / sizeof(list[0]);
    int i;
    for (i = 0; i < list_size; i++) {
        if (strcmp(value, list[i]) == 0) {
            *type=i;
            return 1;
        }
    }
    return 0;
}

int schemaInDefaultSHCS(char *a){
    const char *list[] = {
        "blockchain",
        "cstore",
        "db4ai",
        "dbe_perf",
        "dbe_pldebugger",
        "dbe_pldeveloper",
        "dbe_sql_util",
        "dbms_alert",
        "dbms_application_info",
        "dbms_assert",
        "dbms_job",
        "dbms_lob",
        "dbms_lock",
        "dbms_pipe",
        "dbms_profiler_proctable",
        "dbms_random",
        "dbms_rowid",
        "dbms_scheduler",
        "dbms_session",
        "dbms_utility",
        "dbms_xplan",
        "pkg_service",
        "snapshot",
        "sqladvisor",
        "sys",
        "wmsys",
        "xmltype",
        "pg_catalog",
        "information_schema",
        "pg_toast",
        "audit",
        "pg_toast_temp_",
        "pg_temp_1",
        "pg_toast_temp_1",
        "anon",
        "dbms_sql",
        "perf",
        "src_restrict",
        "SYS_HM",
        "sysaudit",
        "sysmac",
        "xlog_record_read",
        "pg_bitmapindex",
        "sys_catalog",
        "sys_hm"
    };
    int list_size = sizeof(list) / sizeof(list[0]);
    int i;
    for (i = 0; i < list_size; i++) {
        if (strcmp(a, list[i]) == 0) {
            return 1;
        }
    }
    if(strncmp(a,"pg_temp",7) == 0 || strncmp(a,"pg_toast_temp",13) == 0)
        return 1;
    return 0;
}

void trim(char* str1, char* str2) {
    char *p1=str1;
    while ( *p1 && *p1 == '\n' ){
        p1++;
    }

    str1 = p1;

    size_t len1 = strlen(str1);
    size_t j = 0;
    size_t i;
    for (i = 0; i < len1; i++) {
        if (str1[i] != '\n') {
            str2[j++] = str1[i];
        }
        else if (str1[i] == '\n'){
            continue;
        }
    }
    str2[j] = '\0';
}

#define IS_DIR_SEP(ch) ((ch) == '/' || (ch) == '\\')
void get_parent_directory(char* path)
{
    trim_directory(path);
}

static char* skip_drive(const char* path)
{
    if (IS_DIR_SEP(path[0]) && IS_DIR_SEP(path[1])) {
        path += 2;
        while (*path && !IS_DIR_SEP(*path)) {
            path++;
        }
    } else if (isalpha((unsigned char)path[0]) && path[1] == ':') {
        path += 2;
    }
    return (char*)path;
}

static void trim_directory(char* path)
{
    char* p = NULL;

    path = skip_drive(path);

    if (path[0] == '\0') {
        return;
    }
    /* back up over trailing slash(es) */
    for (p = path + strlen(path) - 1; IS_DIR_SEP(*p) && p > path; p--)
        ;
    /* back up over directory name */
    for (; !IS_DIR_SEP(*p) && p > path; p--)
        ;
    /* if multiple slashes before directory name, remove 'em all */
    for (; p > path && IS_DIR_SEP(*(p - 1)); p--)
        ;
    /* don't erase a leading slash */
    if (p == path && IS_DIR_SEP(*p))
        p++;
    *p = '\0';
}

void removeSpaces(char *str) {
    if (str == NULL) {
        return;
    }

    char *src = str;
    char *dst = str;

    while (*src) {
        if (!isspace((unsigned char)*src)) {
            *dst++ = *src;
        }
        src++;
    }
    *dst = '\0';
}

void cleanPadding(const char *buffer, unsigned int *buff_size,int *padding,int *temppadding)
{
    *temppadding=0;
    while (*buffer == 0x00)
	{
		if (buff_size == 0)
			return;

		*buff_size = *buff_size -1;
		*buffer++;
        *temppadding = *temppadding +1;

	}
}

char *xman2Insertxman(char *xman,char *tablename)
{
    char *xmanRet = (char*)malloc(sizeof(char)*(strlen(xman)+250));
    memset(xmanRet,0,strlen(xman)+250);
    char *qoutedTablename=quotedIfUpper(tablename);
    sprintf(xmanRet,"INSERT INTO %s VALUES(%s);",qoutedTablename,xman);
    return xmanRet;
}

char *xman2Updatexman(parray *newxman_arr,parray *oldxman_arr,pg_attributeDesc *allDesc,char *tabname)
{
    int prefixsize = strlen(tabname)+50;
    int suffixsize = 10;
    int isSET = 0;
    int setConfronted = 1;
    for(int i=0;i<parray_num(newxman_arr);i++){
        char *new = parray_get(newxman_arr,i);
        char *old = parray_get(oldxman_arr,i);
        pg_attributeDesc *oneDesc = &allDesc[i];

        if(strcmp(old,new) != 0){
            prefixsize += strlen(oneDesc->attname);
            prefixsize += strlen(old);
        }

        suffixsize += strlen(new);
        suffixsize += strlen(oneDesc->attname);
        suffixsize += 10;
    }

    char *prefix =(char*)malloc(sizeof(char)*(suffixsize+prefixsize));
    char *suffix =(char*)malloc(sizeof(char)*suffixsize);
    char *samePrefix =(char*)malloc(sizeof(char)*suffixsize);
    memset(samePrefix,0,suffixsize);
    sprintf(prefix,"UPDATE %s SET ",tabname);
    sprintf(suffix," WHERE ");

    for(int i=0;i<parray_num(newxman_arr);i++){
        char *new = parray_get(newxman_arr,i);
        char *old = parray_get(oldxman_arr,i);

        pg_attributeDesc *oneDesc = &allDesc[i];

        strcat(suffix,oneDesc->attname);
        if(strcmp(new,"NULL") != 0){
            strcat(suffix,"=");
        }
        else{
            strcat(suffix," is ");
        }
        strcat(suffix,new);
        if(i != parray_num(newxman_arr)-1)
            strcat(suffix," AND ");
        if(strcmp(old,new) != 0){
            if(!setConfronted){
                strcat(prefix," AND ");
            }
            setConfronted = 0;
            strcat(prefix,oneDesc->attname);
            strcat(prefix,"=");
            strcat(prefix,old);
            isSET = 1;
        }
        if(!setConfronted){
            strcat(samePrefix," AND ");
        }
        setConfronted = 0;
        strcat(samePrefix,oneDesc->attname);
        strcat(samePrefix,"=");
        strcat(samePrefix,old);
    }
    char *ret = NULL;
    int yy=strlen(prefix)+strlen(suffix);
    if(!isSET){
        char *finalPrefix = (char*)malloc(sizeof(char)*(strlen(prefix)+strlen(samePrefix)+10));
        sprintf(finalPrefix,"%s%s",prefix,samePrefix);
        ret = (char*)malloc(strlen(finalPrefix)+strlen(suffix)+10);
        sprintf(ret,"%s\n%s;",finalPrefix,suffix);
    }
    else{
        ret = (char*)malloc(strlen(prefix)+strlen(suffix)+10);
        sprintf(ret,"%s\n%s;",prefix,suffix);
    }

    free(suffix);
    free(prefix);
    free(samePrefix);
    suffix=NULL;
    prefix=NULL;
    return ret;
}

void trim_char(char *str, char c) {
    if (!str || *str == '\0')
        return;

    char* start = str;
    while (*start == c) start++;

    if (*start == '\0') {
        *str = '\0';
        return;
    }

    char* end = str + strlen(str) - 1;
    while (end > start && *end == c) end--;

    size_t len = end - start + 1;

    if (start != str) {
        memmove(str, start, len);
    }
    str[len] = '\0';
}

void prepareRestore()
{
    removeDir("restore");
    createDir("restore");
    createDir("restore/public");
    createDir("restore/datafile");
    createDir("restore/.fpw");
    createDir("restore/.dsiso");
    createDir("restore/meta");
    createDir("restore/toastmeta");
    createDir("restore/dropscan");
    char pgSchemaPath[50]="restore/meta/pg_schema.txt";
    char pgPublicPath[50]="restore/meta/public_tables.txt";
    char pgDatabasePath[50]="pg_database.txt";
    char dropScanCfg[50]="restore/tab.config";

    FILE *pgSchemaFile = fopen(pgSchemaPath,"w");
    FILE *pgPublicFile = fopen(pgPublicPath,"w");
    FILE *pgDatabaseFile = fopen(pgDatabasePath,"a");
    FILE *dropScanCfgFile = fopen(dropScanCfg,"w");

    char str1[20]="2200\tpublic";
    commaStrWriteIntoFileSCH_TYP(str1,pgSchemaFile);

    char str2[50]="12345\trestore\t54321\trestore/datafile";
    commaStrWriteIntoFileSCH_TYP(str2,pgDatabaseFile);

    char str3[200]="employee varchar,varchar,json\ndepartment int,varchar,float";
    commaStrWriteIntoDecodeTab(str3,dropScanCfgFile);

    fclose(pgSchemaFile);
    fclose(pgPublicFile);
    fclose(pgDatabaseFile);
    fclose(dropScanCfgFile);
}

bool txInDelArrayOrNot(TransactionId Tx,parray *TxTime_parray,int *index,int restoreMode)
{
    if(parray_num(TxTime_parray) == 0){
        return false;
    }

    for(int i =0 ; i < parray_num(TxTime_parray);i++){
        DELstruct *elem = parray_get(TxTime_parray,i);
        if(Tx == elem->tx){
            *index = i;
            return true;
        }
    }
    return false;
}

/**
 * cleanNoTimeTxArray - Remove transactions without timestamp
 *
 * @array: Transaction array to clean
 *
 * Filters out transactions that have no associated timestamp.
 */
void cleanNoTimeTxArray(parray *array)
{
    int GetTxRetAllLen = parray_num(array);
    for (int x = 0; x < GetTxRetAllLen; x++){
        DELstruct *elem = parray_get(array,x);
        if(elem != NULL && elem->txtime == 0 ){
            parray_remove(array,x);
        }
    }
}

bool ifTxArrayAllWithTime(parray *array)
{
    int GetTxRetAllLen = parray_num(array);
    for (int x = 0; x < GetTxRetAllLen; x++){
        DELstruct *elem = parray_get(array,x);
        if(elem->txtime == 0 ){
            return false;
        }
    }
    return true;
}

int compare_walfile(const void *a, const void *b) {
    const WALFILE *fileA = (const WALFILE *)a;
    const WALFILE *fileB = (const WALFILE *)b;

    int cmp;

    cmp = strncmp(fileA->walnames, fileB->walnames, 8);
    if (cmp != 0) return cmp;

    cmp = strncmp(fileA->walnames + 8, fileB->walnames + 8, 8);
    if (cmp != 0) return cmp;

    return strncmp(fileA->walnames + 16, fileB->walnames + 16, 8);
}

int hexCharToInt(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

int hexStrToInt(const char* hexStr) {
    int result = 0;
    for (int i = 0; i < 8; i++) {
        result = result * 16 + hexCharToInt(hexStr[i]);
    }
    return result;
}

int countFilesBetween(const char* filename1, const char* filename2) {
    char mid1[9] = {0}, mid2[9] = {0};
    char hex1[9] = {0}, hex2[9] = {0};
    strncpy(mid1, filename1 + 8, 8);
    strncpy(mid2, filename2 + 8, 8);
    strncpy(hex1, filename1 + 16, 8);
    strncpy(hex2, filename2 + 16, 8);

    int midNum1 = hexStrToInt(mid1);
    int midNum2 = hexStrToInt(mid2);
    int hexNum1 = hexStrToInt(hex1);
    int hexNum2 = hexStrToInt(hex2);

    if (midNum1 == midNum2) {
        if (hexNum1 <= hexNum2) {
            return hexNum2 - hexNum1 - 1;
        } else {
            return -1;
        }
    } else {
        int midDiff = midNum2 - midNum1 - 1;
        int hexDiff = 256 - hexNum1 + hexNum2 - 1;
        return midDiff * 256 + hexDiff;
    }
}

void trimLeadingSpaces(char **str) {
    while (**str == ' ') {
        (*str)++;
    }
}

void trimSql(char *str) {
    char *end;
    while (isspace((unsigned char)*str)) str++;
    if (*str == 0) return;
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
}

bool txInTxArrayOrNot(TransactionId Tx,parray *Tx_parray,int restoreMode)
{
    if(restoreMode == TxRestore){
        for(int i =0 ; i < parray_num(Tx_parray);i++){
            DELstruct *elem = parray_get(Tx_parray,i);
            if(Tx == elem->tx){
                return true;
            }
        }
    }
    else if(restoreMode == periodRestore){
        DELstruct *elem = parray_get(Tx_parray,0);
        if(parray_contains(elem->Txs,(void *)(uintptr_t)Tx))
            return true;
    }
    return false;
}

void elemforTimeINIT(DELstruct *elem)
{
    memset(elem->startLSN,0,50);
	memset(elem->endLSN,0,50);
	elem->tx = 0;
	elem->delCount = 0;
	elem->txtime = 0;
	memset(elem->datafile,0,50);
	memset(elem->oldDatafile,0,50);
	memset(elem->toast,0,50);
	memset(elem->tabname,0,50);
	memset(elem->typ,0,10240);;
	elem->taboid = NULL;
	elem->Txs = NULL;
	memset(elem->startwal,0,70);
}

#define MAX_LINE_LEN 2048
#define MAX_COLUMNS 200
#define MAX_CONSTRAINT_KEYWORDS 30

const char *constraint_keywords[MAX_CONSTRAINT_KEYWORDS] = {
    "DEFAULT", "NOT", "NULL", "PRIMARY", "FOREIGN", "KEY",
    "UNIQUE", "CHECK", "COLLATE", "GENERATED", "AS", "STORED",
    "VIRTUAL", "REFERENCES", "CONSTRAINT", "AFTER", "BEFORE",
    "UNIQUE", "REFERENCES", "CONSTRAINT", "CHECK", "UNIQUE"
};

typedef struct {
    char name[256];
    char type[20];
    char length[10];
} Column;

int is_constraint_keyword(const char *word) {
    for (int i = 0; i < MAX_CONSTRAINT_KEYWORDS; i++) {
        if (constraint_keywords[i] == NULL) break;
        if (strcasecmp(word, constraint_keywords[i]) == 0) {
            return 1;
        }
    }
    return 0;
}

void process_type_part(char *type_part, char *col_type, char *col_length) {
    char *paren = strchr(type_part, '(');
    if (paren) {
        *paren = '\0';
        char *length_part = paren + 1;
        char *close_paren = strchr(length_part, ')');
        if (close_paren) {
            *close_paren = '\0';
            if(strcmp(type_part,"numeric") == 0){
                for(int x=0;x<strlen(length_part);x++){
                    if(length_part[x] == ',')
                    length_part[x] = '.';
                }
            }
            strncpy(col_length, length_part, 255);
            col_length[255] = '\0';
        } else {
            strcpy(col_length, "");
        }
    } else {
        strcpy(col_length, "");
    }

    if(type_part[strlen(type_part)-1] == ','){
        type_part[strlen(type_part)-1]='\0';
    }

    char lower_type[256] = {0};
    strncpy(lower_type, type_part, 255);
    for (int i = 0; lower_type[i]; i++) {
        lower_type[i] = tolower(lower_type[i]);
    }

    if (strstr(lower_type, "character varying") != NULL ||
        (strstr(lower_type, "character") != NULL && strstr(lower_type, "varying") != NULL)) {
        strcpy(col_type, "varchar");
    } else if (strstr(lower_type, "character") != NULL) {
        strcpy(col_type, "char");
    } else if (strstr(lower_type, "timestamp without time zone") != NULL) {
        strcpy(col_type, "timestamp");
    } else if (strstr(lower_type, "timestamp with time zone") != NULL) {
        strcpy(col_type, "timestamptz");
    } else {
        strcpy(col_type, type_part);
    }
}

void parse_column_definition(const char *def, Column *col) {
    trimLeadingSpaces((char **)&def);
    char *col_name_end = strchr(def, ' ');
    if (!col_name_end) return;
    *col_name_end = '\0';
    strncpy(col->name, def, 255);
    col->name[255] = '\0';

    char *type_part = col_name_end + 1;
    char *words[100];
    int num_words = 0;
    char *token = strtok(type_part, " ");
    while (token && num_words < 100) {
        words[num_words++] = token;
        token = strtok(NULL, " ");
    }

    int type_words = 0;
    for (; type_words < num_words; type_words++) {
        char lower_word[256];
        strncpy(lower_word, words[type_words], 255);
        for (int i = 0; lower_word[i]; i++) lower_word[i] = tolower(lower_word[i]);
        if (is_constraint_keyword(lower_word)) break;
    }

    char combined[1024] = "";
    for (int i = 0; i < type_words; i++) {
        if (i > 0) strcat(combined, " ");
        strcat(combined, words[i]);
    }

    process_type_part(combined, col->type, col->length);
}

void getMetaFromManul(char *sqlPath,char *CUR_DB){
    int ddlCnt = 0;
    char filename[200];
    const char *lastSlash = strrchr(sqlPath, '/');
    const char *dot = strchr(sqlPath, '.');
    if (lastSlash != NULL && dot != NULL && lastSlash < dot) {
        size_t len = dot - lastSlash - 1;
        strncpy(filename, lastSlash + 1, len);
        filename[len] = '\0';
    } else {
        filename[0] = '\0';
    }

    char manualTxtPath[300];
    sprintf(manualTxtPath,"%s/manual/%s.txt",CUR_DB,filename);
    FILE *manualTxt = fopen(manualTxtPath,"w");
    FILE *file = fopen(sqlPath, "r");
    if (!file) {
        perror("Failed to open file");
        return;
    }
    char line[500];
    int inside_create_table = 0;
    int col_count;
    Column cols[MAX_COLUMNS];
    char tabname[256];
    while (fgets(line, sizeof(line), file)) {

        if (strncmp(line, "CREATE TABLE", 12) == 0) {
            memset(tabname,0,256);

            char *tab = strtok(line," ");
            tab = strtok(NULL," ");
            tab = strtok(NULL," ");
            strcpy(tabname,tab);

            inside_create_table = 1;
            col_count = 0;
            continue;
        }

        if (inside_create_table) {
            if (line[0] == ')' && line[1] == ';') {
                inside_create_table = 0;
                char record[col_count*256+col_count*20+col_count*10];
                memset(record,0,col_count*256+col_count*20+col_count*10);

                char attStr[col_count*256];
                char typStr[col_count*20];
                char modStr[col_count*10];
                memset(attStr,0,col_count*256);
                memset(typStr,0,col_count*20);
                memset(modStr,0,col_count*10);

                for (int i = 0; i < col_count; i++) {
                    sprintf(attStr,"%s%s",attStr, cols[i].name);
                    sprintf(typStr,"%s%s",typStr, cols[i].type);
                    sprintf(modStr,"%s(%s)",modStr, cols[i].length);
                    if (i < col_count - 1) {
                        sprintf(attStr,"%s,",attStr);
                        sprintf(typStr,"%s,",typStr);
                        sprintf(modStr,"%s,",modStr);
                    }
                }
                sprintf(record,"<oid>\t<filenode>\t<toastoid>\t<toastnode>\t<namespace>\t%s\t%s\t%s\t%d\t%s\tUNKNOWN\tUNKNOWN\n",tabname,attStr,typStr,col_count,modStr);
                fputs(record,manualTxt);
                ddlCnt++;
                continue;
            }
            trimSql(line);
            parse_column_definition(line, &cols[col_count]);
            col_count++;
        }
        memset(line,0,500);
    }
    printf("Scan complete, %d objects found, file path: %s\n",ddlCnt,manualTxtPath);
    fclose(manualTxt);
    fclose(file);
}

parray *getfilenameParray(char *path2search)
{
    int PATHSIZE=1024;
    char currPath[PATHSIZE];
    char tabName[PATHSIZE];
    char fullPath[2*PATHSIZE];
    int file_count = 0;
    parray *ret=parray_new();

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
    sprintf(fullPath,"%s/%s/*",currPath,path2search);
    WIN32_FIND_DATA findFileData;
    HANDLE hFind = FindFirstFile(fullPath, &findFileData);

    if (hFind != INVALID_HANDLE_VALUE) {
        do {
            if (!(findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
                char filename[MAXPGPATH];
                strcpy(filename,findFileData.cFileName);
                parray_append(ret,filename);
            }
        } while (FindNextFile(hFind, &findFileData) != 0);

        FindClose(hFind);
    }
    size_t len1 = strlen(fullPath);
    fullPath[len1-1]='\0';

#elif defined(__linux__)
    DIR *dir1;
    struct dirent *entry1;
    dir1 = opendir(path2search);
    if (dir1 == NULL) {
        #ifdef EN
        printf("directory <%s> does not exist,please <unload> first\n",path2search);
        #else
        printf("Path <%s> does not exist\n",path2search);
        #endif
        return ret;
    }
    else{
        while ((entry1 = readdir(dir1)) != NULL) {
            if (entry1->d_type == 8) {
                char *filename = (char*)malloc(sizeof(char)*100);
                strcpy(filename,entry1->d_name);
                char *suffix = strrchr(filename, '.');
                if(strcmp(suffix,".sql") == 0){
                    parray_append(ret,filename);
                }

            }
        }
    }
#endif
    return ret;
}

char *hexBuffer2Str(const char *buff, char *hexBuffer,int size) {
    int i;
    for (i = 0; i < size; i++) {
        char a[3];
        unsigned char b = buff[i];
        sprintf(a, "%02X", b);
        strcat(hexBuffer,a);
    }

    hexBuffer[size * 2] = '\0';

    return hexBuffer;
}

void printfParam(char *a,char *b){
    printf("%s│    %-30s│%-30s│%s\n",COLOR_PARAM,a,b,C_RESET);
}

int* findMinMaxNumbers(const char* path)
{
    int min = -1, max = -1;
    DIR* dir;
    struct dirent* entry;

    dir = opendir(path);
    if (dir == NULL) {
        perror("Failed to openDirectory");
        return NULL;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        char* endptr;
        long num = strtol(entry->d_name, &endptr, 10);

        if (*endptr != '\0') {
            continue;
        }

        if (min == -1 || num < min) {
            min = num;
        }
        if (max == -1 || num > max) {
            max = num;
        }
    }

    closedir(dir);

    if (min == -1 || max == -1) {
        return NULL;
    }

    int* result = (int*)malloc(2 * sizeof(int));
    if (result == NULL) {
        perror("Memory allocation failed");
        return NULL;
    }
    result[0] = min;
    result[1] = max;

    return result;
}

void mergeToast(char *PATH,char *toastfile)
{
    char toastPATH[100];
    sprintf(toastPATH,"%s/%s",PATH,toastfile);

    int* result = findMinMaxNumbers(PATH);

    DIR *dir;
    struct dirent *entry;
    FILE *outputFile, *inputFile;
    char buffer[BLCKSZ];
    size_t bytesRead;
    char filePath[256];

    if(result == NULL){
        return;
    }

    outputFile = fopen(toastPATH, "wb");
    if (outputFile == NULL) {
        perror("Failed to openOutputFile");
        closedir(dir);
        return;
    }
    for (int x=0;x<result[1]+1;x++){
        snprintf(filePath, sizeof(filePath), "%s/%d", PATH,x);
        struct stat fileStat;
        if (stat(filePath, &fileStat) == -1) {
            continue;
        }

        if (fileStat.st_size != BLCKSZ) {
            continue;
        }

        inputFile = fopen(filePath, "rb");
        if (inputFile == NULL) {
            perror("Failed to openInputFile");
            continue;
        }

        while ((bytesRead = fread(buffer, 1, BLCKSZ, inputFile)) > 0) {
            fwrite(buffer, 1, bytesRead, outputFile);
        }

        fclose(inputFile);

        if (remove(filePath) != 0) {
            perror("Failed to delete file");
        }

    }
    fclose(outputFile);
}

char* quotedIfUpper(const char* input){
    int len = strlen(input);
    int hasUpper = 0;

    for (int i = 0; i < len; i++) {
        if (isupper(input[i])) {
            hasUpper = 1;
            break;
        }
    }

    char* result;
    if (hasUpper) {
        result = (char*)malloc(len + 3);
        if (result == NULL) {
            printf("Memory allocation failed.\n");
            exit(1);
        }
        sprintf(result, "\"%s\"", input);
    } else {
        result = (char*)malloc(len + 1);
        if (result == NULL) {
            printf("Memory allocation failed.\n");
            exit(1);
        }
        strcpy(result, input);
    }

    return result;
}

/* copied from timestamp.c */
pg_time_t timestamptz_to_time_t_og(TimestampTz t)
{
    pg_time_t result;

    result = (pg_time_t)(t / USECS_PER_SEC + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));

    return result;
}

uintptr_t timestamptz_to_str_og(TimestampTz dt)
{
	char *buf = (char*)malloc(sizeof(char)*1024);
	char		ts[MAXDATELEN + 1];
	char		zone[MAXDATELEN + 1];
	time_t		result = (time_t) timestamptz_to_time_t_og(dt);
	struct tm  *ltime = localtime(&result);

	strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", ltime);
	strftime(zone, sizeof(zone), "%Z", ltime);

    sprintf(buf, "%s.%06d %s", ts, (int) (dt % USECS_PER_SEC), zone);

    return (uintptr_t)buf;
}

#define VERSIONTYPE "PG_VERSION"

int getPGVersion(char *PGDATA){
    char exePath[PATH_MAX];
    ssize_t exeLen = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
    if (exeLen > 0) {
        exePath[exeLen] = '\0';
        char *lastSlash = strrchr(exePath, '/');
        if (lastSlash != NULL) {
            size_t dirLen = lastSlash - exePath;
            char exeDir[PATH_MAX];
            if (dirLen < sizeof(exeDir)) {
                memcpy(exeDir, exePath, dirLen);
                exeDir[dirLen] = '\0';
                char exePgVersionPath[PATH_MAX];
                snprintf(exePgVersionPath, sizeof(exePgVersionPath), "%s/%s", exeDir, VERSIONTYPE);
                if (access(exePgVersionPath, F_OK) == 0) {
                    #ifdef CN
                    printf("%s请不要在PGDATA目录下执行pdu%s\n", COLOR_ERROR, C_RESET);
                    #else
                     printf("%splease do not execute pdu in PGDATA directory%s\n", COLOR_ERROR, C_RESET);
                    #endif
                    return FAILURE_RET;
                }
            }
        }
    }

    char pg_versionPath[strlen(PGDATA)+100];
    char versionNumStr[10];
    memset(versionNumStr,0,10);
    sprintf(pg_versionPath,"%s/%s",PGDATA,VERSIONTYPE);
    FILE *fp = fopen(pg_versionPath,"r");

    if(fp == NULL){
        printf("%sPGDATA path does not contain %s file%s\n",COLOR_ERROR,VERSIONTYPE,C_RESET);
        return FAILURE_RET;
    }
    fread(&versionNumStr, 1, 10, fp);
    int versionNum = atoi(versionNumStr);
    if(versionNum != PG_VERSION_NUM){
        printf("%sPDU version does not match database version%s\n",COLOR_ERROR,C_RESET);
        return FAILURE_RET;
    }
    return SUCCESS_RET;
}

void replace_improper_symbols(char* s) {
    if (s == NULL) return;

    char* start_quote = strchr(s, '\'');
    char* end_quote = strrchr(s, '\'');

    if (!start_quote || !end_quote || start_quote >= end_quote) {
        return;
    }

    size_t count = 0;
    for (char* p = s; *p; p++) {
        if (*p == '\'' && p != start_quote && p != end_quote) {
            count++;
        }
    }

    if (count == 0) {
        return;
    }

    size_t original_len = strlen(s);
    size_t new_len = original_len + count;

    char* p_orig = s + original_len - 1;
    char* q = s + new_len;
    *q = '\0';
    q--;

    while (p_orig >= s) {
        if (*p_orig == '\'') {
            if (p_orig == start_quote || p_orig == end_quote) {
                *q-- = *p_orig--;
            } else {
                *q-- = '\'';
                *q-- = '\'';
                p_orig--;
            }
        } else {
            *q-- = *p_orig--;
        }
    }
}

bool shortCMDMatched(char *cmd){
    if(strncmp(cmd,"u ",2) == 0)
        return true;
    else if(strncmp(cmd,"p ",2) == 0)
        return true;
    else if(strcmp(cmd,"t") == 0)
        return true;
    else if(strncmp(cmd,"i ",2) == 0)
        return true;
    else{
        return false;
    }
}

int countCommas(const char *str)
{
    if (str == NULL) return 0;

    int count = 0;
    while (*str) {
        if (*str == ',') count++;
        str++;
    }
    return count;
}

int compare_hex(const char *s1, const char *s2) {
    const char *start1 = s1;
    while (*start1 == '0') {
        start1++;
    }
    if (*start1 == '\0') {
        start1 = s1 + strlen(s1) - 1;
    }

    const char *start2 = s2;
    while (*start2 == '0') {
        start2++;
    }
    if (*start2 == '\0') {
        start2 = s2 + strlen(s2) - 1;
    }

    int len1 = strlen(start1);
    int len2 = strlen(start2);

    if (len1 > len2) {
        return 1;
    } else if (len1 < len2) {
        return -1;
    } else {
        for (int i = 0; i < len1; ++i) {
            char c1 = toupper(start1[i]);
            char c2 = toupper(start2[i]);
            int val1 = (c1 >= 'A') ? (c1 - 'A' + 10) : (c1 - '0');
            int val2 = (c2 >= 'A') ? (c2 - 'A' + 10) : (c2 - '0');
            if (val1 > val2) {
                return 1;
            } else if (val1 < val2) {
                return -1;
            }
        }
        return 0;
    }
}

int compare_prefix(const char *a, int a_len, const char *b, int b_len) {
    int min_len = a_len < b_len ? a_len : b_len;
    int cmp = strncmp(a, b, min_len);
    if (cmp != 0) {
        return cmp;
    } else {
        if (a_len > b_len) {
            return 1;
        } else if (a_len < b_len) {
            return -1;
        } else {
            return 0;
        }
    }
}

bool compareHexStrings(const char *a, const char *b) {
    const char *slash_a = strchr(a, '/');
    const char *slash_b = strchr(b, '/');

    if (!slash_a || !slash_b) {
        return false;
    }

    int a_prefix_len = slash_a - a;
    int b_prefix_len = slash_b - b;

    int prefix_cmp = compare_prefix(a, a_prefix_len, b, b_prefix_len);
    if (prefix_cmp > 0) {
        return true;
    } else if (prefix_cmp < 0) {
        return false;
    } else {
        const char *hex_a = slash_a + 1;
        const char *hex_b = slash_b + 1;
        int hex_cmp = compare_hex(hex_a, hex_b);
        return hex_cmp > 0;
    }
}

#define LSN_FORMAT_ARGS(lsn)  (uint32) ((lsn) >> 32), ((uint32) (lsn))

bool lsnIsReached(uint64 pre,uint64 suff,char *endLSN)
{
    char currentLSN[50];
	sprintf(currentLSN,"%X/%08X",LSN_FORMAT_ARGS(pre),LSN_FORMAT_ARGS(suff));
	int lsnReached = compareHexStrings(currentLSN,endLSN);
    bool ret = lsnReached ? true : false;
    return ret;
}

#include <sys/stat.h>

static int is_directory(const char *path) {
    struct stat statbuf;
    if (stat(path, &statbuf) != 0)
        return 0;
    return S_ISDIR(statbuf.st_mode);
}

bool find_unique_path(const char *a, const char *dboid, char **result_path) {
    DIR *dir;
    struct dirent *entry;
    char temp_path[PATH_MAX];
    int count = 0;
    char *found_path = NULL;

    *result_path = NULL;

    dir = opendir(a);
    if (!dir) return false;

    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        snprintf(temp_path, sizeof(temp_path), "%s/%s/%s", a, entry->d_name, dboid);

        if (is_directory(temp_path)) {
            if (count == 0) {
                found_path = strdup(temp_path);
                if (!found_path) {
                    closedir(dir);
                    return false;
                }
                count = 1;
            } else {
                free(found_path);
                found_path = NULL;
                count++;
                break;
            }
        }
    }
    closedir(dir);

    if (count == 1) {
        *result_path = found_path;
        return true;
    } else {
        free(found_path);
        return false;
    }
}

void loadTbspc(DBstruct *databaseoid,int dbsize,char *PGDATA)
{
    char sourcepath[MAXPGPATH];
    char targetpath[MAXPGPATH];
	int rllen;
	struct stat st;

    for( int i = 0 ; i < dbsize ; i++){
        if(strcmp(databaseoid[i].database,"restore") == 0){
            continue;
        }

        memset(sourcepath,0,MAXPGPATH);
        memset(targetpath,0,MAXPGPATH);

        char *tbloid = databaseoid[i].tbloid;
        char *dboid = databaseoid[i].oid;

        snprintf(sourcepath, sizeof(sourcepath), "%spg_tblspc/%s",PGDATA, tbloid);

        /*
        * Before reading the link, check if the source path is a link or a
        * junction point.  Note that a directory is possible for a tablespace
        * created with allow_in_place_tablespaces enabled.  If a directory is
        * found, a relative path to the data directory is returned.
        */
        if (lstat(sourcepath, &st) < 0)
        {
            char dbpath[MAXPGPATH];
            sprintf(dbpath,"%sbase/%s",PGDATA,dboid);
            strcpy(databaseoid[i].dbpath,dbpath);
            continue;

        }

        /*
        * In presence of a link or a junction point, return the path pointing to.
        */
        rllen = readlink(sourcepath, targetpath, sizeof(targetpath));

        targetpath[rllen] = '\0';
        char *dbpath;
        if(!find_unique_path(targetpath,dboid,&dbpath)){
            printf("%sTable space issue, please contact support%s",COLOR_WARNING,C_RESET);
            char dbpath_fail[MAXPGPATH];
            sprintf(dbpath_fail,"%s%s",PGDATA,dboid);
            strcpy(databaseoid[i].dbpath,dbpath_fail);
            continue;
        }
        else{
            strcpy(databaseoid[i].dbpath,dbpath);
        }
    }
}

void dboidWithTblIntoFile(DBstruct *databaseoid,int dosize,char *filepath){
    FILE *file = fopen(filepath,"w");
    if(file == NULL){
        return;
    }
    for(int i=0;i<dosize;i++){
        if(strcmp(databaseoid[i].database,"restore") == 0){
            char str[50]="12345\trestore\t54321\trestore/datafile\n";
            fputs(str,file);
            continue;
        }
        int size = strlen(databaseoid[i].oid)+strlen(databaseoid[i].database)+
                   strlen(databaseoid[i].tbloid)+strlen(databaseoid[i].dbpath);
        char ret[size+10];
        sprintf(ret,"%s\t%s\t%s\t%s\n",databaseoid[i].oid,databaseoid[i].database,databaseoid[i].tbloid,databaseoid[i].dbpath);
        fputs(ret,file);
    }
    fclose(file);
}

int chunkInfo_compare(const void *a, const void *b) {
    const chunkInfo *infoA = *(const chunkInfo **)a;
    const chunkInfo *infoB = *(const chunkInfo **)b;

    if (infoA->chunkid < infoB->chunkid) return -1;
    if (infoA->chunkid > infoB->chunkid) return 1;
    return 0;
}

int compare_blk(const void *a, const void *b) {
    const chunkInfo *ci_a = *(const chunkInfo **)a;
    const chunkInfo *ci_b = *(const chunkInfo **)b;
    if (ci_a->blk < ci_b->blk) return -1;
    if (ci_a->blk > ci_b->blk) return 1;
    return 0;
}

int compare_chunkid(const void *a, const void *b) {
    const chunkInfo *ci_a = *(const chunkInfo **)a;
    const chunkInfo *ci_b = *(const chunkInfo **)b;
    if (ci_a->chunkid < ci_b->chunkid) return -1;
    if (ci_a->chunkid > ci_b->chunkid) return 1;
    return 0;
}

int compare_tx(const void *a, const void *b) {
    const TransactionId *ci_a = *(const TransactionId **)a;
    const TransactionId *ci_b = *(const TransactionId **)b;
    if (ci_a < ci_b) return -1;
    if (ci_a > ci_b) return 1;
    return 0;
}

bool ends_with(const char *str, const char *suffix) {
    if (!str || !suffix) return false;

    size_t len_str = strlen(str);
    size_t len_suffix = strlen(suffix);

    if (len_suffix > len_str) return false;
    bool ret = strncmp(str + len_str - len_suffix, suffix, len_suffix) == 0 ? 1:0;
    return ret;
}

uint32_t compute_block_hash(const char* block) {
    PageHeader p = (PageHeader)block;
    LocationIndex lower = p->pd_lower;
    LocationIndex upper = p->pd_upper;
    uint32 signature = p->pd_lsn.xrecoff+
                  p->pd_lsn.xlogid+
                  lower*upper;
    const uint32_t FNV_offset_basis = 2166136261U;
    const uint32_t FNV_prime = 16777619U;

    uint32_t hash = FNV_offset_basis;
    const unsigned char* data = (const unsigned char*)block;

    for (size_t i = 0; i < 1024; ++i) {
        hash ^= data[i];
        hash *= FNV_prime;
    }

    const size_t end_start = 8192 - 1024;
    for (size_t i = end_start; i < 8192; ++i) {
        hash ^= data[i];
        hash *= FNV_prime;
    }

    hash += signature;
    return hash;
}

bool is_valid_char(unsigned char c) {
    if (c >= 32 && c <= 126) {
        return true;
    }

    if (c == '\t') {
        return true;
    }

    return false;
}

bool is_valid_string(const char *str) {
    int numConsecutiveZero=0;
    if (str == NULL || *str == '\0') {
        return false;
    }

    size_t len = strlen(str);
    int chinese_char_count = 0;
    int consecutive_chinese = 0;

    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)str[i];
        if(c == '0')
            numConsecutiveZero++;
        else{
            numConsecutiveZero=0;
        }
        if(numConsecutiveZero > INVALID_CONSCTV_ZERO)
             return false;
    }
    return true;
}

void print_file_blocks(const char *filename,const char *toastfilename)
{

    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        perror("open");
        return;
    }
    struct stat st;
    int datafileIsEmpty=0;

    if (fstat(fd, &st) < 0) {
        perror("fstat");
        close(fd);
        return;
    }
    if (st.st_size == 0) {
        datafileIsEmpty=1;
    }
    struct fiemap *fiemap;
    char buffer[sizeof(struct fiemap) + sizeof(struct fiemap_extent) * 256];
    fiemap = (struct fiemap *)buffer;
    memset(fiemap, 0, sizeof(struct fiemap));
    fiemap->fm_start = 0;
    fiemap->fm_length = ~0ULL;
    fiemap->fm_flags = FIEMAP_FLAG_SYNC;
    fiemap->fm_extent_count = 256;

    if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
        perror("ioctl");
        close(fd);
        return;
    }
    printf("\n");
    infoPhysicalBlkHeader((char *)filename,st.st_size,0);
    for (int i = 0; i < fiemap->fm_mapped_extents; i++) {
        struct fiemap_extent *extent = &fiemap->fm_extents[i];
        if (!(extent->fe_flags & FIEMAP_EXTENT_UNWRITTEN)) {
            infoPhysycalBlkContect(
            (long long)extent->fe_physical,
            (long long)(extent->fe_physical + extent->fe_length - 1),
            (long long)extent->fe_length / BLCKSZ,
            i);
        }
    }
    printf("\n");
    close(fd);

    if(!ends_with(toastfilename,"/0")){
        int fd2 = open(toastfilename, O_RDONLY);
        if (fd2 < 0 ) {
            perror("open");
            return;
        }
        struct stat st2;
        int toastIsEmpty=0;
        if (fstat(fd2, &st2) < 0) {
            perror("fstat");
            close(fd2);
            return;
        }
        if (st2.st_size == 0) {
            toastIsEmpty=1;
        }
        if (ioctl(fd2, FS_IOC_FIEMAP, fiemap) < 0) {
            perror("ioctl");
            close(fd);
            return;
        }
        infoPhysicalBlkHeader((char *)toastfilename,st2.st_size,1);
        for (int i = 0; i < fiemap->fm_mapped_extents; i++) {
            struct fiemap_extent *extent = &fiemap->fm_extents[i];
            if (!(extent->fe_flags & FIEMAP_EXTENT_UNWRITTEN)) {
                infoPhysycalBlkContect(
                (long long)extent->fe_physical,
                (long long)(extent->fe_physical + extent->fe_length - 1),
                (long long)extent->fe_length / BLCKSZ,
                i);
            }
        }
        printf("\n");
        close(fd2);
    }
}

int dropFileRename(dropContext *dc)
{

    char *csvPrefix = dc->csvPrefix;
    off_t currSrtOffset = dc->currSrtOffset;
    int currBlks = dc->currBlks;
    int currItems = dc->currItems;
    if(!currItems){
        return FAILURE_RET;
    }
    int BadPct = ((float)dc->has_gibberish/(float)dc->currItems)*100;
    time_t now = time(NULL);

    struct tm *local = localtime(&now);
    if (local == NULL) {
        perror("localtime() failed");
        return 1;
    }

    char time_str[20];
    strftime(time_str, sizeof(time_str), "%m-%d-%H:%M:%S", local);

    char lastRestoreFile[MAXPGPATH]={0};
    char newRestoreFile[MAXPGPATH]={0};
    sprintf(lastRestoreFile,"%s/%lld.csv",csvPrefix,currSrtOffset);
    if(BadPct){
        sprintf(newRestoreFile,"%s/%d%%BAD_%s_%lld_%dblks_%ditems.csv",csvPrefix,BadPct,time_str,currSrtOffset,currBlks,currItems);
    }
    else{
        sprintf(newRestoreFile,"%s/%s_%lld_%dblks_%ditems.csv",csvPrefix,time_str,currSrtOffset,currBlks,currItems);
    }
    if (access(lastRestoreFile, F_OK) != 0) {
        return FAILURE_RET;
    }
    if (rename(lastRestoreFile, newRestoreFile) != 0) {
        return FAILURE_RET;
    }
    return SUCCESS_RET;
}

int dropFileRenameforToast(char *csvPrefix,off_t currSrtOffset,int currBlks,int currItems)
{
    #if DROPDEBUG == 1
    time_t now = time(NULL);
    struct tm *local = localtime(&now);
    if (local == NULL) {
        perror("localtime() failed");
        return 1;
    }

    char time_str[20];
    strftime(time_str, sizeof(time_str), "%m-%d-%H:%M:%S", local);

    char lastRestoreFile[MAXPGPATH]={0};
    char newRestoreFile[MAXPGPATH]={0};
    sprintf(lastRestoreFile,"%s/.toast/%lld",csvPrefix,currSrtOffset);
    sprintf(newRestoreFile,"%s/.toast/%s-%lld-%dblks-%ditems",csvPrefix,time_str,currSrtOffset,currBlks,currItems);
    if (access(lastRestoreFile, F_OK) != 0) {
        return FAILURE_RET;
    }
    if (rename(lastRestoreFile, newRestoreFile) != 0) {
        return FAILURE_RET;
    }
    return SUCCESS_RET;
    #endif
}

int dropFileRenameforFinal(dropContext *dc)
{
    char *csvPrefix = dc->csvPrefix;
    int currBlks=dc->totalBlks;
    int currItems=dc->totalItems;

    time_t now = time(NULL);
    struct tm *local = localtime(&now);
    if (local == NULL) {
        perror("localtime() failed");
        return 1;
    }

    char time_str[20];
    strftime(time_str, sizeof(time_str), "%m-%d-%H:%M:%S", local);

    char lastRestoreFile[MAXPGPATH]={0};
    char newRestoreFile[MAXPGPATH]={0};
    sprintf(lastRestoreFile,"%s/Toast.csv",csvPrefix);
    sprintf(newRestoreFile,"%s/TOAST_%lld_%dblks_%drecords.csv",csvPrefix,time_str,currBlks,currItems);
    if (access(lastRestoreFile, F_OK) != 0) {
        return FAILURE_RET;
    }
    if (rename(lastRestoreFile, newRestoreFile) != 0) {
        return FAILURE_RET;
    }
    infoUnlodResult(dc->tabname,dc->tabname,dc->totalBlks,dc->totalItems+dc->ErrItems,
    dc->totalItems,dc->ErrItems,newRestoreFile);
    return SUCCESS_RET;
}

int count_effective_lines(const char *filename) {
    FILE *file = fopen(filename, "rb");
    if (!file) {
        perror("Failed to openFile");
        return -1;
    }

    int line_count = 0;
    bool in_line = false;
    bool has_content = false;
    int current_char, prev_char = EOF;

    while ((current_char = fgetc(file)) != EOF) {
        if (current_char == '\n' || current_char == '\r') {
            if (current_char == '\r') {
                int next_char = fgetc(file);
                if (next_char != '\n' && next_char != EOF) {
                    ungetc(next_char, file);
                }
            }

            if (in_line && has_content) {
                line_count++;
            }

            in_line = false;
            has_content = false;
        } else {
            in_line = true;

            if (!isspace(current_char)) {
                has_content = true;
            }
        }

        prev_char = current_char;
    }

    if (in_line && has_content) {
        line_count++;
    }

    fclose(file);
    return line_count;
}

off_t logical_to_physical(off_t logical_offset, struct fiemap *fiemap) {
    uint64_t log_off = (uint64_t)logical_offset;
    for (int i = 0; i < fiemap->fm_mapped_extents; i++) {
        struct fiemap_extent *ext = &fiemap->fm_extents[i];

        if (log_off >= ext->fe_logical &&
            log_off < ext->fe_logical + ext->fe_length) {
            return (off_t)(ext->fe_physical + (log_off - ext->fe_logical));
        }

        if (log_off < ext->fe_logical) break;
    }
    return -1;
}

void genCopyForDs(char *tabname)
{
    char filenames[MAXPGPATH][MAX_FILENAME_LENGTH];
    int file_count=0;
    char path[MAXPGPATH]={0};
    char fullPath[MAXPGPATH]={0};

    ssize_t len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    get_parent_directory(path);
    sprintf(fullPath,"%s/%s/%s",path,dropscanDir,tabname);

    DIR *dir1;
    struct dirent *entry1;
    int arraySize=0;
    dir1 = opendir(fullPath);
    if (dir1 == NULL) {
        #ifdef EN
        printf("directory <%s> does not exist\n",fullPath);
        #else
        printf("Path <%s> does not exist\n",fullPath);
        #endif
        return;
    }
    else{
        while ((entry1 = readdir(dir1)) != NULL) {
            if (entry1->d_type == 8) {
                if(strcmp(entry1->d_name,"COPY.sql") != 0 &&
                   strcmp(entry1->d_name,"Error.csv") != 0 &&
                   strcmp(entry1->d_name,".rec") != 0 )
                   {
                        strcpy(filenames[file_count],entry1->d_name);
                        file_count++;
                   }
            }
        }
    }
    char copyFilename[MAXPGPATH]={0};
    sprintf(copyFilename,"%s/COPY.sql",fullPath);
    FILE *copyfp = fopen(copyFilename,"w");
    if(file_count > 0){
        for (int i = 0; i < file_count; i++) {
            char *str2write=malloc(MAXPGPATH);
            sprintf(str2write,"COPY %s FROM '%s/%s';\n",tabname,fullPath,filenames[i]);
            fputs(str2write,copyfp);
            free(str2write);
        }
        printf("%s%s%s\n",COLOR_UNLOAD,copyFilename,C_RESET);
    }

    fclose(copyfp);
}

int compare_offt(const void *a, const void *b) {
    const off_t val1 = *(const off_t *)a;
    const off_t val2 = *(const off_t *)b;

    if (val1 < val2) return -1;
    if (val1 > val2) return 1;
    return 0;
}

void initToastHashforDs(dropContext *dc)
{
    harray *toastHash = NULL;
    char metaToastPath[100];
    sprintf(metaToastPath,"%s/.toast/dbf_idx",dc->csvPrefix);
    int numLines = 0;
    FILE *file = fileGetLines(metaToastPath,&numLines);
    if(file == NULL)
        return;

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
    dc->toastOids = toastHash;
}

int initPageOffsforDs(dropContext *dc)
{
    char dbf_tab[100]={0};
    sprintf(dbf_tab,"%s/.toast/dbf_tab",dc->csvPrefix);
    FILE *exist = fopen(dbf_tab,"r");
    if(!exist){
        return FAILURE_RET;
    }
    else{
        fclose(exist);
    }

    parray *idxs = parray_new();
    char currIdxName[100]={0};
    sprintf(currIdxName,"%s/.toast/dbf_fsm",dc->csvPrefix);
    FILE *idxfp = fopen(currIdxName,"r");
    if(idxfp ==NULL){
        return FAILURE_RET;
    }
    fseek(idxfp, 0, SEEK_SET);
    char line[256];
    while (!feof(idxfp)){
        dsPageOff *elem = (dsPageOff *)malloc(sizeof(dsPageOff));
        char a[50]={0};
        char b[50]={0};
        fscanf(idxfp, "%s\t%s\n", a,b);
        elem->pageOff = atoll(a);
        elem->itemOff = atoi(b);
        parray_append(idxs,elem);
    }
    fclose(idxfp);
    dc->PageOffs = idxs;
    return SUCCESS_RET;
}

parray **group_chunks(parray *input, int *num_groups) {
    if (parray_num(input) == 0) {
        *num_groups = 0;
        return NULL;
    }

    parray *zero_chunks = parray_new();
    for (size_t i = 0; i < parray_num(input); i++) {
        chunkInfo *ci = (chunkInfo *)parray_get(input, i);
        if (ci->chunkid == 0) {
            parray_append(zero_chunks, ci);
        }
    }

    if (parray_num(zero_chunks) == 1) {
        parray *single_group = parray_new();
        for (size_t i = 0; i < parray_num(input); i++) {
            parray_append(single_group, parray_get(input, i));
        }

        parray_qsort(single_group, compare_chunkid);

        *num_groups = 1;
        parray **result = pgut_malloc(sizeof(parray *));
        result[0] = single_group;

        parray_free(zero_chunks);
        return result;
    }

    parray_qsort(zero_chunks, compare_blk);

    parray *groups_container = parray_new();

    for (size_t i = 0; i < parray_num(zero_chunks); i++) {
        parray *group = parray_new();
        chunkInfo *zero_ci = (chunkInfo *)parray_get(zero_chunks, i);
        parray_append(group, zero_ci);
        parray_append(groups_container, group);
    }

    const BlockNumber BLK_THRESHOLD = 100;

    for (size_t i = 0; i < parray_num(input); i++) {
        chunkInfo *ci = (chunkInfo *)parray_get(input, i);
        if (ci->chunkid == 0) continue;

        BlockNumber min_diff = UINT_MAX;
        parray *closest_group = NULL;

        for (size_t j = 0; j < parray_num(groups_container); j++) {
            parray *group = (parray *)parray_get(groups_container, j);
            chunkInfo *group_zero = (chunkInfo *)parray_get(group, 0);

            BlockNumber diff = (ci->blk > group_zero->blk) ?
                (ci->blk - group_zero->blk) : (group_zero->blk - ci->blk);

            if (diff < min_diff) {
                min_diff = diff;
                closest_group = group;
            }
        }

        if (closest_group != NULL) {
            parray_append(closest_group, ci);
        }
    }

    for (size_t i = 0; i < parray_num(groups_container); i++) {
        parray *group = (parray *)parray_get(groups_container, i);
        parray_qsort(group, compare_chunkid);
    }

    *num_groups = parray_num(groups_container);
    parray **result = pgut_malloc(sizeof(parray *) * parray_num(groups_container));

    for (size_t i = 0; i < parray_num(groups_container); i++) {
        result[i] = (parray *)parray_get(groups_container, i);
    }

    parray_free(zero_chunks);

    return result;
}

parray *list_directories(const char *path) {

    DIR *dir = opendir(path);
    if (dir == NULL) {
        return NULL;
    }
    parray *dirs = parray_new();
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        char full_path[1024];
        snprintf(full_path, sizeof(full_path), "%s/%s", path, entry->d_name);

        struct stat statbuf;
        if (lstat(full_path, &statbuf) != 0) {
            continue;
        }

        if (S_ISDIR(statbuf.st_mode)) {
            char *dirname = strdup(entry->d_name);
            parray_append(dirs,dirname);
        }
    }
    closedir(dir);
    return dirs;
}

bool is_valid_postgres_timestamp(const char *input)
{
    if (input == NULL || *input == '\0') {
        return false;
    }

    const char *pattern =
        "^("
        "\\d{4}-\\d{1,2}-\\d{1,2}"  // YYYY-MM-DD

        "(?:[ T]\\d{1,2}:\\d{1,2}(?::\\d{1,2}(?:\\.\\d{1,6})?)?"

        "(?:[+-]\\d{1,2}(?::?\\d{1,2})?| [A-Za-z]{1,3})?"
        ")$";

    regex_t regex;
    int ret = regcomp(&regex, pattern, REG_EXTENDED | REG_NOSUB);
    if (ret != 0) {
        return false;
    }

    ret = regexec(&regex, input, 0, NULL, 0);
    regfree(&regex);
    if (ret != 0) {
        return false;
    }

    struct tm tm = {0};
    char *result = strptime(input, "%Y-%m-%d", &tm);
    if (result == NULL) {
        return false;
    }

    if (tm.tm_mon < 0 || tm.tm_mon > 11) return false;
    if (tm.tm_mday < 1 || tm.tm_mday > 31) return false;

    time_t t = mktime(&tm);
    if (t == (time_t)-1) {
        return false;
    }

    return true;
}

void printDropScanDirs(parray *dcs){
    #ifdef CN
    printf("%sScan complete, file directory:\n%s",COLOR_SUCC,C_RESET);
    #else
    printf("%sScan completed, file directory as follows: \n%s",COLOR_SUCC,C_RESET);
    #endif
    for(int i=0;i<parray_num(dcs);i++){
        dropContext *elem = parray_get(dcs,i);
        printf("\t%s%s%s\n",COLOR_UNLOAD,elem->csvPrefix,C_RESET);
    }
}

#include <inttypes.h>

int is_address_valid(void *addr) {
    uintptr_t target = (uintptr_t)addr;
    FILE *fp = fopen("/proc/self/maps", "r");
    if (!fp) return 0;

    char line[1024];
    while (fgets(line, sizeof(line), fp)) {
        uintptr_t start, end;
        if (sscanf(line, "%" SCNxPTR "-%" SCNxPTR, &start, &end) == 2) {
            if (target >= start && target < end) {
                fclose(fp);
                return 1;
            }
        }
    }
    fclose(fp);
    return 0;
}

int copyFile(const char *src,const char *dest)
{

    FILE *src_file = fopen(src, "rb");
    if (!src_file) {
        perror("Failed to open source file");
        return EXIT_FAILURE;
    }

    FILE *dest_file = fopen(dest, "wb");
    if (!dest_file) {
        perror("Failed to create destination file");
        fclose(src_file);
        return EXIT_FAILURE;
    }

    unsigned char buffer[4096];
    size_t bytes_read;
    int error = 0;

    while ((bytes_read = fread(buffer, 1, 4096, src_file)) > 0) {
        if (fwrite(buffer, 1, bytes_read, dest_file) != bytes_read) {
            perror("Failed to write to destination file");
            error = 1;
            break;
        }
    }

    if (ferror(src_file)) perror("Failed to read source file");

    fclose(src_file);
    if (fclose(dest_file)) perror("Failed to close destination file");

    return error ? EXIT_FAILURE : EXIT_SUCCESS;

}

void replaceArchPath()
{
    char *srcname = "pdu.ini";
    char *tmpname = "pdu.initmp";
    FILE *srcFile = fopen(srcname, "r");
    FILE *tmpFile = fopen(tmpname, "w");

    char line[1024];
    const char *target = "ARCHIVE_DEST=";
    const char *replacement = "ARCHIVE_DEST=restore/ckwal";
    int found = 0;

    while (fgets(line, sizeof(line), srcFile)) {
        char *match = strstr(line, target);
        if (match != NULL && !found) {
            int prefixLen = match - line;
            fwrite(line, 1, prefixLen, tmpFile);
            fprintf(tmpFile, "%s", replacement);
            char *afterMatch = match + strlen(target);
            while (*afterMatch != '\0' && *afterMatch != '\n' && *afterMatch != '\r' && *afterMatch != ' ' && *afterMatch != '\t') {
                afterMatch++;
            }
            fprintf(tmpFile, "%s", afterMatch);
            found = 1;
        } else {
            fputs(line, tmpFile);
        }
    }

    fclose(srcFile);
    fclose(tmpFile);

    remove(srcname);
    if (rename(tmpname, srcname) != 0) {
        perror("Error replacing file");
        return;
    }
}

void trim_whitespace(char *str) {
    if (str == NULL) return;

    int start = 0;
    while (isspace((unsigned char)str[start]))
        start++;

    int end = strlen(str) - 1;
    while (end >= start && isspace((unsigned char)str[end]))
        end--;

    int i;
    for (i = 0; i <= end - start; i++)
        str[i] = str[start + i];

    str[i] = '\0';
}

static bool is_normal_char(uint32_t c) {
    if (c <= 0x7F) {
        return true;
    }

    if ((c >= 0x4E00 && c <= 0x9FFF) ||
        (c >= 0x3400 && c <= 0x4DBF) ||
        (c >= 0x20000 && c <= 0x2A6DF) ||
        (c >= 0x2A700 && c <= 0x2B73F) ||
        (c >= 0x2B740 && c <= 0x2B81F) ||
        (c >= 0x2B820 && c <= 0x2CEAF) ||
        (c >= 0xF900 && c <= 0xFAFF) ||
        (c >= 0xFE30 && c <= 0xFE4F) ||
        (c >= 0x3000 && c <= 0x303F) ||
        (c >= 0x31F0 && c <= 0x31FF) ||
        (c >= 0xAC00 && c <= 0xD7AF) ||
        (c >= 0x0E00 && c <= 0x0E7F) ||
        (c >= 0xFF00 && c <= 0xFFEF)) {
        return true;
    }

    if ((c >= 0x0080 && c <= 0x00FF) ||
        (c >= 0x0100 && c <= 0x017F) ||
        (c >= 0x0180 && c <= 0x024F) ||
        (c >= 0x0250 && c <= 0x02AF)) {
        return true;
    }

    if ((c >= 0x0370 && c <= 0x03FF) ||
        (c >= 0x0400 && c <= 0x04FF) ||
        (c >= 0x0590 && c <= 0x05FF) ||
        (c >= 0x0600 && c <= 0x06FF) ||
        (c >= 0x0900 && c <= 0x097F)) {
        return true;
    }

    if ((c >= 0x2000 && c <= 0x206F) ||
        (c >= 0x2100 && c <= 0x214F)) {
        return true;
    }

    return false;
}

/**
 * has_gibberish - Check if string contains garbled characters
 *
 * @xman: UTF-8 encoded string to check
 *
 * Returns: true if contains garbled characters, false otherwise
 */
bool has_gibberish(const char *xman) {
    if (xman == NULL) {
        return false;
    }

    while (*xman != '\0') {
        uint8_t byte = (uint8_t)*xman;
        uint32_t code_point;
        int bytes_needed;

        if ((byte & 0x80) == 0) {
            code_point = byte;
            bytes_needed = 1;
        } else if ((byte & 0xE0) == 0xC0) {
            if (xman[1] == '\0') return true;
            code_point = ((byte & 0x1F) << 6) | ((uint8_t)xman[1] & 0x3F);
            bytes_needed = 2;
        } else if ((byte & 0xF0) == 0xE0) {
            if (xman[1] == '\0' || xman[2] == '\0') return true;
            code_point = ((byte & 0x0F) << 12) | (((uint8_t)xman[1] & 0x3F) << 6) | ((uint8_t)xman[2] & 0x3F);
            bytes_needed = 3;
        } else if ((byte & 0xF8) == 0xF0) {
            if (xman[1] == '\0' || xman[2] == '\0' || xman[3] == '\0') return true;
            code_point = ((byte & 0x07) << 18) | (((uint8_t)xman[1] & 0x3F) << 12) |
                         (((uint8_t)xman[2] & 0x3F) << 6) | ((uint8_t)xman[3] & 0x3F);
            bytes_needed = 4;
        } else {
            return true;
        }

        if (!is_normal_char(code_point)) {
            return true;
        }

        xman += bytes_needed;
    }

    return false;
}

void getDropScanOids(TABstruct *taboid,char *flag)
{
    if(strcmp(flag,"pg_class") == 0){
        strcpy(taboid->oid,"1259");
        strcpy(taboid->filenode,"1259");
        strcpy(taboid->toastoid,"0");
        strcpy(taboid->toastnode,"0");
        strcpy(taboid->nsp,"0");
        strcpy(taboid->tab,"pg_class");
        strcpy(taboid->attr,CLASS_ATTR);
        strcpy(taboid->typ,CLASS_ATTR);
        int nattr = countCommas(CLASS_ATTR)+1;
        sprintf(taboid->nattr,"%d",nattr);
        strcpy(taboid->attmod,CLASS_ATTR);
        strcpy(taboid->attlen,CLASS_ATTR);
        strcpy(taboid->attalign,CLASS_ATTR);
    }
    else if(strcmp(flag,"pg_attribute") == 0){
        strcpy(taboid->oid,"1249");
        strcpy(taboid->filenode,"1249");
        strcpy(taboid->toastoid,"0");
        strcpy(taboid->toastnode,"0");
        strcpy(taboid->nsp,"0");
        strcpy(taboid->tab,"pg_attribute");
        strcpy(taboid->attr,ATTR_ATTR);
        strcpy(taboid->typ,ATTR_ATTR);
        int nattr = countCommas(ATTR_ATTR)+1;
        sprintf(taboid->nattr,"%d",nattr);
        strcpy(taboid->attmod,ATTR_ATTR);
        strcpy(taboid->attlen,ATTR_ATTR);
        strcpy(taboid->attalign,ATTR_ATTR);
    }
}

int getDecodeFunctions(const char *typ,decodeFuncs attr2Process[MAX_COL_NUM])
{
    resetArray2Process(attr2Process);
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
        if(!AddList2Prcess(attr2Process,ret,CLASS_BOOTTYPE)){
            return FAILURE_RET;
        }
    }
    for (int i = 0; i < MAX_COL_NUM; i++) {
        free(attrChars[i]);
    }
}

systemDropContext* initSystemDropContext(char *flag)
{
    systemDropContext *sdc = (systemDropContext*)malloc(sizeof(systemDropContext));
    sdc->taboid = (TABstruct*)malloc(sizeof(TABstruct));
    sdc->allDesc = (pg_attributeDesc*)malloc(20*sizeof(pg_attributeDesc));
    if(strcmp(flag,"pg_class") == 0){
        strcpy(sdc->savepath,"restore/meta/pg_class.txt");
    }
    else{
        strcpy(sdc->savepath,"restore/meta/pg_attr.txt");
    }
    FILE *fp = fopen(sdc->savepath,"w");
    fclose(fp);
    getDropScanOids(sdc->taboid,flag);
    int a = getPgAttrDesc(sdc->taboid,sdc->allDesc);
    getDecodeFunctions(sdc->taboid->typ,sdc->attr2Process);
    return sdc;
}

char* read_last_non_empty_line(const char* filename) {
    FILE* file = fopen(filename, "rb");
    if (file == NULL) {
        return NULL;
    }

    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);

    if (file_size == 0) {
        fclose(file);
        return NULL;
    }

    long current_pos = file_size - 1;
    int found_non_blank = 0;

    while (current_pos >= 0) {
        fseek(file, current_pos, SEEK_SET);
        int c = fgetc(file);

        if (!isspace(c)) {
            found_non_blank = 1;
            break;
        }
        current_pos--;
    }

    if (!found_non_blank) {
        fclose(file);
        return NULL;
    }

    long line_end = current_pos;
    long line_start = current_pos;

    while (current_pos >= 0) {
        fseek(file, current_pos, SEEK_SET);
        int c = fgetc(file);

        if (c == '\n' || c == '\r') {
            line_start = current_pos + 1;
            break;
        }
        current_pos--;

        if (current_pos < 0) {
            line_start = 0;
        }
    }

    size_t line_length = line_end - line_start + 1;
    char* line = (char*)malloc(line_length + 1);
    if (line == NULL) {
        fclose(file);
        return NULL;
    }

    fseek(file, line_start, SEEK_SET);
    size_t read_count = fread(line, 1, line_length, file);
    line[read_count] = '\0';

    fclose(file);
    return line;
}

bool unwantedCol(char *colname){
    const char *list[] = {
        "ctid",
        "xmin",
        "cmin",
        "xmax",
        "cmax",
        "tableoid",
        "chunk_id",
        "chunk_seq",
        "chunk_data"
    };
    int list_size = sizeof(list) / sizeof(list[0]);
    int i;
    for (i = 0; i < list_size; i++) {
        if (strcmp(colname, list[i]) == 0) {
            return 1;
        }
    }
    return 0;
}

char *get_field(char delimiter,const char *str, int a) {
    if (a < 1 || str == NULL) {
        return NULL;
    }

    const char *p = str;
    int count = 1;
    const char *start = str;

    while (*p != '\0') {
        if (*p == delimiter) {
            if (count == a) {
                break;
            }
            count++;
            start = p + 1;
        }
        p++;
    }

    if (count == a) {
        size_t len = p - start;
        char *res = (char *)malloc(len + 1);
        if (res == NULL) {
            return NULL;
        }
        strncpy(res, start, len);
        res[len] = '\0';
        return res;
    } else {
        return NULL;
    }
}

int initUnloadHash(char *filename,harray *rechash)
{
    int numLines = 0;
    FILE *file = fileGetLines(filename,&numLines);
    Oid filenode;
    if(file == NULL){
        return FAILURE_RET;
    }

    int i;
    for (i = 0; i < numLines; i++) {
        char str[10];
        if (fscanf(file, "%s\n",str) != 1) {
            printf("Error Reading File %s\n",filename);
            exit(1);
        }
        filenode = atoi(str);
        harray_append(rechash,HARRAYINT,&filenode,filenode);
    }

    fclose(file);
    return SUCCESS_RET;
}

int initIdxFileHash(char *filename,harray *idxhash)
{
    int numLines = 0;
    FILE *file = fileGetLines(filename,&numLines);
    if(file == NULL){
        return FAILURE_RET;
    }
    uint64 offset;
    int i;
    for (i = 0; i < numLines; i++) {
        char str[100];
        if (fscanf(file, "%s\n",str) != 1) {
            printf("Error Reading File %s\n",filename);
            exit(1);
        }
        offset = atol(str);
        harray_append(idxhash,HARRAYLLINT,&offset,offset);
    }

    fclose(file);
    return SUCCESS_RET;
}

bool chkIdxExist()
{
    char currIdxName[100]="restore/.dsiso/idx";
    FILE *idxfp = fopen(currIdxName,"r");
    if(!idxfp){
        #ifdef CN
        printf("%s请先执行ds idx获取磁盘索引\n%s",COLOR_WARNING,C_RESET);
        #else
        printf("%sPlease Execute 'ds idx' Command to Require Disk Index First\n%s",COLOR_WARNING,C_RESET);
        #endif
        return false;
    }
    else{
        int cnt = count_effective_lines(currIdxName);
        fclose(idxfp);
        if(cnt>0){
            return true;
        }
        else{
            #ifdef CN
            printf("%s请先执行ds idx获取磁盘索引\n%s",COLOR_WARNING,C_RESET);
            #else
            printf("%sPlease Execute 'ds idx' Command to Require Disk Index First!\n%s",COLOR_WARNING,C_RESET);
            #endif
            return false;
        }
    }
}

void getAttrAlignAndAttlen(char *attr,char *alignStr,char *attlenStr)
{
    int nattr = countCommas(attr)+1;
    for (int i = 0; i < nattr; i++)
    {
        char *singleAttr = get_field(',',attr,i+1);
        char *singleStdAttr = malloc(10);
        memset(singleStdAttr,0,10);
        if(strcmp(singleAttr,"bpchar") != 0){
            getStdTyp(singleAttr,singleStdAttr);
        }
        else{
            strcpy(singleStdAttr,singleAttr);
        }
        int size = sizeof(alignLen_table)/sizeof(alignLen_table[0]);
        int j;
        for ( j=0;j<size;j++ ){
            if ( strcmp(alignLen_table[j].attname,singleStdAttr) == 0 ){
                if(i != 0){
                    strcat(alignStr,",");
                    strcat(attlenStr,",");
                }
                strcat(alignStr,alignLen_table[j].attalign);
                strcat(attlenStr,alignLen_table[j].attlen);
                free(singleStdAttr);
                free(singleAttr);
                break;
            }
        }
    }

}

#include <stdbool.h>
#include <ctype.h>

bool isValidString(const char* str) {
    for (int i = 0; str[i] != '\0'; i++) {
        char c = str[i];

        if (!(isdigit(c) ||
                isalpha(c) ||
                c == '\t' ||
                c == '-'||
                c == '_'
            ))
            {
            return false;
        }
    }

    return true;
}

char *get_symlink_target(const char *symlink_path) {
    struct stat st;

    if (lstat(symlink_path, &st) == -1) {
        return NULL;
    }

    if (!S_ISLNK(st.st_mode)) {
        return (char *)symlink_path;
    }

    char *target_path = malloc(PATH_MAX);
    if (!target_path) {
        perror("malloc");
        return NULL;
    }

    ssize_t len = readlink(symlink_path, target_path, PATH_MAX - 1);
    if (len == -1) {
        perror("readlink");
        free(target_path);
        return NULL;
    }

    target_path[len] = '\0';
    return target_path;
}

/**
 * getFreeDiskSpace - Get available disk space for current program location
 *
 * Returns: Available disk space in bytes on success, -1 on failure
 */
long long getFreeDiskSpace() {
    char path[1024];
    struct statvfs buf;

    ssize_t len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if (len == -1) {
        perror("readlink");
        return -1;
    }
    path[len] = '\0';

    if (statvfs(path, &buf) != 0) {
        perror("statvfs");
        return -1;
    }

    unsigned long long free_space = (unsigned long long)buf.f_bavail * buf.f_frsize;
    return (long long)free_space;
}

/**
 * is_file_larger_than - Check if file size exceeds specified bytes
 *
 * @filename: File path to check
 * @n_bytes:  Byte threshold to compare against
 *
 * Returns: 1 if file > n_bytes, 0 if file <= n_bytes, -1 if file not found or read failed
 */
int is_file_larger_than(const char *filename, long long n_bytes) {
    FILE *file = fopen(filename, "rb");
    if (file == NULL) {
        return 0;
    }

    if (fseek(file, 0, SEEK_END) != 0) {
        perror("fseekfailed");
        fclose(file);
        return -1;
    }

    long long file_size = ftell(file);

    if (file_size == -1) {
        perror("ftellfailed");
        fclose(file);
        return -1;
    }

    fclose(file);

    return (file_size > n_bytes) ? 1 : 0;
}

void rmLastn(char *str)
{
    if(str[strlen(str)-1] == '\n'){
        str[strlen(str)-1]='\0';
    }
}

char* execCMD(const char *command)
{
    FILE *pipe;
    char buffer[1024];
    char *output = (char*)malloc(sizeof(char)*200);

    pipe = popen(command, "r");
    if (pipe == NULL) {
        perror("popen failed");
        return NULL;
    }

    while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
        size_t len = strlen(buffer);
        strcpy(output, buffer);
    }

    pclose(pipe);
    rmLastn(output);
    return output;
}
