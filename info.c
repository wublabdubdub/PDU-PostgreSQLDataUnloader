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

#include "info.h"
#include "basic.h"

/**
 * cprDeclaration - Display copyright and version information
 *
 * Prints the PDU copyright notice, version information, supported database
 * versions, and current edition details to stdout.
 */
void cprDeclaration(void)
{
    printf("\n");
    char DBTYPE[100];
    char SUPPORTVER[100];

    char linetitle[50];
    char line1[100];
    char line2[100];
    char line3[100];
    char line4[100];
    char line5[100];


    sprintf(DBTYPE,"PDU: PostgreSQL Data Unloader           ");
    sprintf(SUPPORTVER,"  • PostgreSQL %d", PG_VERSION_NUM);

    sprintf(linetitle,"COMMUNITY VERSION");
    sprintf(line1,"• Licensed to everyone%-18s");

    printf("%s╔══════════════════════════════════════════════════════╗%s\n",COLOR_COPYRIGHT,C_RESET);
    printf("%s║  Copyright 2024-2025 ZhangChen. All rights reserved  ║%s\n",COLOR_COPYRIGHT,C_RESET);
    printf("%s║  %s            ║%s\n",COLOR_COPYRIGHT,DBTYPE,C_RESET);
    printf("%s║  Version %-10s (%s)                     ║%s\n",COLOR_COPYRIGHT,PDUVERSION,PKGTIME,C_RESET);
    printf("%s╚══════════════════════════════════════════════════════╝%s\n\n",COLOR_COPYRIGHT,C_RESET);

    printf("  %sCurrent DB Supported Version:%s\n",COLOR_VERSION,C_RESET);
    printf("  %s──────────────────────────%s\n",COLOR_VERSION,C_RESET);
    printf("%s%s%s\n\n", COLOR_VERSION,SUPPORTVER,C_RESET);

    printf("%s╔═══════════════════════════════════════════╗%s\n",COLOR_EDITION,C_RESET);
    printf("%s║           %20s            ║%s\n",COLOR_EDITION,linetitle,C_RESET);
    printf("%s╠═══════════════════════════════════════════╣%s\n",COLOR_EDITION,C_RESET);
    printf("%s║ %-44s║%s\n",COLOR_EDITION,line1,C_RESET);
    printf("%s╚═══════════════════════════════════════════╝%s\n\n",COLOR_EDITION,C_RESET);
}

/**
 * printUsage - Display command help and usage information
 *
 * Prints the complete command reference including basic operations,
 * database context switching, metadata display, data export, data recovery,
 * and parameter settings. Output language is controlled by CN/EN macros.
 */
void printUsage()
{
#ifdef CN
    printf("\nPDU数据拯救工具 | 命令帮助\n");
    printf("%s┌──────────────────────────────────────────────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);
    printf("%s  **基础操作**%s\n",COLOR_helpBasic,C_RESET);
    printf("%s  b;                                      │ 初始化数据库元信息%s\n",COLOR_helpBasic,C_RESET);
    printf("%s  <exit;>|<\\q;>                           │ 退出工具%s\n",COLOR_helpBasic,C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n", COLOR_helpBasic, C_RESET);

    printf("%s  **数据库切换**%s\n",COLOR_helpSwitch,C_RESET);
    printf("%s  use <db>;                               │ 指定当前数据库（例: use logs;）%s\n",COLOR_helpSwitch,C_RESET);
    printf("%s  set <schema>;                           │ 指定当前模式（例: set recovery;）%s\n",COLOR_helpSwitch,C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n",COLOR_helpSwitch,C_RESET);

    printf("%s  **元数据展示**\n",COLOR_helpShow,C_RESET);
    printf("%s  \\l;                                     │ 列出所有数据库%s\n",COLOR_helpShow,C_RESET);
    printf("%s  \\dn;                                    │ 列出当前数据库所有模式%s\n",COLOR_helpShow,C_RESET);
    printf("%s  \\dt;                                    │ 列出当前模式下的所有表%s\n",COLOR_helpShow,C_RESET);
    printf("%s  \\d+ <table>;                            │ 查看表结构详情（例: \\d+ users;）%s\n",COLOR_helpShow,C_RESET);
    printf("%s  \\d <table>;                             │ 查看表列类型（例: \\d users;）%s\n",COLOR_helpShow,C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n",COLOR_helpShow,C_RESET);

    printf("%s  **数据导出**%s\n",COLOR_helpUnload,C_RESET);
    printf("%s  u|unload tab <table>;                   │ 导出表数据到CSV（例: unload tab orders;）%s\n",COLOR_helpUnload,C_RESET);
    printf("%s  u|unload sch <schema>;                  │ 导出整个模式数据（例: unload sch public;）%s\n",COLOR_helpUnload,C_RESET);
    printf("%s  u|unload ddl;                           │ 生成当前模式DDL语句文件%s\n",COLOR_helpUnload,C_RESET);
    printf("%s  u|unload copy;                          │ 生成CSV的COPY语句脚本%s\n",COLOR_helpUnload,C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n",COLOR_helpUnload,C_RESET);

    printf("%s  **误操作数据恢复**%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  scan <t1|manual>;                       │ 扫描误删表/从manual目录初始化元数据%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  scan drop;                              │ 扫描被drop的表结构%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  meta tab/sch <tabname/schema>;          │ 将指定的表结构/模式下的所有表结构自动填入tab.conifg中%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  restore del/upd [<TxID>|all];           │ 按事务号/时间区间恢复数据%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  add <filenode> <表名> <字段类型列表>;   │ 手动添加表信息（例: add 12345 t1 varchar,...）[!] 需将数据文件放入restore/datafile%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  restore db <库名> <路径>;               │ 初始化自定义数据库目录（例: restore db xmandb /home/...）%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n",COLOR_helpRestore,C_RESET);

    printf("%s  **Drop Table恢复** [Pro/Enterprise]%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds idx;                        │ [Pro/Enterprise] 获取磁盘上被drop的PG数据页索引%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds;                            │ [Pro/Enterprise] 针对tab.config中配置的表进行磁盘扫描恢复%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds iso;                        │ [Pro/Enterprise] 针对tab.config中配置的表进行镜像文件恢复%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds repair;                     │ [Pro/Enterprise] 针对此前扫描失败的TOAST表进行恢复%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds clean;                      │ [Pro/Enterprise] 删除restore/dropscan下的所有目录%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds copy;                       │ [Pro/Enterprise] 生成restore/dropscan下的所有表文件的COPY命令%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n",COLOR_helpRestore,C_RESET);

    printf("%s  **参数设置**%s\n",COLOR_helpParam,C_RESET);
    printf("%s  p|param startwal/endwal <WAL文件>;      │ 设置WAL扫描范围（默认归档目录首尾）%s\n",COLOR_helpParam,C_RESET);
    printf("%s  p|param starttime/endtime <时间>;       │ 设置时间扫描范围（例: 2025-01-01 00:00:00）%s\n",COLOR_helpParam,C_RESET);
    printf("%s  p|param resmode tx|time;                │ 设置恢复模式（事务号/时间区间）%s\n",COLOR_helpParam,C_RESET);
    printf("%s  p|param restype delete|update;          │ 设置恢复类型（删除/更新）%s\n",COLOR_helpParam,C_RESET);
    printf("%s  p|param exmode csv|sql;                 │ 设置导出格式（默认CSV）%s\n",COLOR_helpParam,C_RESET);
    printf("%s  p|param encoding utf8|gbk;              │ 设置字符编码（默认utf8）%s\n",COLOR_helpParam,C_RESET);
    printf("%s  p|param isomode on|off;                 │ 设置镜像保存模式（默认off）%s\n",COLOR_helpParam,C_RESET);
    printf("%s  reset <参数名>|all;                     │ 重置指定参数|所有参数%s\n",COLOR_helpParam,C_RESET);
    printf("%s  show;                                   │ 查看所有参数状态%s\n",COLOR_helpParam,C_RESET);
    printf("%s  t;                                      │ 查看当前支持的数据类型%s\n",COLOR_helpParam,C_RESET);
    printf("%s└──────────────────────────────────────────────────────────────────────────────────────────────────┘%s\n", COLOR_UNLOAD, C_RESET);

    printf("\n语法规则\n");
    printf("◈ 所有指令必须以`;`结尾\n");

#else
    printf("\nPDU Data Rescue Tool | Command Reference\n");
    printf("%s┌──────────────────────────────────────────────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);

    printf("%s  **Basic Operations**%s\n", COLOR_helpBasic, C_RESET);
    printf("%s  b;                                      │ Initialize database metadata%s\n", COLOR_helpBasic, C_RESET);
    printf("%s  <exit>;<\\q>;                            │ Exit the tool%s\n", COLOR_helpBasic, C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n", COLOR_helpBasic, C_RESET);

    printf("%s  **Database Context**%s\n", COLOR_helpSwitch, C_RESET);
    printf("%s  use <db>;                               │ Set current database (e.g. use logs;)%s\n", COLOR_helpSwitch, C_RESET);
    printf("%s  set <schema>;                           │ Set current schema (e.g. set recovery;)%s\n", COLOR_helpSwitch, C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n", COLOR_helpSwitch, C_RESET);

    printf("%s  **Metadata Display**%s\n", COLOR_helpShow, C_RESET);
    printf("%s  \\l;                                     │ List all databases%s\n", COLOR_helpShow, C_RESET);
    printf("%s  \\dn;                                    │ List all schema in current database%s\n", COLOR_helpShow, C_RESET);
    printf("%s  \\dt;                                    │ List tables in current schema%s\n", COLOR_helpShow, C_RESET);
    printf("%s  \\d+ <table>;                            │ View table structure details (e.g. \\d+ users;)%s\n", COLOR_helpShow, C_RESET);
    printf("%s  \\d <table>;                             │ View column types (e.g. \\d users;)%s\n", COLOR_helpShow, C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n", COLOR_helpShow, C_RESET);

    printf("%s  **Data Export**%s\n", COLOR_helpUnload, C_RESET);
    printf("%s  u|unload tab <table>;                   │ Export table to CSV (e.g. unload tab orders;)%s\n", COLOR_helpUnload, C_RESET);
    printf("%s  u|unload sch <schema>;                  │ Export entire schema (e.g. unload sch public;)%s\n", COLOR_helpUnload, C_RESET);
    printf("%s  u|unload ddl;                           │ Generate DDL statements of current schema%s\n", COLOR_helpUnload, C_RESET);
    printf("%s  u|unload copy;                          │ Generate COPY statements for CSVs%s\n", COLOR_helpUnload, C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n", COLOR_helpUnload, C_RESET);

    printf("%s  **Accidental Operation Data Recovery**%s\n", COLOR_helpRestore, C_RESET);
    printf("%s  scan [t1|manual];                       │ Scan deleted/update records of tables/Init metadata from manual%s\n", COLOR_helpRestore, C_RESET);
    printf("%s  restore del/upd [<TxID>|all];           │ Restore data by transaction ID/time range%s\n", COLOR_helpRestore, C_RESET);
    printf("%s  add <filenode> <table> <columns>;       │ Manually add table info (e.g. add 12345 t1 varchar,...) [!] Datafile should be put into path 'restore/datafile'%s\n", COLOR_helpRestore, C_RESET);
    printf("%s  restore db <db> <path>;                 │ [Pro/Enterprise] Init customized database directory (e.g. restore db xmandb /home/...)%s\n", COLOR_helpRestore, C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n", COLOR_helpRestore, C_RESET);

    printf("%s  **Drop Table Recovery** [Pro/Enterprise]%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds idx;                        │ [Pro/Enterprise] Get disk page index for dropped PG data%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds;                            │ [Pro/Enterprise] Perform disk scan recovery for tables in tab.config%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds iso;                        │ [Pro/Enterprise] Recover from ISO image file for tables in tab.config%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds repair;                     │ [Pro/Enterprise] Recover previously failed TOAST table scans%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds clean;                      │ [Pro/Enterprise] Delete all directories under restore/dropscan%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  dropscan/ds copy;                       │ [Pro/Enterprise] Generate COPY commands for all table files under restore/dropscan%s\n",COLOR_helpRestore,C_RESET);
    printf("%s  ----------------------------------------.--------------------------------%s\n\n",COLOR_helpRestore,C_RESET);

    printf("%s  **Parameters**%s\n", COLOR_helpParam, C_RESET);
    printf("%s  p|param startwal/endwal <WAL>;          │ Set WAL scan range (default archive boundaries)%s\n", COLOR_helpParam, C_RESET);
    printf("%s  p|param starttime/endtime <TIME>;       │ Set time scan range (e.g. 2025-01-01 00:00:00)%s\n", COLOR_helpParam, C_RESET);
    printf("%s  p|param resmode tx|time;                │ Set recovery mode (Transaction/Time)%s\n", COLOR_helpParam, C_RESET);
    printf("%s  p|param restype delete|update;          │ Set recovery type (Delete/Update)%s\n", COLOR_helpParam, C_RESET);
    printf("%s  p|param exmode csv|sql;                 │ Set export format (default CSV)%s\n", COLOR_helpParam, C_RESET);
    printf("%s  p|param encoding utf8|gbk;              │ Set character encoding (default utf8)%s\n", COLOR_helpParam, C_RESET);
    printf("%s  reset <parameter>|all;                  │ Reset specified parameter|all parameter%s\n", COLOR_helpParam, C_RESET);
    printf("%s  show;                                   │ Display all parameters%s\n", COLOR_helpParam, C_RESET);
    printf("%s  t;                                      │ Display all supported datatypes %s\n", COLOR_helpParam, C_RESET);
    printf("%s└──────────────────────────────────────────────────────────────────────────────────────────────────┘%s\n", COLOR_UNLOAD, C_RESET);

    printf("\nSyntax Rules\n");
    printf("◈ All commands must end with `;`\n");
#endif
}

/**
 * infoBootstrap - Display bootstrap initialization progress
 *
 * @stage:          Current stage of bootstrap process (1-5)
 * @pgDatabaseFile: Path to pg_database file
 * @CUR_DB:         Current database name
 * @pgSchemaFile:   Path to pg_namespace file
 * @pgClassFile:    Path to pg_class file
 * @tabSize:        Number of tables
 * @pgAttrFile:     Path to pg_attribute file
 * @attroidsize:    Number of attribute records
 * @CUR_SCH:        Current schema name
 * @tabSizeOuput:   Number of tables in output
 *
 * Displays progress information during the database metadata initialization.
 */
void infoBootstrap(int stage,char *pgDatabaseFile,char *CUR_DB,char *pgSchemaFile,char *pgClassFile,
                    int tabSize,char *pgAttrFile,int attroidsize,char *CUR_SCH,int tabSizeOuput)
{
    char *bootPadding="  ";
    switch(stage){
        case 1:
            #ifdef EN
            printf("%s\nInitializing...%s\n",C_RED2,C_RESET);
            #elif defined(CN)
            printf("%s\nStart初始化...%s\n",C_RED2,C_RESET);
            #endif
            printf("%s -pg_database:<%s>%s\n",C_RED2,pgDatabaseFile,C_RESET);
            break;
        case 2:
            #ifdef CN
            printf("\n%s数据库:%s %s\n",C_WHITE2,CUR_DB,C_RESET);
            #else
            printf("\n%sDatabase:%s %s\n",C_WHITE2,CUR_DB,C_RESET);
            #endif
            break;
        case 3:
            printf("%s%s%s%s-pg_schema:<%s>%s\n",C_RED3,bootPadding,bootPadding,bootPadding,pgSchemaFile,C_RESET);
            #ifdef CN
            printf("%s%s%s%s-pg_class:<%s> 共%d行%s\n",C_RED3,bootPadding,bootPadding,bootPadding,pgClassFile,tabSize,C_RESET);
            printf("%s%s%s%s-pg_attribute:<%s> 共%d行%s\n",C_RED3,bootPadding,bootPadding,bootPadding,pgAttrFile,attroidsize,C_RESET);
            #else
            printf("%s%s%s%s-pg_class:<%s> %d Records%s\n",C_RED3,bootPadding,bootPadding,bootPadding,pgClassFile,tabSize,C_RESET);
            printf("%s%s%s%s-pg_attribute:<%s> %d Records%s\n",C_RED3,bootPadding,bootPadding,bootPadding,pgAttrFile,attroidsize,C_RESET);
            #endif
            break;
        case 4:
            #ifdef EN
            printf("%s%s%s%sSchema:\n",C_WHITE2,bootPadding,bootPadding,bootPadding,C_RESET);
            #else
            printf("%s%s%s%s模式:\n",C_WHITE2,bootPadding,bootPadding,bootPadding,C_RESET);
            #endif
            break;
        case 5:
            #ifdef CN
            printf("%s%s%s%s%s▌ %s %d张表%s\n",C_WHITE2,bootPadding,bootPadding,bootPadding,bootPadding,CUR_SCH,tabSizeOuput,C_RESET);
            #else
            printf("%s%s%s%s%s▌ %s %d tables%s\n",C_WHITE2,bootPadding,bootPadding,bootPadding,bootPadding,CUR_SCH,tabSizeOuput,C_RESET);
            #endif
    }

}

/**
 * infoUnlodResult - Display table unload result summary
 *
 * @tabname:     Table name
 * @oidpath:     OID file path
 * @nPages:      Number of pages processed
 * @nTotal:      Total number of records
 * @nItemsSucc:  Number of successfully processed items
 * @nItemsErr:   Number of failed items
 * @csvpath:     Output CSV file path
 */
void infoUnlodResult(char *tabname,char *oidpath,int nPages,int nTotal,int nItemsSucc,int nItemsErr,char *csvpath)
{
    #ifdef CN
    printf("\n%s▌ 解析完成%s\n", C_BLUE2, C_RESET);
    printf("%s┌─────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);
    printf("%s 表 %s(%s)%s\n", COLOR_UNLOAD,tabname,oidpath,C_RESET);
    printf("%s ● 数据页: %-10d     ● 共计 %d 行%s\n", COLOR_UNLOAD,nPages,nTotal,C_RESET);
    printf("%s ● 成功: %s%-10d%s   ● 失败: %s%d%s%s\n", COLOR_UNLOAD,COLOR_SUCC,nItemsSucc,C_RESET,COLOR_ERROR,nItemsErr,C_RESET, C_RESET);
    printf("%s ● 文件路径: %s\n", COLOR_UNLOAD,csvpath, C_RESET);
    printf("%s└─────────────────────────────────────────────────────────┘%s\n", COLOR_UNLOAD, C_RESET);
    #else
    printf("\n%s▌ Parse Complete%s\n", C_BLUE2, C_RESET);
    printf("%s┌─────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);
    printf("%s Table %s(%s)%s\n", COLOR_UNLOAD,tabname,oidpath,C_RESET);
    printf("%s ● Pages: %-10d     ● %d Records in total%s\n", COLOR_UNLOAD,nPages,nTotal,C_RESET);
    printf("%s ● Success: %s%-10d%s   ● Faliure: %s%d%s%s\n", COLOR_UNLOAD,COLOR_SUCC,nItemsSucc,C_RESET,COLOR_ERROR,nItemsErr,C_RESET, C_RESET);
    printf("%s ● File Path: %s\n", COLOR_UNLOAD,csvpath, C_RESET);
    printf("%s└─────────────────────────────────────────────────────────┘%s\n", COLOR_UNLOAD, C_RESET);
    #endif
}

/**
 * infoDBHeader - Print database list header
 *
 * Displays formatted header for database listing.
 */
void infoDBHeader()
{
    printf("\n%s┌───────────────────┐%s\n",COLOR_DB,C_RESET);
    #ifdef CN
    printf("%s│%-4s 数据库名 %-3s  │%s\n",COLOR_DB,"","",C_RESET);
    #else
    printf("%s│%-4s Database %-4s │%s\n",COLOR_DB,"","",C_RESET);
    #endif
    printf("%s├───────────────────┤%s\n",COLOR_DB,C_RESET);
}

/**
 * infoTabHeader - Print table list header
 *
 * Displays formatted header for table listing.
 */
void infoTabHeader()
{
    #ifdef CN
    printf("\n%s┌──────────────────────────────────────────────────┐%s\n",COLOR_TABLE,C_RESET);
    printf("%s│%-14s 表名 %-14s   │%-2s表大小 %-3s│%s\n",COLOR_TABLE,"","","","",C_RESET);
    printf("%s├──────────────────────────────────────────────────┤%s\n",COLOR_TABLE,C_RESET);
    #else
    printf("%s┌──────────────────────────────────────────────────┐%s\n",COLOR_TABLE,C_RESET);
    printf("%s|%-14sTablename%-14s|%-4sSize%-4s|%s\n",COLOR_TABLE,"","","","",C_RESET);
    printf("%s├──────────────────────────────────────────────────┤%s\n",COLOR_TABLE,C_RESET);
    #endif
}

/**
 * infoDescHeader - Print table description header
 *
 * @flag: Header format flag
 *
 * Displays formatted header for table structure description.
 */
void infoDescHeader(int flag)
{
    if(flag == 1){
        #ifdef CN
        printf("\n%s┌──────────────────────────────────────────────────────────────┐%s\n",COLOR_TABLE,C_RESET);
        printf("%s│                            建表语句                           │%s\n",COLOR_TABLE,C_RESET);
        printf("%s└──────────────────────────────────────────────────────────────┘%s\n",COLOR_TABLE,C_RESET);
        #else
        printf("%s┌──────────────────────────────────────────────────────────────┐%s\n",COLOR_TABLE,C_RESET);
        printf("%s│                              DDL                             │%s\n",COLOR_TABLE,C_RESET);
        printf("%s└──────────────────────────────────────────────────────────────┘%s\n",COLOR_TABLE,C_RESET);
        #endif
    }
    else{
        #ifdef CN
        printf("\n%s┌──────────────────────────────────────────────────────────────┐%s\n",COLOR_TABLE,C_RESET);
        printf("%s│                            列类型                            │%s\n",COLOR_TABLE,C_RESET);
        printf("%s└──────────────────────────────────────────────────────────────┘%s\n",COLOR_TABLE,C_RESET);
        #else
        printf("%s┌──────────────────────────────────────────────────────────────┐%s\n",COLOR_TABLE,C_RESET);
        printf("%s│                        Attributes types                      │%s\n",COLOR_TABLE,C_RESET);
        printf("%s└──────────────────────────────────────────────────────────────┘%s\n",COLOR_TABLE,C_RESET);
        #endif
    }
}

/**
 * infoSchHeader - Print schema list header
 *
 * Displays formatted header for schema listing.
 */
void infoSchHeader()
{
    #ifdef CN
    printf("%s\n┌────────────────────────────────────────┐%s\n",COLOR_SCHEMA,C_RESET);
    printf("%s│%-9s 模式 %-9s   │%-2s表数量 %-3s│%s\n",COLOR_SCHEMA,"","","","",C_RESET);
    printf("%s├────────────────────────────────────────┤%s\n",COLOR_SCHEMA,C_RESET);
    #else
    printf("%s┌────────────────────────────────────────┐%s\n",COLOR_SCHEMA,C_RESET);
    printf("%s│%-9s Schema %-10s│%-2sTab Num %-2s│%s\n",COLOR_SCHEMA,"","","","",C_RESET);
    printf("%s├────────────────────────────────────────┤%s\n",COLOR_SCHEMA,C_RESET);
    #endif
}

/**
 * infoParamHeader - Print parameter header
 *
 * Displays formatted header for parameter listing.
 */
void infoParamHeader()
{
    #ifdef CN
    printf("%s\n┌─────────────────────────────────────────────────────────────────┐%s\n",COLOR_PARAM,C_RESET);
    printf("%s│              %-22s│             %-20s│%s\n",COLOR_PARAM,"参数","当前值",C_RESET);
    printf("%s├─────────────────────────────────────────────────────────────────┤%s\n",COLOR_PARAM,C_RESET);
    #else
    printf("%s┌─────────────────────────────────────────────────────────────────┐%s\n",COLOR_PARAM,C_RESET);
    printf("%s│            %-22s│             %-17s│%s\n",COLOR_PARAM,"parameter","value",C_RESET);
    printf("%s├─────────────────────────────────────────────────────────────────┤%s\n",COLOR_PARAM,C_RESET);
    #endif
}

/**
 * infoTxScanResult - Display transaction scan result
 *
 * @elem:   DELETE structure containing scan results
 * @resStr: Result type string
 *
 * Prints transaction scan summary for recovery operations.
 */
void infoTxScanResult(DELstruct *elem,char *resStr)
{
    char *timestp = (char *)timestamptz_to_str_og(elem->txtime);

    int tx = elem->tx;
    char *oid = elem->datafile;
    char *toastoid = elem->toast;
    int rec = elem->delCount;
    char *startLSN = elem->startLSN;
    char *startwal = elem->startwal;
    char *endLSN = elem->endLSN;
    char *endwal = elem->endwal;

    #ifdef CN
    printf("\n%s▌ 事务详情%s\n", C_SUBTITLE, C_RESET);
    printf("%s┌─────────────────────────────────────────────────────────┐%s\n", C_GREY2, C_RESET);
    printf("%s 时间戳: %s%s\n", C_WHITE2,timestp,C_RESET);
    printf("%s LSN: %s - %s    %s\n", C_WHITE2,startLSN,endLSN,C_RESET);
    printf("%s 建议startwal: %s%s%s    %s\n", C_WHITE2,C_FILEPATH,startwal,C_RESET,C_RESET);
    printf("%s 建议endwal: %s%s%s    %s\n", C_WHITE2,C_FILEPATH,endwal,C_RESET,C_RESET);
    printf("%s       -------------------.--------------------%s\n",C_WHITE2,C_RESET);
    printf("%s ● 事务号: %-10d     ● 该事务%s%s%s的数据量: %s%d%s 行%s\n", C_WHITE2,tx,COLOR_WARNING,resStr,C_RESET,COLOR_WARNING,rec,C_RESET,C_RESET);
    printf("%s ● 数据文件OID: %-10s● Toast文件OID: %s%s\n", C_WHITE2,oid,toastoid, C_RESET);
    printf("%s└─────────────────────────────────────────────────────────┘%s\n", C_GREY2, C_RESET);
    #else
    printf("\n%s▌ Tx Details%s\n", C_SUBTITLE, C_RESET);
    printf("%s┌─────────────────────────────────────────────────────────┐%s\n", C_GREY2, C_RESET);
    printf("%s Timestamp: %s%s\n", C_WHITE2,timestp,C_RESET);
    printf("%s LSN: %s - %s    %s\n", C_WHITE2,startLSN,endLSN,C_RESET);
    printf("%s Recommanded startwal: %s%s%s    %s\n", C_WHITE2,C_FILEPATH,startwal,C_RESET,C_RESET);
    printf("%s Recommanded endwal: %s%s%s    %s\n", C_WHITE2,C_FILEPATH,endwal,C_RESET,C_RESET);
    printf("%s       -------------------.--------------------%s\n",C_WHITE2,C_RESET);
    printf("%s ● Tx Number: %-10d   ● Records %s%s%s by the TX: %s%d%s %s\n", C_WHITE2,tx,COLOR_WARNING,resStr,C_RESET,COLOR_WARNING,rec,C_RESET,C_RESET);
    printf("%s ● Datafiel OID: %-10s● Toastfile OID: %s%s\n", C_WHITE2,oid,toastoid, C_RESET);
    printf("%s└─────────────────────────────────────────────────────────┘%s\n", C_GREY2, C_RESET);
    #endif
}

/**
 * infoScanDropResult - Display drop scan result
 *
 * @elem: Drop element containing scan results
 *
 * Prints summary of dropped table scan operation.
 */
void infoScanDropResult(dropElem *elem)
{
    char *timestp = (char *)timestamptz_to_str_og(elem->timestamp);

    TransactionId tx = elem->Tx;
    int oid = elem->filenode;
    char *tabname = elem->tabname;

    #ifdef CN
    printf("\n%s▌ 事务详情%s\n", C_SUBTITLE, C_RESET);
    printf("%s┌─────────────────────────────────────────────────────────┐%s\n", C_GREY2, C_RESET);
    printf("%s 时间戳: %s%s\n", C_WHITE2,timestp,C_RESET);
    printf("%s       -------------------.--------------------%s\n",C_WHITE2,C_RESET);
    printf("%s ● 表名:%s %s%s%s     \n",C_WHITE2,C_RESET, COLOR_ERROR,tabname,C_RESET);
    printf("%s ● 事务号: %-10d● 文件filenode: %d%s\n", C_WHITE2,tx,oid, C_RESET);
    printf("%s└─────────────────────────────────────────────────────────┘%s\n", C_GREY2, C_RESET);
    #else
    printf("\n%s▌ Tx Details%s\n", C_SUBTITLE, C_RESET);
    printf("%s┌─────────────────────────────────────────────────────────┐%s\n", C_GREY2, C_RESET);
    printf("%s Timestamp: %s%s\n", C_WHITE2,timestp,C_RESET);
    printf("%s       -------------------.--------------------%s\n",C_WHITE2,C_RESET);
    printf("%s ● Table:%s %s%s%s     \n",C_WHITE2,C_RESET, COLOR_ERROR,tabname,C_RESET);
    printf("%s ● Tx Number: %-10d● datafile: %d%s\n", C_WHITE2,tx,oid, C_RESET);
    printf("%s└─────────────────────────────────────────────────────────┘%s\n", C_GREY2, C_RESET);
    #endif
}

/**
 * infoTimeScanResult - Display time-based scan result
 *
 * @elem:    DELETE structure containing scan results
 * @resStr:  Result type string
 * @SrtTime: Start time of scan range
 * @EndTime: End time of scan range
 *
 * Prints time-ranged scan summary for recovery operations.
 */
void infoTimeScanResult(DELstruct *elem,char *resStr,TimestampTz *SrtTime,TimestampTz *EndTime)
{
    char *startstmp = (char *)timestamptz_to_str_og(*SrtTime);
    char *endstmp = (char *)timestamptz_to_str_og(*EndTime);
    char *oid = elem->datafile;
    char *toastoid = elem->toast;
    char *startLSN = elem->startLSN;
    char *startwal = elem->startwal;
    char *endLSN = elem->endLSN;
    char *endwal = elem->endwal;
    int totalCount = elem->delCount;

    #ifdef CN
    printf("\n%s▌ 时间区间详情%s\n", C_SUBTITLE, C_RESET);
    printf("%s┌─────────────────────────────────────────────────────────┐%s\n", C_GREY2, C_RESET);
    printf("%s 开始时间: %s%s\n", C_WHITE2,startstmp,C_RESET);
    printf("%s 结束时间: %s%s\n", C_WHITE2,endstmp,C_RESET);
    printf("%s LSN: %s - %s    %s\n", C_WHITE2,startLSN,endLSN,C_RESET);
    printf("%s 建议startwal: %s%s%s    %s\n", C_WHITE2,C_FILEPATH,startwal,C_RESET,C_RESET);
    printf("%s 建议endwal: %s%s%s    %s\n", C_WHITE2,C_FILEPATH,endwal,C_RESET,C_RESET);
    printf("%s       -------------------.--------------------%s\n",C_WHITE2,C_RESET);
    printf("%s ● 数据文件OID: %-10s● Toast文件OID: %s%s\n", C_WHITE2,oid,toastoid, C_RESET);
    printf("%s ● 该区间内%s%s%s的数据量: %s%d%s 行%s\n", C_WHITE2,COLOR_WARNING,resStr,C_RESET,COLOR_WARNING,totalCount,C_RESET,C_RESET);
    printf("%s└─────────────────────────────────────────────────────────┘%s\n", C_GREY2, C_RESET);
    #else
    printf("\n%s▌ Time Range Details%s\n", C_SUBTITLE, C_RESET);
    printf("%s┌─────────────────────────────────────────────────────────┐%s\n", C_GREY2, C_RESET);
    printf("%s Start Time: %s%s\n", C_WHITE2,startstmp,C_RESET);
    printf("%s End Time: %s%s\n", C_WHITE2,endstmp,C_RESET);
    printf("%s LSN: %s - %s    %s\n", C_WHITE2,startLSN,endLSN,C_RESET);
    printf("%s Recommanded startwal: %s%s%s    %s\n", C_WHITE2,C_FILEPATH,startwal,C_RESET,C_RESET);
    printf("%s Recommanded endwal: %s%s%s    %s\n", C_WHITE2,C_FILEPATH,endwal,C_RESET,C_RESET);
    printf("%s       -------------------.--------------------%s\n",C_WHITE2,C_RESET);
    printf("%s ● Datafile OID: %-10s● Toasfile OID: %s%s\n", C_WHITE2,oid,toastoid, C_RESET);
    printf("%s ● Records %s%s%s %sin the Time Range:%s %s%d%s%s\n", C_WHITE2,COLOR_WARNING,resStr,C_RESET,COLOR_UNLOAD,C_RESET,COLOR_WARNING,totalCount,C_RESET,C_RESET);
    printf("%s└─────────────────────────────────────────────────────────┘%s\n", C_GREY2, C_RESET);
    #endif
}

/**
 * infoRestoreResult - Display restore operation result
 *
 * @tabname:       Table name
 * @nTotal:        Total records
 * @nItemsSucc:    Successful records
 * @nItemsErr:     Failed records
 * @FPIUpdateSame: FPI update count
 * @csvpath:       Output file path
 * @resTyp_there:  Result type
 *
 * Prints summary of data restore operation.
 */
void infoRestoreResult(char *tabname,int nTotal,int nItemsSucc,int nItemsErr,int FPIUpdateSame,char *csvpath,int resTyp_there)
{
    #ifdef CN
    printf("\n%s▌ 解析完成%s\n", C_BLUE2, C_RESET);
    printf("%s┌───────────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);
    printf("%s 表 %s%s\n", COLOR_UNLOAD,tabname,C_RESET);
    printf("%s ● 恢复数据共计 %d 行%s\n", COLOR_UNLOAD,nTotal,C_RESET);
    if(resTyp_there == DELETEtyp)
        printf("%s ● 成功: %s%-10d%s ● 失败: %s%d%s%s\n", COLOR_UNLOAD,COLOR_SUCC,nItemsSucc,C_RESET,COLOR_ERROR,nItemsErr,C_RESET, C_RESET);
    else if (resTyp_there == UPDATEtyp)
        printf("%s ● 成功: %s%-10d%s ● update前后值不变: %s%d%s ● 失败: %s%d%s%s\n", COLOR_UNLOAD,COLOR_SUCC,nItemsSucc,C_RESET,COLOR_SUCC,FPIUpdateSame,C_RESET,COLOR_ERROR,nItemsErr,C_RESET, C_RESET);
    printf("%s ● 文件路径: %s\n", COLOR_UNLOAD,csvpath, C_RESET);
    printf("%s└───────────────────────────────────────────────────────────────┘%s\n", COLOR_UNLOAD, C_RESET);
    #else
    printf("\n%s▌ Restore Complete%s\n", C_BLUE2, C_RESET);
    printf("%s┌───────────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);
    printf("%s Table <%s>%s\n", COLOR_UNLOAD,tabname,C_RESET);
    printf("%s ● Records Restored: %d%s\n", COLOR_UNLOAD,nTotal,C_RESET);
    if(resTyp_there == DELETEtyp)
        printf("%s ● Success: %s%-10d%s ● Failure: %s%d%s%s\n", COLOR_UNLOAD,COLOR_SUCC,nItemsSucc,C_RESET,COLOR_ERROR,nItemsErr,C_RESET, C_RESET);
    else if (resTyp_there == UPDATEtyp)
        printf("%s ● Success:%s %s%-10d%s %s● Same Values for Updates:%s %s%d%s %s● Failure:%s %s%d%s\n",COLOR_UNLOAD,C_RESET,COLOR_SUCC,nItemsSucc,C_RESET,COLOR_UNLOAD,C_RESET,COLOR_SUCC,FPIUpdateSame,C_RESET,COLOR_UNLOAD,C_RESET,COLOR_ERROR,nItemsErr,C_RESET);
    printf("%s ● File Path: %s\n", COLOR_UNLOAD,csvpath, C_RESET);
    printf("%s└───────────────────────────────────────────────────────────────┘%s\n", COLOR_UNLOAD, C_RESET);
    #endif
}

/**
 * infoRestoreMode - Display current restore mode
 *
 * @item: Mode description string
 *
 * Shows the active restore mode setting.
 */
void infoRestoreMode(char *item)
{
    printf("%s%s%s\n",COLOR_SUCC,item,C_RESET);
    printf("%s────────────────────────────────────────%s\n",C_GREY1,C_RESET);
}

/**
 * infoWalRange - Display WAL file range
 *
 * @start_archfilename: Starting WAL file name
 * @end_archfilename:   Ending WAL file name
 *
 * Shows the WAL file range being processed.
 */
void infoWalRange(char *start_archfilename,char *end_archfilename)
{
    #ifdef CN
    printf("\n%s▌ 扫描归档目录%s\n", C_SUBTITLE, C_RESET);
    printf("%s   起始文件: %s%s\n", C_FILEPATH,start_archfilename,C_RESET);
    printf("%s   终点文件: %s%s\n\n", C_FILEPATH,end_archfilename,C_RESET);
    #else
    printf("\n%s▌ Scanning Archived Wal Directory%s\n", C_SUBTITLE, C_RESET);
    printf("%s   StartWal: %s%s\n", C_FILEPATH,start_archfilename,C_RESET);
    printf("%s   EndWal: %s%s\n\n", C_FILEPATH,end_archfilename,C_RESET);
    #endif
}

/**
 * infoTimeRange - Display time range for recovery
 *
 * @start: Start time string
 * @end:   End time string
 *
 * Shows the time range for recovery operation.
 */
void infoTimeRange(char *start,char *end)
{
    #ifdef CN
    printf("%s▌ 扫描结束,当前时间范围%s\n",C_BLUE2,C_RESET);
    printf("  %s开始: %s%s\n",C_TIMESTAMP,start,C_RESET);
    printf("  %s结束: %s%s\n",C_TIMESTAMP,end,C_RESET);
    #else
    printf("%s▌ End of Scanning, current time range:%s\n",C_BLUE2,C_RESET);
    printf("  %sStart: %s%s\n",C_TIMESTAMP,start,C_RESET);
    printf("  %sEnd: %s%s\n",C_TIMESTAMP,end,C_RESET);
    #endif
}

/**
 * infoRestoreRecs - Display FPI record count
 *
 * @FPIcount: Number of FPI records
 *
 * Shows count of Full Page Image records found.
 */
void infoRestoreRecs(int FPIcount)
{
    #ifdef CN
    printf("\r%s|-已解析数据条数: %s%d%s%s", C_WHITE2,COLOR_SUCC,FPIcount,C_RESET,C_RESET);
    #else
    printf("\r%s|-Records Parsed: %s%d%s%s", C_WHITE2,COLOR_SUCC,FPIcount,C_RESET,C_RESET);
    #endif
}

/**
 * InfoStartwalMeaning - Explain startwal parameter
 *
 * Displays help information about the startwal parameter.
 */
void InfoStartwalMeaning()
{
    #ifdef CN
    printf("%s\n  [!] 提示: 建议startwal仅表示该事务恢复时建议将startwal设置为该值%s\n",COLOR_VERSION,C_RESET);
    printf("%s            建议endwal表示该事务恢复时必须将startwal设置为该值，否则恢复可能会失败%s\n\n",COLOR_VERSION,C_RESET);
    #else
    printf("%s\n  [!] Note: The 'Recommended startwal' indicates the suggested value to set for startwal during transaction recovery%s\n",COLOR_VERSION,C_RESET);
    printf("%s            The 'Recommended endwal' mandates that startwal must be set to this value, otherwise recovery may fail%s\n\n",COLOR_VERSION,C_RESET);
    #endif
}

/**
 * infoDecodeLive - Display live decode progress
 *
 * @bootFileName: Current file being processed
 * @nPages:       Number of pages processed
 * @nItemsSucc:   Successful records count
 *
 * Shows real-time decode progress information.
 */
void infoDecodeLive(char *bootFileName,int nPages,int nItemsSucc)
{
    #ifdef EN
    printf("\r%sTable <%s>. Pages Parsed: %d, Records Parsed: %d%s",COLOR_UNLOAD, bootFileName,nPages,nItemsSucc,C_RESET);
    #else
    printf("\r%s正在解析表 <%s>. 已解析数据页: %d, 已解析数据: %d 条%s", COLOR_UNLOAD,bootFileName,nPages,nItemsSucc,C_RESET);
    #endif
}

/**
 * infoUCopySucc - Display COPY command generation success
 *
 * @copyFilename: Generated COPY file name
 * @file_count:   Number of files generated
 *
 * Shows success message for COPY command generation.
 */
void infoUCopySucc(char *copyFilename,int file_count)
{
    printf("%s┌───────────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);
    #ifdef EN
    printf("%s  COPY Export Completed\n ● File Path:%s  ● CSV Files Number: %d%s\n",COLOR_UNLOAD,copyFilename,file_count,C_RESET);
    #else
    printf("%s  COPY命令导出完成\n ● 文件路径: %s  ● CSV文件个数: %d%s\n",COLOR_UNLOAD,copyFilename,file_count,C_RESET);
    #endif
    printf("%s└───────────────────────────────────────────────────────────────┘%s\n", COLOR_UNLOAD, C_RESET);
}

void infoUddlSucc(char *ddlFilename,int tabSize)
{
    printf("%s┌───────────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);
    #ifdef EN
    printf("%s  DDL Export Completed\n ● Path:%s  ● Table Number: %d%s\n",COLOR_UNLOAD,ddlFilename,tabSize,C_RESET);
    #else
    printf("%s  DDL导出完成\n ● 文件路径: %s  ● 共计 %d 张表%s\n",COLOR_UNLOAD,ddlFilename,tabSize,C_RESET);
    #endif
    printf("%s└───────────────────────────────────────────────────────────────┘%s\n", COLOR_UNLOAD, C_RESET);

}

void infoUSchSucc(char *schemaname,int tabSize,int nNodata,int nErr,char *logPathErr,char *logPathSucc)
{
    printf("\n\n%s┌───────────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);
    #ifdef EN
    printf("%s\t\tSchema <%s> %d tables in total\n\t● Success: %s%d%s  ● Empty table: %s%d%s  ● Failure: %s%d%s \n  ● Success Log：%s\n  ● Failure Log：%s%s\n",COLOR_UNLOAD,schemaname,tabSize,COLOR_SUCC,tabSize-nErr-nNodata,COLOR_UNLOAD,C_SUBTITLE,nNodata,COLOR_UNLOAD,COLOR_ERROR,nErr,COLOR_UNLOAD,logPathErr,logPathSucc,C_RESET);
    #else
    printf("%s  \t\t     模式<%s>共 %d 张表\n  \t\t  ● 成功：%s%d%s  ● 无数据：%s%d%s  ● 失败：%s%d%s \n  ● 成功日志: %s\n  ● 失败日志: %s%s\n",COLOR_UNLOAD,schemaname,tabSize,COLOR_SUCC,tabSize-nErr-nNodata,COLOR_UNLOAD,C_SUBTITLE,nNodata,COLOR_UNLOAD,COLOR_ERROR,nErr,COLOR_UNLOAD,logPathErr,logPathSucc,C_RESET);
    #endif
    printf("%s└───────────────────────────────────────────────────────────────┘%s\n", COLOR_UNLOAD, C_RESET);

}

void infoPhysicalBlkHeader(char *filename,uint32 st_size,int flag)
{
    #ifdef CN
    if(flag == 0){
        printf("%s数据文件: %s%s\n",COLOR_UNLOAD,filename,C_RESET);
        printf("%s数据文件大小: %ld bytes%s\n",COLOR_UNLOAD, st_size,C_RESET);
        printf("%s物理块分布:%s\n",COLOR_UNLOAD,C_RESET);
    }
    else{
        printf("%sTOAST文件: %s%s\n",COLOR_UNLOAD, filename,C_RESET);
        printf("%sTOAST文件大小: %ld bytes%s\n",COLOR_UNLOAD, st_size,C_RESET);
        printf("%s物理块分布:\n%s",COLOR_UNLOAD,C_RESET);
    }
    #else
    if(flag == 0){
        printf("%sDatafile: %s%s\n",COLOR_UNLOAD,filename,C_RESET);
        printf("%sDatafile size: %ld bytes%s\n",COLOR_UNLOAD, st_size,C_RESET);
        printf("%sPhysical block distribution:%s\n",COLOR_UNLOAD,C_RESET);
    }
    else{
        printf("%sTOAST file: %s%s\n",COLOR_UNLOAD, filename,C_RESET);
        printf("%sTOAST file size: %ld bytes%s\n",COLOR_UNLOAD, st_size,C_RESET);
        printf("%sPhysical block distribution:\n%s",COLOR_UNLOAD,C_RESET);
    }
    #endif
}

void infoPhysycalBlkContect(long long a,long long b,long long c,int i)
{
    #ifdef CN
    printf("%s  段号 %-4d: 字节偏移量 %-10lld - %lld (PG数据块数量: %-5lld)%s\n",
        COLOR_UNLOAD,
        i + 1,
        a,
        b,
        c,
        C_RESET);
    #else
    printf("%s  Extent %-5d: Byte Offsets %lld - %lld (Length: %lld blocks)%s\n",
        COLOR_UNLOAD,
        i + 1,
        a,
        b,
        c,
        C_RESET);
    #endif
}

void infoDirCleaned(char *dir)
{
    #ifdef CN
    printf("%s路径 [%s] 清理完毕!%s\n",COLOR_SUCC,dir,C_RESET);
    #else
    printf("%sPath [%s] Cleaned!%s\n",COLOR_SUCC,dir,C_RESET);
    #endif
}

void infoDropScanHeader(char *header,char *color)
{
    #ifdef CN
    printf("%s\n ▌%s \n%s",color,header,C_RESET);
    printf("%s┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);
    printf("%s%-10s表名 %-10s│%-35s结果 %-35s%s\n",COLOR_UNLOAD,"","","","",C_RESET);
    printf("%s├────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤%s",COLOR_UNLOAD,C_RESET);
    #else
    printf("%s\n ▌%s \n%s",color,header,C_RESET);
    printf("%s┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);
    printf("%s%-7sTableName %-7s│%-35sResult %-35s%s\n",COLOR_UNLOAD,"","","","",C_RESET);
    printf("%s├────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤%s",COLOR_UNLOAD,C_RESET);
    #endif
}

void infoRestoreDB()
{
    #ifdef CN
    printf("%s[初始化中] 正在恢复数据库 'testdb' 的表结构...%s\n",COLOR_UNLOAD,C_RESET);
    #else
    printf("%s[Bootstraping] Restoring the metadata of database 'testdb'...%s\n",COLOR_UNLOAD,C_RESET);
    #endif
}

void infoDsIdxHeader()
{
    #ifdef CN
    printf("%s=== 磁盘扫描进度监控 ===%s\n",COLOR_UNLOAD,C_RESET);
    #else
    printf("%s=== Disk Scan Process Monitor ===%s\n",COLOR_UNLOAD,C_RESET);
    #endif
}

void infoIdxScanModeHeader()
{
    #ifdef CN
    printf("%s\n ▌索引扫描恢复模式 \n%s",COLOR_WARNING,C_RESET);
    #else
    printf("%s\n ▌Index Scan Recovery Mode \n%s",COLOR_WARNING,C_RESET);
    #endif
}

void infoIsoScanModeHeader()
{
    #ifdef CN
    printf("%s\n ▌镜像扫描恢复模式 \n%s",COLOR_WARNING,C_RESET);
    #else
    printf("%s\n ▌Image Scan Recovery Mode \n%s",COLOR_WARNING,C_RESET);
    #endif
}

void infoDsIdxInfoHeader()
{
    #ifdef CN
    printf("\n%s▌ 开始执行索引获取%s\n", COLOR_SUCC, C_RESET);
    printf("%s┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐%s\n", C_GREY2, C_RESET);
    printf("%s ● 获取索引时会将未被drop的数据页排除在外\n", COLOR_WARNING,C_RESET);
    printf("%s└─────────────────────────────────────────────────────────────────────────────────────────────────────┘%s\n", C_GREY2, C_RESET);
    #else
    printf("\n%s▌ Starting index retrieval%s\n", COLOR_SUCC, C_RESET);
    printf("%s┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐%s\n", C_GREY2, C_RESET);
    printf("%s ● Undropped data pages will be excluded during index retrieval\n", COLOR_WARNING,C_RESET);
    printf("%s└─────────────────────────────────────────────────────────────────────────────────────────────────────┘%s\n", C_GREY2, C_RESET);
    #endif
}

void warningUseDBFirst()
{
    #ifdef EN
    printf("%splease choose current database by <use> command%s\n",COLOR_WARNING,C_RESET);
    #else
    printf("%s请用<use>命令选择当前数据库%s\n",COLOR_WARNING,C_RESET);
    #endif
}

void warningInvalidLp()
{
    #ifdef CN
    printf("%s\n无效的行指针，建议扩大wal范围%s\n",COLOR_WARNING,C_RESET);
    #else
    printf("%s\nInvalid Line Poiner,bigger range of walfile is recommanded%s\n",COLOR_WARNING,C_RESET);
    #endif
}

void ErrUnknownParam(char *latter)
{
    #ifdef CN
    printf("%s未知的参数<%s>%s\n",COLOR_ERROR,latter,C_RESET);
    #else
    printf("%sunknown parameter <%s>%s\n",COLOR_ERROR,latter,C_RESET);
    #endif
}

void ErrorWalname()
{
    #ifdef CN
    printf("%sWAL文件名有误 ,请重新输入%s\n",COLOR_ERROR,C_RESET);
    #else
    printf("%sWRONG WAL FILENAME,please type again%s\n",COLOR_ERROR,C_RESET);
    #endif
}

void ErrorDropScanOff()
{
    #ifdef CN
    printf("%s偏移量必须是4096的整数倍%s\n",COLOR_ERROR,C_RESET);
    #else
    printf("%sOffset must be %s\n",COLOR_ERROR,C_RESET);
    #endif
}

void ErrorArchivePath(char *initArchPath)
{
    #ifdef CN
    printf("%s归档路径<%s>中的WAL文件名有误 ,请确认pdu.ini中的路径%s\n",COLOR_ERROR,initArchPath,C_RESET);
    #else
    printf("%sWAL filename in Archive Path <%s> is wrong ,please checkout pdu.ini%s\n",COLOR_ERROR,initArchPath,C_RESET);
    #endif
}

void ErrorArchPathNotExist(char *path)
{
    #ifdef EN
    printf("%sARCHIVE_DEST <%s> Does not Exist,please checkout pdu.ini%s\n",COLOR_ERROR,path,C_RESET);
    #else
    printf("%s归档路径 <%s> 不存在 ,请确认pdu.ini中的路径%s\n",COLOR_ERROR,path,C_RESET);
    #endif
}

typedef uint64 XLogRecPtr;
void ErrBlkNotFound(int blk,XLogRecPtr lsn)
{
    #ifdef CN
    printf("%s\n位置: %s%d%s %s未找到数据块 %s%d%s %s请扩大WAL日志的搜索范围或搜索时间%s\n",COLOR_ERROR,COLOR_UNLOAD,lsn,C_RESET,COLOR_ERROR,COLOR_UNLOAD,blk,C_RESET,COLOR_ERROR,C_RESET);
    #else
    printf("%s\nBlock %s%d%s %sNot Found, Please Increase WAL Range or Time Range%s\n",COLOR_ERROR,COLOR_UNLOAD,blk,C_RESET,COLOR_ERROR,C_RESET);
    #endif
}

void ErrorBlkLsnInfo(parray *LsnBlkInfos)
{
    int cnt = parray_num(LsnBlkInfos);
    if(cnt != 0){
        printf("%s未Search到如下数据块\n%s",COLOR_ERROR,C_RESET);
        printf("%s┌───────────────────────────────────────────────────────────────┐%s\n", COLOR_UNLOAD, C_RESET);
        for (int i = 0; i < cnt; i++)
        {
            LsnBlkInfo *elem = parray_get(LsnBlkInfos,i);
            if(elem->isFatal == 0){
                printf("%s %s-%d%s",COLOR_UNLOAD,elem->LSN,elem->blk,C_RESET);
                if((i+1) % 3 == 0){
                    printf("\n");
                }
            }
        }

        printf("%s\n└───────────────────────────────────────────────────────────────┘%s\n", COLOR_UNLOAD, C_RESET);
    }
}

void ErrLpIsEmpty(OffsetNumber offnum,BlockNumber blk){
    #ifdef CN
    printf("%s数据块%d的第%d偏移位置指针为空，请联系开发者排查%s",COLOR_ERROR,blk,offnum,C_RESET);
    #else
    printf("%slinepointer in offnum %d of block %d is empty,please contact PDU developper%s",COLOR_ERROR,offnum,blk,C_RESET);
    #endif
}

void ErrorUpdDelWrong(int flag)
{
    if(flag == 0){
        #ifdef CN
        printf("%s在UPDATE恢复下请使用restore upd 命令%s\n",COLOR_ERROR,C_RESET);
        #else
        printf("%sPlease use <restore upd> command when restore UPDATE%s\n",COLOR_ERROR,C_RESET);
        #endif
    }
    else{
        #ifdef CN
        printf("%s在DELETE恢复下请使用restore del 命令%s\n",COLOR_ERROR,C_RESET);
        #else
        printf("%sPlease use <restore del> command when restore DELETE%s\n",COLOR_ERROR,C_RESET);
        #endif
    }
}

void ErrorFileNotExist(char *filename)
{
    #ifdef CN
    printf("%s[%s] 不存在%s\n",COLOR_ERROR,filename,C_RESET);
    #else
    printf("%s[%s] does not exist%s\n",COLOR_ERROR,filename,C_RESET);
    #endif
}

void ErrorSchNotExist(char *schname)
{
    #ifdef EN
    printf("%sUnknown schema <%s>%s\n",COLOR_ERROR,schname,C_RESET);
    #else
    printf("%s不存在的模式 <%s>%s\n",COLOR_ERROR,schname,C_RESET);
    #endif
}

void ErrorTabNotExist(char *tabname)
{
    #ifdef EN
    printf("%sUnknown tablename <%s>%s\n",COLOR_ERROR,tabname,C_RESET);
    #else
    printf("%s不存在的表名 <%s>%s\n",COLOR_ERROR,tabname,C_RESET);
    #endif
}

void ErrorToastNoExist(Oid toastname)
{
    #ifdef CN
    printf("%soid为%d的toast文件不存在，所有相关数据将无法解析\n%s",C_YELLOW3,toastname,C_RESET);
    #else
    printf("%stoast file %d does not exist, all related records will not be parsed\n%s",C_YELLOW3,toastname,C_RESET);
    #endif
}

void ErrorDiskPathNotSet()
{
    #ifdef CN
    printf("%spdu.ini中未设置需要扫描的DISK_PATH%s\n",COLOR_WARNING,C_RESET);
    #else
    printf("%sNo DISK_PATH Parameter Found in pdu.ini%s\n",COLOR_WARNING,C_RESET);
    #endif
}

void ErrorOpenDisk(char *diskPath)
{
    #ifdef CN
    printf("%s无法打开磁盘设备 %s%s\n",COLOR_ERROR,diskPath,C_RESET);
    #else
    printf("%sUnable to open device %s%s\n",COLOR_ERROR,diskPath,C_RESET);
    #endif
}

void ErrorISONotExist()
{
    #ifdef CN
    printf("%s镜像文件不存在，请打开 isomode 参数并重新执行ds idx%s\n",COLOR_ERROR,C_RESET);
    #else
    printf("%sISO file does not exist, please turn on parameter <isomode> and execute <ds idx> again%s\n",COLOR_ERROR,C_RESET);
    #endif
}