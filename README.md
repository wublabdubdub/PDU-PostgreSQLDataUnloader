# PDU - PostgreSQL Data Unloader

[![website](https://img.shields.io/badge/website-pduzc.com-0a6fe8?style=flat-square&logo=firefoxbrowser&logoColor=white)](https://pduzc.com/)
[![features](https://img.shields.io/badge/features-overview-1f6feb?style=flat-square&logo=postgresql&logoColor=white)](https://pduzc.com/features)
[![docs](https://img.shields.io/badge/docs-instant%20recovery-4c8eda?style=flat-square&logo=readthedocs&logoColor=white)](https://pduzc.com/docs/instant-recovery)
[![quickstart](https://img.shields.io/badge/quickstart-ready-009688?style=flat-square)](https://pduzc.com/quickstart)
[![license](https://img.shields.io/badge/license-BSL--1.1-6f42c1?style=flat-square)](LICENSE)

[English](#english) | [中文](#中文)

---

<a name="english"></a>
## English

PDU (PostgreSQL Data Unloader) is a comprehensive disaster recovery and data extraction tool for PostgreSQL databases (versions 14-18). It can read PostgreSQL data files directly without requiring a running database instance, making it ideal for disaster recovery, forensic analysis, and data extraction scenarios.


### [Project Introduction](https://pduzc.com/)
Consider these scenarios in a PostgreSQL database:  
1. **The database integrity is completely corrupted and cannot be opened.**
2. **Data is accidentally deleted/updated.**
3. **Data files are accidentally deleted.**
4. **Tables are accidentally dropped without any backups.**

- Some of these scenarios might still have a chance cause we have tools like: *pg_filedump, pg_dirtyread, pg_resetlogs, pg_waldump*; the other scenario like drop table without backup is seen as no hope.
- However, each of the tools above has its own unique usage methods, which undoubtedly **increases the learning curve for users** and also **adds trial-and-error costs during data recovery** without guaranteeing effectiveness for the above scenarios.

Data rescue in extreme scenarios is a crucial part of any database's ecosystem. In such scenarios,
- Oracle can use odu/dul to directly mine data from data files or ASM disks.
- PostgreSQL also has the pg_filedump tool in its ecosystem, which can **mine single table provided the table structure is known**. However, for cases of complete database corruption, **how to effectively obtain the entire database's data dictionary and achieve orderly and convenient data export** is a **significant challenge** facing the PG ecosystem.

This project, **PDU (PostgreSQL Data Unloader)**, is a tool that integrates data file mining and recovery of accidentally deleted/updated/dropped data. Its characteristics are **lower learning cost and higher recovery efficiency**.  
  
  **ALL KINDS OF POSTGRESQL DATA RECOVERY JOB, SOLVED IN ONE TOOL.**

The PDU tool's file structure is simple, consisting of only two parts: the ***pdu executable file and the pdu.ini configuration file***. The overall design philosophy is to lower the learning cost for users.


### [Core Capabilities](https://pduzc.com/features)

PDU handles four critical failure scenarios:

1. **Database Corruption** - Extract data when the database cannot start
2. **Accidental DELETE/UPDATE** - Recover original data from archived WAL files
3. **Deleted Data Files** - Direct extraction from remaining data files
4. **Dropped Tables** - Recovery of dropped tables without backups (unique capability)

### Features

- **Direct File Access**: Read PostgreSQL data files without a running instance
- **Data Export**: Export table data to CSV or SQL COPY format
- **WAL Analysis**: Parse WAL (Write-Ahead Log) files for transaction recovery
- **Deleted Data Recovery**: Recover deleted or truncated data from data files
- **DROP Table Recovery**: Reconstruct dropped tables from disk fragments
- **TOAST Support**: Handle large object (TOAST) data decompression with LZ4
- **Multi-threaded**: Efficient parallel processing for large databases

### Supported Data Types

**Fully Supported:**
- Numeric types (integers, floats, numeric/decimal)
- Temporal data (date, time, timestamp, interval)
- Text and binary data (varchar, text, bytea)
- JSON/JSONB structures
- Arrays of basic types
- UUID and network address types (inet, cidr, macaddr)
- Geometric types (point, line, polygon, circle, etc.)

**Not Supported:**
- User-defined enumeration types
- Composite types
- Range types
- Full-text search vectors (tsvector)

### System Requirements

- Linux (x86_64)
- GCC compiler (C99 standard)
- LZ4 library (`liblz4-dev` on Debian/Ubuntu, `lz4-devel` on RHEL/CentOS)
- zlib library

### Set PG_VERSION_NUM and Build

```bash
sed -i 's/#define PG_VERSION_NUM [0-9]\+/#define PG_VERSION_NUM 15/g' basic.
make
```

This will produce the `pdu` executable.

### [Quick Start](https://pduzc.com/quickstart)

1. **Configure** `pdu.ini` with your PostgreSQL data directory:
   ```ini
   PGDATA=/path/to/postgresql/data
   ARCHIVE_DEST=/path/to/wal/archive
   ```

2. **Run** PDU:
   ```bash
   ./pdu
   ```

3. **Initialize** metadata:
   ```
   PDU> b;
   ```

4. **Navigate and Export**:
   ```
   PDU> \l;              -- List databases
   PDU> use mydb;        -- Select database
   PDU> \dn;             -- List schemas
   PDU> set public;      -- Select schema
   PDU> \dt;             -- List tables
   PDU> \d+ mytable;    -- Describe table structure
   PDU> unload mytable;  -- Export table data
   ```

### Configuration (pdu.ini)

| Parameter | Description |
|-----------|-------------|
| `PGDATA` | PostgreSQL data directory path |
| `ARCHIVE_DEST` | WAL archive directory path (for DELETE/UPDATE recovery) |
| `DISK_PATH` | Disk device for DROPSCAN feature |
| `BLOCK_INTERVAL` | Block skip interval for DROPSCAN (default: 20, lower = more thorough but slower) |
| `PGDATA_EXCLUDE` | Directories to exclude during DROPSCAN |

### Commands Reference

| Command | Description |
|---------|-------------|
| `b;` | Bootstrap/initialize metadata from PGDATA |
| `use <db>;` | Switch to specified database |
| `set <schema>;` | Switch to specified schema |
| `\l;` | List all databases |
| `\dn;` | List schemas in current database |
| `\dt;` | List tables in current schema |
| `\d+ <table>;` | Describe table structure |
| `unload <table>;` | Export table data to CSV |
| `unload sch;` | Export all tables in current schema |
| `unload ddl <table>;` | Export table DDL definition |
| `scan <table>;` | Scan WAL files for recovery |
| `restore del <table>;` | Restore deleted records |
| `restore upd <table>;` | Restore updated records (original values) |
| `dropscan;` | Scan disk for dropped table fragments |
| `exit;` or `\q;` | Exit PDU |

### [Recovery Workflows](https://pduzc.com/docs/instant-recovery)

#### DELETE/UPDATE Recovery
```
PDU> b;                          -- Initialize
PDU> use mydb;
PDU> set public;
PDU> param startwal 0000000100000000000000XX;  -- Set starting WAL
PDU> scan;                       -- Scan WAL files
PDU> restore del mytable;        -- Restore deleted records
PDU> restore upd mytable;        -- Restore original values before UPDATE
```

#### DROP Table Recovery
```
PDU> b;
PDU> use mydb;
PDU> dropscan;                   -- Scan disk for fragments
PDU> \dt;                        -- List recovered tables
PDU> unload recovered_table;     -- Export recovered data
```

### Output

Exported data is saved in the current working directory under:
- `<database>/<schema>/<table>.csv` - Table data
- `<database>/meta/` - Schema metadata
- `<database>/toastmeta/` - TOAST metadata

---

<a name="中文"></a>
## 中文

### [项目介绍](https://pduzc.com/)

PDU（PostgreSQL Data Unloader）是一款专业的 PostgreSQL 数据库灾难恢复和数据提取工具，支持 PostgreSQL 14-18 版本。它可以直接读取 PostgreSQL 数据文件，无需运行中的数据库实例，特别适用于灾难恢复、数据取证和数据提取场景。

### [核心能力](https://pduzc.com/features)

PDU 可处理四种关键故障场景：

1. **数据库损坏** - 在数据库无法启动时提取数据
2. **误执行 DELETE/UPDATE** - 从归档 WAL 文件恢复原始数据
3. **数据文件被删除** - 从残留数据文件中直接提取
4. **表被 DROP** - 无备份情况下恢复被删除的表（独特能力）

### 功能特性

- **直接文件访问**：无需运行数据库实例即可读取数据文件
- **数据导出**：将表数据导出为 CSV 或 SQL COPY 格式
- **WAL 分析**：解析 WAL（预写日志）文件进行事务恢复
- **删除数据恢复**：从数据文件中恢复已删除或截断的数据
- **DROP 表恢复**：从磁盘碎片重建被删除的表
- **TOAST 支持**：处理大对象（TOAST）数据的 LZ4 解压
- **多线程**：高效的并行处理，适用于大型数据库

### 支持的数据类型

**完全支持：**
- 数值类型（整数、浮点数、numeric/decimal）
- 时间类型（date、time、timestamp、interval）
- 文本和二进制（varchar、text、bytea）
- JSON/JSONB 结构
- 基本类型数组
- UUID 和网络地址类型（inet、cidr、macaddr）
- 几何类型（point、line、polygon、circle 等）

**不支持：**
- 用户自定义枚举类型
- 复合类型
- 范围类型
- 全文搜索向量（tsvector）

### 系统要求

- Linux (x86_64)
- GCC 编译器（C99 标准）
- LZ4 库（Debian/Ubuntu: `liblz4-dev`，RHEL/CentOS: `lz4-devel`）
- zlib 库

### 设置 PG_VERSION_NUM 并 编译

```bash
sed -i 's/#define PG_VERSION_NUM [0-9]\+/#define PG_VERSION_NUM 15/g' basic.
make
```

将会生成`pdu` 可执行文件.

### [快速开始](https://pduzc.com/quickstart)

1. **配置** `pdu.ini`，设置 PostgreSQL 数据目录：
   ```ini
   PGDATA=/path/to/postgresql/data
   ARCHIVE_DEST=/path/to/wal/archive
   ```

2. **运行** PDU：
   ```bash
   ./pdu
   ```

3. **初始化**元数据：
   ```
   PDU> b;
   ```

4. **导航和导出**：
   ```
   PDU> \l;              -- 列出数据库
   PDU> use mydb;        -- 选择数据库
   PDU> \dn;             -- 列出模式
   PDU> set public;      -- 选择模式
   PDU> \dt;             -- 列出表
   PDU> \dt+ mytable;    -- 查看表结构
   PDU> unload mytable;  -- 导出表数据
   ```

### 配置说明 (pdu.ini)

| 参数 | 说明 |
|------|------|
| `PGDATA` | PostgreSQL 数据目录路径 |
| `ARCHIVE_DEST` | WAL 归档目录路径（用于 DELETE/UPDATE 恢复） |
| `DISK_PATH` | DROPSCAN 功能需要扫描的磁盘设备 |
| `BLOCK_INTERVAL` | DROPSCAN 的块跳过间隔（默认：20，值越小越全面但越慢） |
| `PGDATA_EXCLUDE` | DROPSCAN 时需要排除的目录 |

### 命令参考

| 命令 | 说明 |
|------|------|
| `b;` | 从 PGDATA 初始化/引导元数据 |
| `use <db>;` | 切换到指定数据库 |
| `set <schema>;` | 切换到指定模式 |
| `\l;` | 列出所有数据库 |
| `\dn;` | 列出当前数据库的模式 |
| `\dt;` | 列出当前模式的表 |
| `\dt+ <table>;` | 查看表结构 |
| `unload <table>;` | 导出表数据为 CSV |
| `unload sch;` | 导出当前模式的所有表 |
| `unload ddl <table>;` | 导出表的 DDL 定义 |
| `scan;` | 扫描 WAL 文件用于恢复 |
| `restore del <table>;` | 恢复已删除的记录 |
| `restore upd <table>;` | 恢复 UPDATE 前的原始值 |
| `dropscan;` | 扫描磁盘查找被 DROP 表的碎片 |
| `exit;` 或 `\q;` | 退出 PDU |

### [恢复工作流程](https://pduzc.com/docs/instant-recovery)

#### DELETE/UPDATE 恢复
```
PDU> b;                          -- 初始化
PDU> use mydb;
PDU> set public;
PDU> param startwal 0000000100000000000000XX;  -- 设置起始 WAL
PDU> scan;                       -- 扫描 WAL 文件
PDU> restore del mytable;        -- 恢复已删除记录
PDU> restore upd mytable;        -- 恢复 UPDATE 前的原始值
```

#### DROP 表恢复
```
PDU> b;
PDU> use mydb;
PDU> dropscan;                   -- 扫描磁盘碎片
PDU> \dt;                        -- 列出恢复的表
PDU> unload recovered_table;     -- 导出恢复的数据
```

### 输出位置

导出的数据保存在当前工作目录下：
- `<database>/<schema>/<table>.csv` - 表数据
- `<database>/meta/` - 模式元数据
- `<database>/toastmeta/` - TOAST 元数据

---

## License

This project is licensed under the Business Source License 1.1 (BSL 1.1).

- **Licensor**: ZhangChen
- **Licensed Work**: PDU (PostgreSQL Data Unloader)
- **Change Date**: 2029-01-01
- **Change License**: Apache License, Version 2.0

See [LICENSE](LICENSE) for the full license text.

### Third-Party Code

This project includes code derived from:
- PostgreSQL (PostgreSQL License) - See [LICENSE-PostgreSQL](LICENSE-PostgreSQL)
- NTT pg_rman project (PostgreSQL License)

See [NOTICE](NOTICE) for details.

## Author

Copyright (c) 2024-2025 ZhangChen

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## Links

- **GitHub**: https://github.com/wublabdubdub/PDU-PostgresqlDataUnloader
- **Issues**: https://github.com/wublabdubdub/PDU-PostgresqlDataUnloader/issues
