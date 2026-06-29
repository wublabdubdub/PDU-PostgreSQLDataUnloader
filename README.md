<div align="center">

![PDU Logo](assets/logo-banner.svg)


### All PostgreSQL Data Recovery Scenarios, One Tool

[![Website](https://img.shields.io/badge/🌐_Website-pduzc.com-4A90E2?style=flat)](https://pduzc.com/)
[![Features](https://img.shields.io/badge/⚡_Features-Overview-F39C12?style=flat)](https://pduzc.com/features)
[![Documentation](https://img.shields.io/badge/📚_Docs-Instant_Recovery-9B59B6?style=flat)](https://pduzc.com/docs/instant-recovery)
[![Quick Start](https://img.shields.io/badge/🚀_Quick-Start-E74C3C?style=flat)](https://pduzc.com/quickstart)

[![License](https://img.shields.io/badge/license-Apache--2.0-27AE60?style=flat)](LICENSE)
[![Professional](https://img.shields.io/badge/Professional-Grade-34495E?style=flat)](https://pduzc.com/)
[![Easy to Use](https://img.shields.io/badge/Easy_to-Use-1ABC9C?style=flat)](https://pduzc.com/quickstart)
[![Platform](https://img.shields.io/badge/platform-Linux_x86__64-E67E22?style=flat&logo=linux&logoColor=white)](https://www.kernel.org/)

[English](#english) | [中文](#中文)

</div>

---

<a name="english"></a>

## 🎯 What is PDU?

**PDU (PostgreSQL Data Unloader)** is a professional-grade, open-source disaster recovery and data extraction tool for PostgreSQL databases (versions 14-18). It reads PostgreSQL data files **directly** without requiring a running database instance—making it the ultimate solution for disaster recovery, forensic analysis, and emergency data extraction.

### 💔 The Problem

PostgreSQL DBAs face critical scenarios like these:

▸ **Database completely corrupted** - won't start, can't recover
▸ **Accidental DELETE/UPDATE** - critical data gone in seconds
▸ **Data files deleted** - filesystem corruption or operator error

Traditional tools like `pg_filedump`, `pg_dirtyread`, and `pg_waldump` each solve *one* problem with *different* learning curves. **PDU unifies all these scenarios** into a single, intuitive tool.

> 💬 *In Oracle ecosystems, professionals use ODU/DUL for direct data mining. PostgreSQL deserved the same level of capability—that's why PDU exists.*

---

## ✨ Why Choose PDU?

<table>
<tr>
<td width="33%">

### ✓ Simple & Unified
Two files (`pdu` + `pdu.ini`), zero dependencies. One tool handles corruption, deletion, and updates.

</td>
<td width="33%">

### ✓ Safe & Instant
100% read-only operations. Your original data files remain untouched. Recover data faster than traditional PITR methods.

</td>
<td width="33%">

### ✓ Unique & Compatible
Powerful direct data file reading capabilities. Supports PostgreSQL 14-18.

</td>
</tr>
</table>

---

## 🔥 Core Capabilities

PDU handles **three critical disaster scenarios** with **two recovery methods**:

### 📌 Three Disaster Scenarios

| Scenario | What Happened | PDU Solution | Guide |
|----------|---------------|--------------|-------|
| ▸ **Database Corruption** | PostgreSQL won't start due to catalog/file corruption | Extract data directly from damaged files | [Read →](https://pduzc.com/docs/instant-recovery/corrupted-instance) |
| ▸ **Accidental DELETE/UPDATE** | Wrong `DELETE`/`UPDATE` executed in production | Replay WAL archives to recover original data | [Read →](https://pduzc.com/docs/instant-recovery/deleted-records) |
| ▸ **Data File Loss** | Specific data files deleted or corrupted | Rebuild data from remaining files | [Read →](https://pduzc.com/docs/instant-recovery/corrupted-datafile) |

### 🛠️ Two Recovery Methods

**1. Direct Data File Extraction**
- Parse PostgreSQL heap/TOAST files directly
- Bypass database engine entirely
- fast parsing of TOAST data

**2. WAL Archive Scanning**
- Analyze Write-Ahead Logs (WAL) for transaction history
- Extract deleted rows or pre-update values
- Time-based and transaction-based filtering

---

## 🚀 Quick Start

### Step 1: Build PDU

```bash
# Set PostgreSQL version (14-18)
sed -i 's/#define PG_VERSION_NUM [0-9]\+/#define PG_VERSION_NUM <Version>/g' basic.h

# Compile
make

# Result: ./pdu executable
```

### Step 2: Configure `pdu.ini`

```ini
PGDATA=/var/lib/postgresql/15/main
ARCHIVE_DEST=/var/lib/postgresql/wal_archive
```

### Step 3: Launch PDU

```bash
./pdu
```

### Step 4: Interactive Commands

```sql
PDU> b;                    -- Bootstrap metadata from PGDATA
PDU> \l;                   -- List databases
PDU> use production_db;    -- Switch database
PDU> \dn;                  -- List schemas
PDU> set public;           -- Switch schema
PDU> \dt;                  -- List tables
PDU> \d+ customers;        -- Describe table structure
PDU> unload tab customers; -- Export one table to CSV
```

📖 **Full Guide**: [pduzc.com/quickstart](https://pduzc.com/quickstart)

---

## 📖 Usage Examples

### Scenario 1: Extract Data from Corrupted Database

**Problem**: PostgreSQL won't start due to corrupted system catalog.

**Solution**:
```sql
PDU> b;                          -- Initialize metadata
PDU> use mydb;
PDU> set public;
PDU> unload tab customers;       -- Export one table
PDU> unload sch public;          -- Export all tables in schema
PDU> unload ddl;                 -- Generate DDL for current schema
PDU> unload copy;                -- Generate COPY script for exported CSVs
```

**Result**: CSV files exported to current directory, even if PostgreSQL won't start.

---

### Scenario 2: Recover Deleted Records

**Problem**: A critical `DELETE` statement was executed, and you need the deleted rows back.

**Solution**:
```sql
PDU> use production;
PDU> set public;
PDU> param restype delete;       -- Scan DELETE records
PDU> param resmode tx;           -- Restore by transaction ID
PDU> scan orders;                -- Scan WAL for deleted rows
PDU> restore del <TxID>;         -- Restore one transaction from scan results
```

**Result**: Deleted rows recovered from WAL archives and exported to CSV.

For time-range recovery, use `param resmode time;`, set `param starttime <YYYY-MM-DD HH:MM:SS>;` and `param endtime <YYYY-MM-DD HH:MM:SS>;`, then run `scan orders;` and `restore del all;`.

---

### Scenario 3: Undo Accidental UPDATE

**Problem**: An `UPDATE` statement modified hundreds of rows with wrong values.

**Solution**:
```sql
PDU> use production;
PDU> set public;
PDU> param restype update;       -- Scan UPDATE records
PDU> param resmode tx;           -- Restore by transaction ID
PDU> scan users;
PDU> restore upd <TxID>;         -- Restore pre-UPDATE values
```

**Result**: Original values before `UPDATE` statement recovered from WAL.

📖 **More Scenarios**: [pduzc.com/docs/instant-recovery](https://pduzc.com/docs/instant-recovery)

---

## 📋 Command Reference

| Command | Description |
|---------|-------------|
| `b;` | Bootstrap metadata from `PGDATA` |
| `use <database>;` | Switch to specified database |
| `set <schema>;` | Switch to specified schema |
| `\l;` | List all databases |
| `\dn;` | List schemas in current database |
| `\dt;` | List tables in current schema |
| `\d+ <table>;` | Describe table structure (columns, types) |
| `\d <table>;` | Show table column types |
| `u tab <table>;` / `unload tab <table>;` | Export one table to CSV |
| `u sch <schema>;` / `unload sch <schema>;` | Export all tables in a schema |
| `u ddl;` / `unload ddl;` | Generate DDL for the current schema |
| `u copy;` / `unload copy;` | Generate COPY statements for exported CSVs |
| `scan <table>;` | Scan WAL archives for DELETE/UPDATE records of one table |
| `scan manual;` | Initialize metadata from files under the manual directory |
| `scan drop;` | Scan WAL for dropped/truncated table metadata |
| `meta tab <table>;` | Write one table's metadata to `tab.config` |
| `meta sch <schema>;` | Write all table metadata for a schema to `tab.config` |
| `restore del <TxID>;` | Restore deleted rows for one scanned transaction |
| `restore upd <TxID>;` | Restore pre-UPDATE rows for one scanned transaction |
| `restore del all;` / `restore upd all;` | Restore scanned results in time-range mode |
| `add <filenode> <table> <columns>;` | Manually add table metadata; put data files in `restore/datafile` |
| `restore db <db> <path>;` | Initialize a custom database directory (Pro/Enterprise) |
| `dropscan idx;` / `ds idx;` | Build dropped-page index (Pro/Enterprise) |
| `dropscan;` / `ds;` | Scan disk using tables in `tab.config` (Pro/Enterprise) |
| `dropscan iso;` / `ds iso;` | Recover from an image file using tables in `tab.config` (Pro/Enterprise) |
| `dropscan repair;` / `ds repair;` | Retry failed TOAST table scans (Pro/Enterprise) |
| `dropscan clean;` / `ds clean;` | Clean `restore/dropscan` directories (Pro/Enterprise) |
| `dropscan copy;` / `ds copy;` | Generate COPY commands for DropScan output (Pro/Enterprise) |
| `p startwal <WAL>;` / `param startwal <WAL>;` | Set WAL scan start file |
| `p endwal <WAL>;` / `param endwal <WAL>;` | Set WAL scan end file |
| `p starttime <YYYY-MM-DD HH:MM:SS>;` / `param starttime <YYYY-MM-DD HH:MM:SS>;` | Set time-range recovery start time |
| `p endtime <YYYY-MM-DD HH:MM:SS>;` / `param endtime <YYYY-MM-DD HH:MM:SS>;` | Set time-range recovery end time |
| `p resmode tx\|time;` / `param resmode tx\|time;` | Set recovery mode |
| `p restype delete\|update;` / `param restype delete\|update;` | Set recovery type |
| `p exmode csv\|sql;` / `param exmode csv\|sql;` | Set export format |
| `p encoding utf8\|gbk;` / `param encoding utf8\|gbk;` | Set output encoding |
| `p isomode on\|off;` / `param isomode on\|off;` | Set image-save mode |
| `reset <parameter>;` / `reset all;` | Reset one parameter or all parameters |
| `show;` | Display current parameters |
| `t;` | Display supported data types |
| `exit;` / `\q;` | Exit PDU |

All interactive commands must end with `;`.

📖 **Full Reference**: [pduzc.com/docs](https://pduzc.com/docs)

---

## ⚙️ Configuration (pdu.ini)

| Parameter | Description | Example |
|-----------|-------------|---------|
| `PGDATA` | PostgreSQL data directory path | `/var/lib/postgresql/15/main` |
| `ARCHIVE_DEST` | WAL archive directory (for DELETE/UPDATE recovery) | `/var/lib/postgresql/wal_archive` |

---

## 📊 Supported Data Types

### ✅ Fully Supported

- **Numeric**: `int2`, `int4`, `int8`, `float4`, `float8`, `numeric`, `decimal`
- **Temporal**: `date`, `time`, `timestamp`, `timestamptz`, `interval`
- **Text**: `varchar`, `text`, `char`, `bpchar`
- **JSON**: `json`
- **UUID**: `uuid`
- **Network**: `inet`, `cidr`, `macaddr`, `macaddr8`
- **TOAST**: Automatically handles large objects with LZ4 decompression

### ❌ Not Supported

- User-defined enum types
- Composite types
- Range types (`int4range`, `tsrange`, etc.)
- Full-text search vectors (`tsvector`, `tsquery`)

---

## 🏆 Recovery Workflows

> 📚 **Comprehensive guides available at [pduzc.com/docs/instant-recovery](https://pduzc.com/docs/instant-recovery)**

| Scenario | Description | Official Guide |
|----------|-------------|----------------|
| **Corrupted Instance** | PostgreSQL won't start—extract data straight from disk | [Open Guide →](https://pduzc.com/docs/instant-recovery/corrupted-instance) |
| **Corrupted Datafile** | Specific datafile damaged—salvage reachable data | [Open Guide →](https://pduzc.com/docs/instant-recovery/corrupted-datafile) |
| **Corrupted Database** | Entire database broken—reconstruct catalog and data | [Open Guide →](https://pduzc.com/docs/instant-recovery/corrupted-database) |
| **Deleted Records** | Rows accidentally deleted—replay WAL to recover | [Open Guide →](https://pduzc.com/docs/instant-recovery/deleted-records) |
| **Updated Records** | Wrong UPDATE executed—rollback with WAL analysis | [Open Guide →](https://pduzc.com/docs/instant-recovery/updated-records) |

---

## ❓ FAQ / Troubleshooting

### Compilation Issues

**Q: `make` fails with "lz4.h: No such file or directory"**

A: Install LZ4 development library:
```bash
# Ubuntu/Debian
sudo apt-get install liblz4-dev zlib1g-dev

# RHEL/CentOS
sudo yum install lz4-devel zlib-devel
```

**Q: How do I change the PostgreSQL version target?**

A: Edit `basic.h` and modify `PG_VERSION_NUM`:
```bash
sed -i 's/#define PG_VERSION_NUM [0-9]\+/#define PG_VERSION_NUM 16/g' basic.h
make clean && make
```

### Configuration Issues

**Q: PDU shows "PGDATA directory not found"**

A: Verify the `PGDATA` path in `pdu.ini` is correct and accessible:
```bash
ls -la /var/lib/postgresql/15/main
```

### Usage Issues

**Q: WAL scanning (`scan <table>`) returns no results**

A: Ensure:
- `ARCHIVE_DEST` in `pdu.ini` points to the correct WAL archive directory
- WAL archiving was enabled when the DELETE/UPDATE occurred
- Make sure WAL files scanned includes the DELETE/UPDATE operations

**Q: Exported CSV files contain garbled characters**

A: Check database encoding:
```sql
PDU> \l;  -- Look at the encoding column
```
The CSV will use the database's encoding. Convert if needed:
```bash
iconv -f UTF8 -t GBK input.csv > output.csv
```

### Performance Optimization

**Q: Unloading large tables takes too long**

A: PDU performs full table scans. For better performance:
- Run PDU on a machine with fast disk I/O
- Consider unloading specific columns only (if supported in future versions)
- Use parallel exports for different tables

---

## 📦 System Requirements

- **OS**: Linux (x86_64)
- **Compiler**: GCC (C99 standard)
- **Libraries**:
  - `liblz4-dev` (Debian/Ubuntu) or `lz4-devel` (RHEL/CentOS)
  - `zlib` library

### Installation on Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install build-essential liblz4-dev zlib1g-dev
```

### Installation on RHEL/CentOS

```bash
sudo yum install gcc lz4-devel zlib-devel
```

---

## 🤝 Contributing

We welcome contributions from the community! Here's how you can help:

### 🐛 Report Issues

Found a bug or have a feature request? [Open an issue](https://github.com/wublabdubdub/PDU-PostgresqlDataUnloader/issues)

### 💻 Submit Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### 📝 Development Guidelines

- Follow C99 coding standards
- Add tests for new recovery features
- Update documentation for new commands
- Ensure compatibility with PostgreSQL 14-18

### 🌍 Translate Documentation

Help make PDU accessible to more communities by translating the documentation.

---

## 📄 License

This project is licensed under the **Apache License, Version 2.0**. See [LICENSE](LICENSE) for the full license text.

**Historical Note**: Earlier drafts used the Business Source License 1.1. Starting from this release, PDU is 100% open-source under Apache-2.0 for maximum community compatibility.

### Third-Party Code

This project includes code derived from:
- **PostgreSQL** (PostgreSQL License) - See [LICENSE-PostgreSQL](LICENSE-PostgreSQL)
- **NTT pg_rman** (PostgreSQL License)

See [NOTICE](NOTICE) for complete attribution.

---

## 👤 Author

**ZhangChen (Jay)**
Copyright © 2024-2025

---

## 🔗 Links & Resources

- **🌐 Official Website**: [pduzc.com](https://pduzc.com/)
- **📚 Full Documentation**: [pduzc.com/docs](https://pduzc.com/docs)
- **⚡ Features Overview**: [pduzc.com/features](https://pduzc.com/features)
- **🚀 Quick Start Guide**: [pduzc.com/quickstart](https://pduzc.com/quickstart)
- **💬 GitHub Issues**: [github.com/wublabdubdub/PDU-PostgresqlDataUnloader/issues](https://github.com/wublabdubdub/PDU-PostgresqlDataUnloader/issues)
- **📂 GitHub Repository**: [github.com/wublabdubdub/PDU-PostgresqlDataUnloader](https://github.com/wublabdubdub/PDU-PostgresqlDataUnloader)

---

## 🌟 Show Your Support

If PDU helped you recover critical data or saved your day, please consider:

⭐ Starring this repository on GitHub
📢 Sharing PDU with your team
🐛 Reporting issues to help improve the tool
💬 Joining discussions and sharing your use cases

---

<div align="center">

### 💬 Questions or Feedback?

Visit [pduzc.com](https://pduzc.com/) for comprehensive guides, or [open an issue](https://github.com/wublabdubdub/PDU-PostgresqlDataUnloader/issues) on GitHub.

**Made with ❤️ for the PostgreSQL Community**

</div>

---
---

<a name="中文"></a>

<div align="center">

![PDU Logo](assets/logo-banner.svg)

### 所有 PostgreSQL 数据恢复场景，一个工具搞定

[![官网](https://img.shields.io/badge/🌐_官网-pduzc.com-4A90E2?style=flat)](https://pduzc.com/)
[![功能特性](https://img.shields.io/badge/⚡_功能-特性概览-F39C12?style=flat)](https://pduzc.com/features)
[![文档](https://img.shields.io/badge/📚_文档-即时恢复-9B59B6?style=flat)](https://pduzc.com/docs/instant-recovery)
[![快速开始](https://img.shields.io/badge/🚀_快速-开始-E74C3C?style=flat)](https://pduzc.com/quickstart)

[![许可证](https://img.shields.io/badge/许可证-Apache--2.0-27AE60?style=flat)](LICENSE)
[![专业级](https://img.shields.io/badge/专业-级别-34495E?style=flat)](https://pduzc.com/)
[![易上手](https://img.shields.io/badge/易于-上手-1ABC9C?style=flat)](https://pduzc.com/quickstart)
[![平台](https://img.shields.io/badge/平台-Linux_x86__64-E67E22?style=flat&logo=linux&logoColor=white)](https://www.kernel.org/)

</div>

---

## 🎯 PDU 是什么？

**PDU (PostgreSQL Data Unloader)** 是一款专业级、开源的 PostgreSQL 数据库灾难恢复和数据提取工具，支持 PostgreSQL 14-18 版本。它可以**直接读取** PostgreSQL 数据文件，无需运行中的数据库实例——是灾难恢复、取证分析和紧急数据提取的终极解决方案。

### 💔 问题场景

PostgreSQL DBA 经常面临这些关键场景：

▸ **数据库完全损坏** - 无法启动，无法恢复
▸ **误执行 DELETE/UPDATE** - 关键数据瞬间消失
▸ **数据文件被删除** - 文件系统损坏或操作失误

传统工具如 `pg_filedump`、`pg_dirtyread` 和 `pg_waldump` 各自解决*一个*问题，且有*不同*的学习曲线。**PDU 将所有这些场景统一到一个直观的工具中**。

> 💬 *在 Oracle 生态系统中，专业人士使用 ODU/DUL 进行直接数据挖掘。PostgreSQL 也应该拥有同等级别的能力——这就是 PDU 存在的原因。*

---

## ✨ 为什么选择 PDU？

<table>
<tr>
<td width="33%">

### ✓ 简单统一
两个文件（`pdu` + `pdu.ini`），零依赖。一个工具处理损坏、删除和更新的恢复。

</td>
<td width="33%">

### ✓ 安全快速
100% 只读操作。您的原始数据文件保持不变。比传统 PITR 方法更快地恢复数据。

</td>
<td width="33%">

### ✓ 独特兼容
强大的直接数据文件读取能力。支持 PostgreSQL 14-18。

</td>
</tr>
</table>

---

## 🔥 核心能力

PDU 使用**两种恢复方法**处理**三种关键灾难场景**：

### 📌 三种灾难场景

| 场景 | 发生了什么 | PDU 解决方案 | 指南 |
|------|-----------|--------------|------|
| ▸ **数据库损坏** | PostgreSQL 因目录/文件损坏无法启动 | 直接从损坏文件中提取数据 | [查看 →](https://pduzc.com/docs/instant-recovery/corrupted-instance) |
| ▸ **误执行 DELETE/UPDATE** | 在生产环境执行了错误的 `DELETE`/`UPDATE` | 重放 WAL 归档以恢复原始数据 | [查看 →](https://pduzc.com/docs/instant-recovery/deleted-records) |
| ▸ **数据文件丢失** | 特定数据文件被删除或损坏 | 从剩余文件重建数据 | [查看 →](https://pduzc.com/docs/instant-recovery/corrupted-datafile) |

### 🛠️ 两种恢复方法

**1. 直接数据文件提取**
- 直接解析 PostgreSQL 堆/TOAST 文件
- 完全绕过数据库引擎
- 快速解析 TOAST 数据

**2. WAL 归档扫描**
- 分析预写日志 (WAL) 的事务历史
- 提取已删除的行或更新前的值
- 基于时间和事务的过滤

---

## 🚀 快速开始

### 步骤 1：编译 PDU

```bash
# 设置 PostgreSQL 版本（14-18）
sed -i 's/#define PG_VERSION_NUM [0-9]\+/#define PG_VERSION_NUM 大版本号/g' basic.h

# 编译
make

# 结果：./pdu 可执行文件
```

### 步骤 2：配置 `pdu.ini`

```ini
PGDATA=/var/lib/postgresql/15/main
ARCHIVE_DEST=/var/lib/postgresql/wal_archive
```

### 步骤 3：启动 PDU

```bash
./pdu
```

### 步骤 4：交互式命令

```sql
PDU> b;                    -- 从 PGDATA 引导元数据
PDU> \l;                   -- 列出数据库
PDU> use production_db;    -- 切换数据库
PDU> \dn;                  -- 列出模式
PDU> set public;           -- 切换模式
PDU> \dt;                  -- 列出表
PDU> \d+ customers;        -- 查看表结构
PDU> unload tab customers; -- 导出单表为 CSV
```

📖 **完整指南**: [pduzc.com/quickstart](https://pduzc.com/quickstart)

---

## 📖 使用示例

### 场景 1：从损坏的数据库中提取数据

**问题**：PostgreSQL 因系统目录损坏无法启动。

**解决方案**：
```sql
PDU> b;                          -- 初始化元数据
PDU> use mydb;
PDU> set public;
PDU> unload tab customers;       -- 导出单表
PDU> unload sch public;          -- 导出模式中的所有表
PDU> unload ddl;                 -- 生成当前模式的 DDL
PDU> unload copy;                -- 为已导出的 CSV 生成 COPY 脚本
```

**结果**：CSV 文件导出到当前目录，即使 PostgreSQL 无法启动。

---

### 场景 2：恢复已删除的记录

**问题**：执行了关键的 `DELETE` 语句，需要找回已删除的行。

**解决方案**：
```sql
PDU> use production;
PDU> set public;
PDU> param restype delete;       -- 扫描 DELETE 记录
PDU> param resmode tx;           -- 按事务号恢复
PDU> scan orders;                -- 扫描 WAL 查找已删除的行
PDU> restore del <TxID>;         -- 根据扫描结果恢复指定事务
```

**结果**：从 WAL 归档中恢复已删除的行并导出为 CSV。

如需按时间区间恢复，先执行 `param resmode time;`，再设置 `param starttime <YYYY-MM-DD HH:MM:SS>;` 和 `param endtime <YYYY-MM-DD HH:MM:SS>;`，之后执行 `scan orders;` 与 `restore del all;`。

---

### 场景 3：撤销误执行的 UPDATE

**问题**：一个 `UPDATE` 语句用错误的值修改了数百行。

**解决方案**：
```sql
PDU> use production;
PDU> set public;
PDU> param restype update;       -- 扫描 UPDATE 记录
PDU> param resmode tx;           -- 按事务号恢复
PDU> scan users;
PDU> restore upd <TxID>;         -- 恢复 UPDATE 前的值
```

**结果**：从 WAL 恢复 `UPDATE` 语句执行前的原始值。

📖 **更多场景**: [pduzc.com/docs/instant-recovery](https://pduzc.com/docs/instant-recovery)

---

## 📋 命令参考

| 命令 | 说明 |
|------|------|
| `b;` | 从 `PGDATA` 引导元数据 |
| `use <数据库>;` | 切换到指定数据库 |
| `set <模式>;` | 切换到指定模式 |
| `\l;` | 列出所有数据库 |
| `\dn;` | 列出当前数据库的模式 |
| `\dt;` | 列出当前模式的表 |
| `\d+ <表>;` | 查看表结构（列、类型） |
| `\d <表>;` | 查看表列类型 |
| `u tab <表>;` / `unload tab <表>;` | 导出单表为 CSV |
| `u sch <模式>;` / `unload sch <模式>;` | 导出指定模式的所有表 |
| `u ddl;` / `unload ddl;` | 生成当前模式的 DDL |
| `u copy;` / `unload copy;` | 为已导出的 CSV 生成 COPY 语句 |
| `scan <表>;` | 扫描单表的 DELETE/UPDATE WAL 记录 |
| `scan manual;` | 从 manual 目录初始化元数据 |
| `scan drop;` | 扫描被 DROP/TRUNCATE 的表结构 |
| `meta tab <表>;` | 将指定表结构写入 `tab.config` |
| `meta sch <模式>;` | 将指定模式下的所有表结构写入 `tab.config` |
| `restore del <TxID>;` | 恢复扫描结果中的指定 DELETE 事务 |
| `restore upd <TxID>;` | 恢复扫描结果中的指定 UPDATE 事务 |
| `restore del all;` / `restore upd all;` | 在时间区间模式下恢复扫描结果 |
| `add <filenode> <表名> <字段类型列表>;` | 手动添加表信息；数据文件需放入 `restore/datafile` |
| `restore db <库名> <路径>;` | 初始化自定义数据库目录（Pro/Enterprise） |
| `dropscan idx;` / `ds idx;` | 获取被 DROP 数据页索引（Pro/Enterprise） |
| `dropscan;` / `ds;` | 按 `tab.config` 进行磁盘扫描恢复（Pro/Enterprise） |
| `dropscan iso;` / `ds iso;` | 按 `tab.config` 从镜像文件恢复（Pro/Enterprise） |
| `dropscan repair;` / `ds repair;` | 修复此前扫描失败的 TOAST 表恢复（Pro/Enterprise） |
| `dropscan clean;` / `ds clean;` | 清理 `restore/dropscan` 目录（Pro/Enterprise） |
| `dropscan copy;` / `ds copy;` | 为 DropScan 输出生成 COPY 命令（Pro/Enterprise） |
| `p startwal <WAL>;` / `param startwal <WAL>;` | 设置 WAL 扫描起始文件 |
| `p endwal <WAL>;` / `param endwal <WAL>;` | 设置 WAL 扫描结束文件 |
| `p starttime <YYYY-MM-DD HH:MM:SS>;` / `param starttime <YYYY-MM-DD HH:MM:SS>;` | 设置时间区间恢复起始时间 |
| `p endtime <YYYY-MM-DD HH:MM:SS>;` / `param endtime <YYYY-MM-DD HH:MM:SS>;` | 设置时间区间恢复结束时间 |
| `p resmode tx\|time;` / `param resmode tx\|time;` | 设置恢复模式 |
| `p restype delete\|update;` / `param restype delete\|update;` | 设置恢复类型 |
| `p exmode csv\|sql;` / `param exmode csv\|sql;` | 设置导出格式 |
| `p encoding utf8\|gbk;` / `param encoding utf8\|gbk;` | 设置输出编码 |
| `p isomode on\|off;` / `param isomode on\|off;` | 设置镜像保存模式 |
| `reset <参数名>;` / `reset all;` | 重置指定参数或全部参数 |
| `show;` | 查看当前参数 |
| `t;` | 查看支持的数据类型 |
| `exit;` / `\q;` | 退出 PDU |

所有交互式命令都必须以 `;` 结尾。

📖 **完整参考**: [pduzc.com/docs](https://pduzc.com/docs)

---

## ⚙️ 配置说明 (pdu.ini)

| 参数 | 说明 | 示例 |
|------|------|------|
| `PGDATA` | PostgreSQL 数据目录路径 | `/var/lib/postgresql/15/main` |
| `ARCHIVE_DEST` | WAL 归档目录（用于 DELETE/UPDATE 恢复） | `/var/lib/postgresql/wal_archive` |

---

## 📊 支持的数据类型

### ✅ 完全支持

- **数值类型**：`int2`、`int4`、`int8`、`float4`、`float8`、`numeric`、`decimal`
- **时间类型**：`date`、`time`、`timestamp`、`timestamptz`、`interval`
- **文本**：`varchar`、`text`、`char`、`bpchar`
- **JSON**：`json`
- **UUID**：`uuid`
- **网络**：`inet`、`cidr`、`macaddr`、`macaddr8`

### ❌ 不支持

- 用户自定义枚举类型
- 复合类型
- 范围类型（`int4range`、`tsrange` 等）
- 全文搜索向量（`tsvector`、`tsquery`）

---

## 🏆 恢复工作流程

> 📚 **完整指南请访问 [pduzc.com/docs/instant-recovery](https://pduzc.com/docs/instant-recovery)**

| 场景 | 说明 | 官方指南 |
|------|------|----------|
| **实例损坏** | PostgreSQL 无法启动——直接从磁盘提取数据 | [打开指南 →](https://pduzc.com/docs/instant-recovery/corrupted-instance) |
| **数据文件损坏** | 特定数据文件损坏——抢救可访问的数据 | [打开指南 →](https://pduzc.com/docs/instant-recovery/corrupted-datafile) |
| **数据库损坏** | 整个数据库损坏——重建目录和数据 | [打开指南 →](https://pduzc.com/docs/instant-recovery/corrupted-database) |
| **记录被删除** | 行被误删——重放 WAL 以恢复 | [打开指南 →](https://pduzc.com/docs/instant-recovery/deleted-records) |
| **记录被更新** | 执行了错误的 UPDATE——用 WAL 分析回滚 | [打开指南 →](https://pduzc.com/docs/instant-recovery/updated-records) |

---

## ❓ FAQ / 故障排除

### 编译问题

**问：`make` 失败，提示 "lz4.h: No such file or directory"**

答：安装 LZ4 开发库：
```bash
# Ubuntu/Debian
sudo apt-get install liblz4-dev zlib1g-dev

# RHEL/CentOS
sudo yum install lz4-devel zlib-devel
```

**问：如何更改 PostgreSQL 版本目标？**

答：编辑 `basic.h` 并修改 `PG_VERSION_NUM`：
```bash
sed -i 's/#define PG_VERSION_NUM [0-9]\+/#define PG_VERSION_NUM 16/g' basic.h
make clean && make
```

### 配置问题

**问：PDU 显示 "PGDATA directory not found"**

答：验证 `pdu.ini` 中的 `PGDATA` 路径是否正确且可访问：
```bash
ls -la /var/lib/postgresql/15/main
```

### 使用问题

**问：WAL 扫描（`scan <表>`）返回无结果**

答：确保：
- `pdu.ini` 中的 `ARCHIVE_DEST` 指向正确的 WAL 归档目录
- DELETE/UPDATE 发生时启用了 WAL 归档
- 扫描的WAL日志包含误操作期间的日志

**问：导出的 CSV 文件包含乱码**

答：检查数据库编码：
```sql
PDU> \l;  -- 查看编码列
```
CSV 将使用数据库的编码。如需转换：
```bash
iconv -f UTF8 -t GBK input.csv > output.csv
```

### 性能优化

**问：卸载大表耗时太长**

答：PDU 执行全表扫描。为获得更好性能：
- 在具有快速磁盘 I/O 的机器上运行 PDU
- 考虑只卸载特定列（如果未来版本支持）
- 对不同表使用并行导出

---

## 📦 系统要求

- **操作系统**：Linux (x86_64)
- **编译器**：GCC（C99 标准）
- **库**：
  - `liblz4-dev`（Debian/Ubuntu）或 `lz4-devel`（RHEL/CentOS）
  - `zlib` 库

### Ubuntu/Debian 安装

```bash
sudo apt-get update
sudo apt-get install build-essential liblz4-dev zlib1g-dev
```

### RHEL/CentOS 安装

```bash
sudo yum install gcc lz4-devel zlib-devel
```

---

## 🤝 贡献

欢迎社区贡献！您可以通过以下方式帮助：

### 🐛 报告问题

发现错误或有功能请求？[提交问题](https://github.com/wublabdubdub/PDU-PostgresqlDataUnloader/issues)

### 💻 提交拉取请求

1. Fork 仓库
2. 创建功能分支（`git checkout -b feature/amazing-feature`）
3. 提交更改（`git commit -m 'Add amazing feature'`）
4. 推送到分支（`git push origin feature/amazing-feature`）
5. 打开拉取请求

### 📝 开发指南

- 遵循 C99 编码标准
- 为新恢复功能添加测试
- 为新命令更新文档
- 确保与 PostgreSQL 14-18 兼容

### 🌍 翻译文档

帮助让 PDU 惠及更多社区，翻译文档。

---

## 📄 许可证

本项目采用 **Apache License, Version 2.0** 许可。完整文本见 [LICENSE](LICENSE)。

**历史说明**：早期草案使用 Business Source License 1.1。从本版本开始，PDU 在 Apache-2.0 下 100% 开源，以实现最大的社区兼容性。

### 第三方代码

本项目包含来自以下项目的派生代码：
- **PostgreSQL**（PostgreSQL 许可证）- 见 [LICENSE-PostgreSQL](LICENSE-PostgreSQL)
- **NTT pg_rman**（PostgreSQL 许可证）

完整归属见 [NOTICE](NOTICE)。

---

## 👤 作者

**张晨 (Jay)**
版权所有 © 2024-2025

---

## 🔗 链接与资源

- **🌐 官方网站**：[pduzc.com](https://pduzc.com/)
- **📚 完整文档**：[pduzc.com/docs](https://pduzc.com/docs)
- **⚡ 功能概览**：[pduzc.com/features](https://pduzc.com/features)
- **🚀 快速开始指南**：[pduzc.com/quickstart](https://pduzc.com/quickstart)
- **💬 GitHub Issues**：[github.com/wublabdubdub/PDU-PostgresqlDataUnloader/issues](https://github.com/wublabdubdub/PDU-PostgresqlDataUnloader/issues)
- **📂 GitHub 仓库**：[github.com/wublabdubdub/PDU-PostgresqlDataUnloader](https://github.com/wublabdubdub/PDU-PostgresqlDataUnloader)

---

## 🌟 显示您的支持

如果 PDU 帮助您恢复了关键数据或拯救了您的一天，请考虑：

⭐ 在 GitHub 上给仓库加星
📢 与您的团队分享 PDU
🐛 报告问题以帮助改进工具
💬 加入讨论并分享您的使用案例

---

<div align="center">

### 💬 有问题或反馈？

访问 [pduzc.com](https://pduzc.com/) 获取全面指南，或在 GitHub 上[提交问题](https://github.com/wublabdubdub/PDU-PostgresqlDataUnloader/issues)。

**用 ❤️ 为 PostgreSQL 社区打造**

</div>
