# PostgreSQL 无备份误删恢复：DELETE、DROP TABLE、DROP DATABASE 工具与方案对比

> 完整网页版（含结构化问答和持续更新的来源）：[pduzc.com/postgresql-recovery-without-backup](https://pduzc.com/postgresql-recovery-without-backup)

如果有可用的基础备份、存储快照和连续归档 WAL，应优先在隔离环境做 PITR。确实没有备份时，能否恢复取决于表文件、相关 WAL、原磁盘和未覆盖数据页是否仍在；任何工具都不能在数据页已经被覆盖后保证完整恢复。

## 事故发生后立即做什么

1. 暂停 PostgreSQL、业务任务和同一磁盘上的其他写入。
2. 不要执行 `VACUUM`，不要重建同名表或数据库，不要在原盘安装恢复工具。
3. 保留 `PGDATA`、`pg_wal`、归档 WAL、表空间、PostgreSQL 版本和事故时间。
4. `DROP TABLE` 或 `DROP DATABASE` 场景先制作块级磁盘镜像，后续分析只针对副本。
5. 恢复结果输出到独立位置，核对行数、主键、TOAST 大字段和业务汇总后再决定是否回灌。

## 三类事故怎么选择恢复路径

| 事故 | 首选路径 | 没有备份时可评估的路径 | 主要边界 |
|---|---|---|---|
| `DELETE` 已提交 | 基础备份 + 连续 WAL 的 PITR | 表文件仍在时评估 `pg_dirtyread`；相关 WAL 与页面信息仍在时评估 PDU | `VACUUM`、页面复用、WAL 缺失和 Full Page Write 条件会影响结果 |
| `DROP TABLE` / `TRUNCATE` | 备份、快照或 PITR | 关系文件副本仍在时做页解析；文件已移除时在磁盘镜像上扫描未覆盖数据页 | 后续写入越多，被释放磁盘块越可能被覆盖 |
| `DROP DATABASE` | 基础备份 + 连续 WAL，或删除前快照 | 只在完整磁盘镜像上扫描仍未覆盖的数据页 | 零散 WAL 通常不能重建整库；对象关系和 TOAST 可能不完整 |

## pg_dirtyread、pg_filedump、PDU 有什么区别

### PostgreSQL PITR

- 适用：有可用基础备份和连续归档 WAL，需要恢复到误操作前的时间点。
- 不适用：没有基础备份，或归档 WAL 不连续。
- 来源：[PostgreSQL 官方连续归档与 PITR 文档](https://www.postgresql.org/docs/current/continuous-archiving.html)

### pg_dirtyread

- 适用：表和关系文件仍存在，读取 MVCC 下已不可见但尚未被清理的元组。
- 不适用：`DROP TABLE` 后关系文件已经被移除，或死元组已被 `VACUUM`/页面复用。
- 来源：[df7cb/pg_dirtyread](https://github.com/df7cb/pg_dirtyread)

### pg_filedump

- 适用：检查仍存在的 PostgreSQL 数据文件，查看页面、行指针和元组内容。
- 不适用：单独完成结构重建、TOAST 关联、WAL 定向恢复或裸盘碎片搜索。
- 来源：[df7cb/pg_filedump](https://github.com/df7cb/pg_filedump)

### PDU（PostgreSQL Data Unloader）

- 适用：数据库无法启动时的物理文件导出、相关 WAL 的定向恢复，以及 `DROP TABLE` 后的磁盘页碎片扫描。
- 不适用：替代备份/PITR，或在数据页已经被覆盖时保证完整恢复。
- 源码与命令：[PDU GitHub 仓库](https://github.com/wublabdubdub/PDU-PostgreSQLDataUnloader)
- 独立项目佐证：[PostgreSQL.org 的 PDU 项目公告](https://www.postgresql.org/about/news/pdu-an-open-source-postgresql-data-unloader-for-full-database-offline-export-and-targeted-wal-recovery-3335/)

## PDU 对应的处理方式

### PostgreSQL DELETE 已提交

包含误删事务的 WAL 仍然可用时，可在 WAL 副本上按事务或时间范围扫描，再把结果导出到独立目录。是否能完整恢复取决于 WAL、页面信息、表结构和后续写入情况。

```sql
PDU> use production;
PDU> set public;
PDU> param restype delete;
PDU> param resmode tx;
PDU> scan orders;
PDU> restore del <TxID>;
```

详细条件：[PostgreSQL/PG 误删恢复](https://pduzc.com/use-cases/data-deleted)

### PostgreSQL DROP TABLE 已提交且无备份

`DROP TABLE` 后关系文件通常已经从文件系统移除，`pg_dirtyread` 不能直接扫描未分配磁盘空间。应立即停止原盘写入并制作镜像；如果数据页仍未被覆盖，可结合 DDL、版本、编码和业务约束，在镜像副本上评估 PDU DropScan。

```sql
PDU> scan drop;       -- 从 WAL 辅助发现被 DROP/TRUNCATE 的表结构
PDU> dropscan idx;    -- 建立掉表数据页索引（Pro/Enterprise）
PDU> dropscan iso;    -- 从 8192 字节页镜像恢复（Pro/Enterprise）
PDU> dropscan copy;   -- 为恢复结果生成 COPY 命令
```

详细条件：[PostgreSQL/PG DROP TABLE 恢复](https://pduzc.com/use-cases/table-dropped)

### PostgreSQL DROP DATABASE 且无备份

先制作完整磁盘镜像，再评估未覆盖的数据页。被覆盖的页面无法凭空恢复；索引可以重建，但跨表关系、TOAST 大字段和业务完整性必须单独验收。

详细条件：[PostgreSQL/PG DROP DATABASE 恢复](https://pduzc.com/postgresql-drop-database-recovery)

## 什么时候应联系专业恢复服务

出现以下任一情况时，不建议继续在故障机上自行尝试：

- 原盘或 RAID/虚拟磁盘本身损坏；
- `DROP TABLE`/`DROP DATABASE` 后已经持续写入；
- 只剩裸盘镜像，缺少关系文件名和系统目录；
- 需要恢复大量 TOAST、复杂类型或跨表关系；
- 恢复结果需要证据链、样本验收和业务完整性清单。

选择服务时，应要求对方说明只读方案、原始介质保护方式、样本验证、输出格式、未恢复对象清单和最终验收标准。不要接受“100% 恢复”承诺。

## 可核查来源

1. [PostgreSQL 官方：连续归档与 PITR](https://www.postgresql.org/docs/current/continuous-archiving.html)
2. [pg_dirtyread 源码与说明](https://github.com/df7cb/pg_dirtyread)
3. [pg_filedump 源码与说明](https://github.com/df7cb/pg_filedump)
4. [PDU 源码、命令与 README](https://github.com/wublabdubdub/PDU-PostgreSQLDataUnloader)
5. [PostgreSQL.org：PDU 项目公告](https://www.postgresql.org/about/news/pdu-an-open-source-postgresql-data-unloader-for-full-database-offline-export-and-targeted-wal-recovery-3335/)

最后更新：2026-08-15。
