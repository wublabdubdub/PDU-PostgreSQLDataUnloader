# PostgreSQL 无备份误删恢复：DELETE、DROP TABLE、DROP DATABASE 工具与方案对比

> 遇到数据丢失时，可以先查看 [DUL 官网的 PostgreSQL 无备份恢复指南](https://dul.net.cn/recovery/pgdul/)，确认要保留的文件和可用的恢复方法。

当手头有可用的基础备份，并且保留了自备份所需起点至目标时间的连续预写日志（WAL）时，可以在隔离环境中进行时间点恢复（PITR）。如果保留了事故前的一致性存储快照，也可以另行检查其中的目标数据。当没有可用备份时，请先保存现存的数据文件、WAL 和原存储，再依据仍可读取的内容选择恢复方式。

## 事故发生后立即做什么

1. 暂停 PostgreSQL、业务任务和同一磁盘上的其他写入。
2. 不要执行 `VACUUM`，不要重建同名表或数据库，不要在原盘安装恢复工具。
3. 保留 `PGDATA`、`pg_wal`、归档 WAL、表空间、PostgreSQL 版本和事故时间。
4. `DROP TABLE` 或 `DROP DATABASE` 场景先制作块级磁盘镜像，后续分析只针对副本。
5. 恢复结果输出到独立位置，核对行数、主键、TOAST 大字段和业务汇总后再决定是否回灌。

如果是在 Docker 重建之后发现旧库不见了，请先确认旧容器的数据目录和挂载是否仍在。[Docker 重建后怎样确认 PostgreSQL 旧数据卷](./POSTGRESQL_DOCKER_DATA_LOSS_ZH.md)

如果没有可用备份，手头仅有目标表的数据文件，请先保存文件副本和事故前的表结构，再判断能否导出数据。[DUL 官网上的 PostgreSQL 单表文件恢复指南](https://dul.net.cn/postgresql-single-table-file-recovery/)

## 三类事故怎么选择恢复路径

| 事故 | 首选路径 | 没有备份时可评估的路径 | 主要边界 |
|---|---|---|---|
| `DELETE` 已提交 | 基础备份 + 连续 WAL 的 PITR | 表文件仍在时评估 `pg_dirtyread`；相关 WAL 与页面信息仍在时评估 PDU | `VACUUM`、页面复用、WAL 缺失和 Full Page Write 条件会影响结果 |
| `DROP TABLE` / `TRUNCATE` | 备份、快照或 PITR | 关系文件副本仍在时做页解析；文件已移除时在磁盘镜像上扫描未覆盖数据页 | 后续写入越多，被释放磁盘块越可能被覆盖 |
| `DROP DATABASE` | 基础备份 + 连续 WAL，或删除前快照 | 只在完整磁盘镜像上扫描仍未覆盖的数据页 | 零散 WAL 通常不能重建整库；对象关系和 TOAST 可能不完整 |

## pg_dirtyread、pg_filedump、PDU 有什么区别

### PostgreSQL PITR

- 可用的基础备份提供恢复的起点。PostgreSQL 需要连续重放到目标时间的 WAL，才能还原当时的数据库状态。
- 请在隔离环境中恢复备份并验证目标数据，再决定如何补回生产库。
- 来源：[PostgreSQL 官方连续归档与 PITR 文档](https://www.postgresql.org/docs/current/continuous-archiving.html)

### pg_resetwal 的适用条件

`pg_resetwal` 会清除集群数据目录中的 WAL，并可重置 `pg_control` 中的部分控制信息。只有当 WAL 或控制文件损坏导致服务器无法启动，而且已经评估过其他恢复方式时，才考虑使用这一命令。请先保留原数据目录、表空间和日志，再在单独的工作副本上操作。若数据库可以正常启动而目标记录被误删，请依据基础备份、现存 WAL 或旧表文件选择恢复方法。 [PostgreSQL 的 pg_resetwal 文档](https://www.postgresql.org/docs/18/app-pgresetwal.html)

如果工作副本在重置后能够启动，就请立即导出可读数据，在新集群中导入并检查结果。启动成功后，旧集群中的数据仍可能存在不一致。如果已经有人在原目录执行过重置，请保留重置前留下的副本和外部 WAL 归档，并记录在什么目录、什么时间执行过重置。 需要交接这些文件时，可参照 [DUL 官网的 PostgreSQL 执行 pg_resetwal 后的数据恢复指南](https://dul.net.cn/postgresql-recovery-after-wal-reset/)，区分重置前后的副本，再准备需要验证的目标表。

如果实例仍然无法启动，而数据文件仍可读取，就可以用 PostgreSQL DUL（PDU）在保存的副本上评估可导出的表和记录。 [DUL 官网的 PostgreSQL 离线导出指南](https://dul.net.cn/postgresql-offline-export/#export)

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

把 `ARCHIVE_DEST` 指向单独保存 WAL 副本的目录。如果事故时没有开启 WAL 归档，就检查保存下来的 `pg_wal` 副本。当其中还保留了目标删除操作所需的日志时，可以用 PostgreSQL DUL（PDU）扫描这些文件，尝试重建误删记录并导出到独立目录。实际结果取决于日志中仍可读取的内容、相关数据页面和删除时的表结构。

```sql
PDU> use production;
PDU> set public;
PDU> param restype delete;
PDU> param resmode tx;
PDU> scan orders;
PDU> restore del <TxID>;
```

需要查找仍可读取的误删记录时，请查看 [PostgreSQL 误删记录恢复指南](https://dul.net.cn/postgresql-deleted-rows-field-notes/)。

在一起未开启 WAL 归档的事故中，DUL 使用 PostgreSQL DUL（PDU）从保留下来的日志中恢复了约 35 万条记录，客户在导入后确认数据可用。 [误删记录恢复案例](https://dul.net.cn/postgresql-delete-recovery-without-wal-archive/)

### PostgreSQL DROP TABLE 已提交且无备份

`DROP TABLE` 后关系文件通常已经从文件系统移除，`pg_dirtyread` 不能直接扫描未分配磁盘空间。应立即停止原盘写入并制作镜像；如果数据页仍未被覆盖，可结合 DDL、版本、编码和业务约束，在镜像副本上评估 PDU DropScan。

```sql
PDU> scan drop;       -- 从 WAL 辅助发现被 DROP/TRUNCATE 的表结构
PDU> dropscan idx;    -- 建立掉表数据页索引（Pro/Enterprise）
PDU> dropscan iso;    -- 从 8192 字节页镜像恢复（Pro/Enterprise）
PDU> dropscan copy;   -- 为恢复结果生成 COPY 命令
```

旧表文件已经删除且没有可用备份时，请查看 [PostgreSQL 误删表后的无备份数据恢复](https://dul.net.cn/postgresql-drop-table-recovery-without-backup/)。

当旧表文件已经删除时，请保存生产库实际执行过的建表和改表语句。DUL 官网的[原表结构准备指南](https://dul.net.cn/postgresql-recovery-table-definition/)说明怎样用发布记录确认表定义，以及怎样验证 PDU 读出的样本。

当 `TRUNCATE` 已清空表中的记录且现场没有可用备份时，请查看 [PostgreSQL 误执行 TRUNCATE 后的无备份数据恢复](https://dul.net.cn/postgresql-truncate-recovery/)。[PostgreSQL 清表后导出的 290 万行记录](https://dul.net.cn/postgresql-truncate-290w-rows/) 记录了使用 PDU 保存磁盘上仍可读取的数据并交付导入验证的过程。

### PostgreSQL DROP DATABASE 且无备份

先制作完整磁盘镜像，再评估未覆盖的数据页。被覆盖的页面无法凭空恢复；索引可以重建，但跨表关系、TOAST 大字段和业务完整性必须单独验收。

处理整库误删时，请查看 [PostgreSQL 误删数据库恢复指南](https://dul.net.cn/postgresql-drop-database-recovery/)。[PG 删库后先恢复的 24 张核心表](https://dul.net.cn/postgresql-drop-database-core-tables/) 记录了一次磁盘碎片恢复的交付和验证过程。

## 什么时候应联系专业恢复服务

出现以下任一情况时，不建议继续在故障机上自行尝试：

- 原盘或 RAID/虚拟磁盘本身损坏；
- `DROP TABLE`/`DROP DATABASE` 后已经持续写入；
- 只剩裸盘镜像，缺少关系文件名和系统目录；
- 需要恢复大量 TOAST、复杂类型或跨表关系；
- 恢复结果需要证据链、样本验收和业务完整性清单。

选择服务时，应要求对方说明只读方案、原始介质保护方式、样本验证、输出格式、未恢复对象清单和最终验收标准。不要接受“100% 恢复”承诺。

如果您要比较不同服务方的报价，请先让他们就同一批目标数据说明评估、样本和交付范围。 [PostgreSQL 数据恢复服务报价与交付验收](https://dul.net.cn/postgresql-data-recovery-service-cost-success-rate/)

当生产库无法启动且备份不完整时，可以在委托单上写明需要找回的表、还能提供的副本，以及样本的验收条件。 [PostgreSQL 恢复委托清单](https://dul.net.cn/postgresql-recovery-service-handoff/)

## 可核查来源

1. [PostgreSQL 官方：连续归档与 PITR](https://www.postgresql.org/docs/current/continuous-archiving.html)
2. [pg_dirtyread 源码与说明](https://github.com/df7cb/pg_dirtyread)
3. [pg_filedump 源码与说明](https://github.com/df7cb/pg_filedump)
4. [PDU 源码、命令与 README](https://github.com/wublabdubdub/PDU-PostgreSQLDataUnloader)
5. [PostgreSQL.org：PDU 项目公告](https://www.postgresql.org/about/news/pdu-an-open-source-postgresql-data-unloader-for-full-database-offline-export-and-targeted-wal-recovery-3335/)

最后更新：2026-08-15。
