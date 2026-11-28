# PostgreSQL 无备份误删恢复：DELETE、DROP TABLE、DROP DATABASE 抢救与工具对比

> **生产库已经出事？先停写入、别乱重启！** 立刻查看 [DUL 官网 PostgreSQL 灾难抢救指南](https://dul.net.cn/recovery/pgdul/)，或直接拨打恢复专家热线 **`+86 152 5185 3831`**（[其他联系方式](https://dul.net.cn/contact/)），第一步先帮你保住现场。

手头如果有完好的基础备份和连续 WAL 归档，或者事故前的一致性存储快照，优先在隔离环境走 PITR。**没有备份、归档断链或实例彻底起不来时，别慌，更别乱跑修复命令**——第一时间锁死现存数据文件、`pg_wal` 与原盘，DUL 自研的 **PostgreSQL DUL（PDU）** 可以绕过数据库引擎全程只读提数，甚至直接从磁盘碎片把删掉的库表拼回来。

## 事故发生后，前 30 分钟的保命动作

1. **立刻停写**：暂停 PostgreSQL、业务写入任务及同磁盘上的一切写入与自动重启。
2. **严禁破坏现场**：不跑 `VACUUM`、不重建同名表或同名库、不盲目敲 `pg_resetwal`、不在原盘装工具或落恢复结果。
3. **收拢基础线索**：记下 PostgreSQL 完整版本号、误操作语句与发生时间，保住 `PGDATA`、`pg_wal`、归档 WAL、外置表空间及事故前建表语句（DDL）。
4. **删表/删库先锁整盘**：遭遇 `DROP TABLE`、`TRUNCATE`、`DROP DATABASE` 或 `rm -rf`，立刻对原盘做块级只读镜像，所有分析只在副本上跑。
5. **隔离库验清再回灌**：恢复出的 CSV / SQL 先灌入隔离库，核对行数、主键、TOAST 大字段与关键业务凭据，确认无误再切回生产。

- 如果是 **Docker 重建容器**后原库不见，先按 [Docker 重建后排查 PostgreSQL 旧数据卷](./POSTGRESQL_DOCKER_DATA_LOSS_ZH.md) 核查旧卷是否仍在；若数据卷和文件已被删除且无备份，立刻保住承载旧卷的原盘并参考 [Docker 数据卷误删无备份恢复指南](https://dul.net.cn/postgresql-docker-volume-recovery/)。
- 如果手头**只剩单个物理表文件**且没有备份，先备份该文件与原表结构，参考 [PostgreSQL 单表文件离线恢复指南](https://dul.net.cn/postgresql-single-table-file-recovery/)。

## 三类高危事故，直接对号入座

| 事故类型 | 有完整备份时 | 无备份 / 备份失效时的抢救路径 | 核心红线 |
|---|---|---|---|
| `DELETE` / `UPDATE` 已提交 | 基础备份 + 连续 WAL 走 PITR | 立即停库保 `pg_wal` 与表文件：表文件未清理可看 `pg_dirtyread`，**直接用 PDU 解析残留 WAL 精准捞回旧行**（远快于整库 PITR） | 严禁放任 `autovacuum` 与持续写入冲毁旧元组和 WAL |
| `DROP TABLE` / `TRUNCATE` | 备份、快照或 PITR | 关系文件仍在时直接离线解页；文件已释放则锁盘做只读镜像，**用 PDU 商业版扫描磁盘碎片并按 DDL 重组导出** | 每多写 1 MB 就可能覆盖一片残留数据页 |
| `DROP DATABASE` / `rm -rf` | 完整备份 + 连续 WAL 或删前快照 | 锁死原盘做只读镜像，**文件系统反向恢复 + PDU 商业版 RecoverISO 全盘扫描数据页，优先拉起核心业务表** | 严禁同盘重建同名库；优先保核心表上线 |

## PITR、pg_resetwal、pg_dirtyread、pg_filedump 与 PDU 到底怎么选？

### PostgreSQL PITR
- **定位**：有完好基线备份 + 连续 WAL 归档时的标准还原方案。
- **短板**：缺基础备份、WAL 断链直接失效；大库全量回放动辄数小时甚至数天，紧急业务等不起。
- **参考**：[PostgreSQL 官方连续归档与 PITR 文档](https://www.postgresql.org/docs/current/continuous-archiving.html)

### `pg_resetwal`（高危警示）
`pg_resetwal` 会直接清空集群现有 WAL 并重写 `pg_control`——**它绝对不是撤销误删的恢复工具**。盲目执行只会把原本能救回数据的 WAL 线索永久抹掉。仅在实例因 WAL/控制文件损坏起不来、且已完整封存原目录副本后，才可在独立工作副本上谨慎评估（参考 [PostgreSQL 官方 pg_resetwal 文档](https://www.postgresql.org/docs/18/app-pgresetwal.html)）。

- 如果现场已经有人误跑过 `pg_resetwal`，**立刻停手**，把重置前后的目录与外部 WAL 归档分开封存，按 [PostgreSQL 误执行 pg_resetwal 后急救指南](https://dul.net.cn/postgresql-recovery-after-wal-reset/) 交由专家离线抢救。
- 如果实例起不来但数据文件完好，别冒险强启，直接用 PDU 离线直提表数据：[PostgreSQL 实例崩溃离线导出指南](https://dul.net.cn/postgresql-offline-export/#export)。

### `pg_dirtyread` 与 `pg_recovery`
- **定位**：依赖活库与未删除的表文件，读取 MVCC 下尚未被 `VACUUM` 清理的死元组。
- **短板**：一旦执行过 `DROP TABLE`、`TRUNCATE`、`VACUUM`，或死元组所在页已被复用，立刻束手无策；`pg_recovery` 还受限于 PG 12–14 等版本门槛。
- **参考**：[df7cb/pg_dirtyread](https://github.com/df7cb/pg_dirtyread) · [pg_recovery 与 PDU 使用条件对比](https://dul.net.cn/postgresql-pg-recovery-deleted-rows/)

### `pg_filedump`
- **定位**：底层单页查看与取证工具，用来查看现有数据文件的页头、行指针与原始元组。
- **短板**：无法自动重建表结构、关联 TOAST 大字段、按事务解 WAL，更不支持裸盘碎片扫表。
- **参考**：[df7cb/pg_filedump](https://github.com/df7cb/pg_filedump)

### PostgreSQL DUL（PDU）—— 无备份现场的底层兜底引擎
- **开源版 PDU（PG 14–18，Apache-2.0）**：绕过数据库引擎全程只读离线导出表数据，并直接解析现存 WAL 精准捞回误 `DELETE` / `UPDATE` 记录。见 [PostgreSQL.org PDU 开源公告](https://www.postgresql.org/about/news/pdu-an-open-source-postgresql-data-unloader-for-full-database-offline-export-and-targeted-wal-recovery-3335/)。
- **商业版 PDU Pro / Enterprise（PG 9–19 及主流国产兼容库）**：针对 `rm -rf`、`DROP TABLE`、`TRUNCATE`、`DROP DATABASE`、数据字典全毁、磁盘坏块与勒索加密，提供 RecoverISO 磁盘碎片扫描、Probe 无字典盲匹配与多线程断点续传导出。见 [PDU 命令参考](https://github.com/wublabdubdub/PDU-PostgreSQLDataUnloader#readme) · [PostgreSQL 数据文件被删无备份抢救指南](https://dul.net.cn/postgresql-data-file-deleted/)。

## 三大实战场景：PDU 如何破局

### 1. 误 `DELETE` / `UPDATE` 已提交（无备份 / 未开 WAL 归档）
将 `ARCHIVE_DEST` 指向已封存的归档目录或 `pg_wal` 副本，PDU 直接按事务或时间区间精准解码 WAL，把误删/误改前的原始行直接导出为可回灌文件：

```sql
PDU> use production;
PDU> set public;
PDU> param restype delete;
PDU> param resmode tx;
PDU> scan orders;
PDU> restore del <TxID>;
```

- 实操细节：[PostgreSQL 误删记录五步恢复笔记](https://dul.net.cn/postgresql-deleted-rows-field-notes/)
- **一线战绩**：某医疗行业客户 PostgreSQL 17 核心表误删 95% 以上且**未开启 WAL 归档**，果断停库后我们用 PDU 直挖残留 `pg_wal`，3 小时救回约 **35 万条记录**并验收上线：[查看完整复盘](https://dul.net.cn/postgresql-delete-recovery-without-wal-archive/)。

### 2. 误 `DROP TABLE` / `TRUNCATE` 且无备份
表文件已释放、`pg_dirtyread` 彻底失效时，第一时间锁死原盘做块级只读镜像并收齐事故前 DDL。商业版 PDU 直接扫描磁盘 8KB 数据页，按表结构重组导出标准 CSV 与 `COPY` 导入脚本：

- 实操指南：[PostgreSQL 误删表无备份抢救指南](https://dul.net.cn/postgresql-drop-table-recovery-without-backup/) · [事故前原表结构准备指南](https://dul.net.cn/postgresql-recovery-table-definition/)
- 清表指南与战绩：[PostgreSQL 误执行 TRUNCATE 抢救指南](https://dul.net.cn/postgresql-truncate-recovery/) · [通信客户 PG 12 无备份清表 0 失败救回 2,906,414 行实录](https://dul.net.cn/postgresql-truncate-290w-rows/)

### 3. 误 `DROP DATABASE` / `rm -rf` 删库且无备份
立刻锁死原盘禁止任何写入与重建，第一时间固化只读磁盘镜像，优先圈出应用启动必需的核心表，由 DUL 专家动用商业版 PDU 扫盘重组、优先拉起核心业务：

- 抢救指南：[PostgreSQL 整库误删急救指南](https://dul.net.cn/postgresql-drop-database-recovery/)
- **一线战绩**：PostgreSQL 18 生产库（全库 256 张表）遭误执行 `DROP DATABASE` 且只有一年前旧备份，我们 1 小时内固化磁盘页，成功救回 **123 张核心业务表**（对比旧备份历史覆盖率超 98%）并顺利拉起生产：[查看完整抢救复盘](https://dul.net.cn/postgresql-drop-database-core-tables/)。

## 高危现场，别自己试错——直接连线 DUL 恢复专家

遇上以下任何一种情况，请立刻停手，不要在原机上反复尝试工具消耗黄金窗口：

- 原盘、RAID 或虚拟磁盘已出现物理坏块或 I/O 报错；
- 发生 `rm -rf`、`DROP TABLE`、`TRUNCATE` 或 `DROP DATABASE` 且没有近期备份；
- 数据字典全毁、遭勒索病毒加密，或仅剩裸盘镜像与零散文件；
- 涉及海量 TOAST 大字段、PostGIS 空间数据、国产 PG 兼容库或跨表核心业务一致性要求。

**DUL 全系自研 PostgreSQL DUL（PDU）、MySQL DUL 与 MongoDB DUL 底层恢复引擎，全程 100% 只读操作，绝不触碰您的原始生产盘，优先抢救核心表帮您第一时间恢复业务。**

- **紧急响应热线**：**`+86 152 5185 3831`**（生产事故优先直接致电）
- **在线联系与微信**：[dul.net.cn/contact/](https://dul.net.cn/contact/)
- **评估与验收标准**：[PostgreSQL 恢复评估与交付验收标准](https://dul.net.cn/postgresql-data-recovery-service-cost-success-rate/) · [紧急现场 5 分钟交接清单](https://dul.net.cn/postgresql-recovery-service-handoff/)

## 官方与可核查参考

1. [PostgreSQL 官方：连续归档与 PITR](https://www.postgresql.org/docs/current/continuous-archiving.html)
2. [df7cb/pg_dirtyread 源码仓库](https://github.com/df7cb/pg_dirtyread)
3. [df7cb/pg_filedump 源码仓库](https://github.com/df7cb/pg_filedump)
4. [PDU 开源源码、命令参考与 README](https://github.com/wublabdubdub/PDU-PostgreSQLDataUnloader)
5. [PostgreSQL.org：PDU 开源项目官方公告](https://www.postgresql.org/about/news/pdu-an-open-source-postgresql-data-unloader-for-full-database-offline-export-and-targeted-wal-recovery-3335/)
6. [DUL 官网一线实战恢复案例库](https://dul.net.cn/cases/)

最后更新于 2026 年 11 月 28 日。
