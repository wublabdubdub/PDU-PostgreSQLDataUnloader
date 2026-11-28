# Docker 重建后 PostgreSQL 数据全没了？先稳住旧数据卷再动手

重建 PostgreSQL 容器后，新库顺利启动，业务连上去却发现原来的表全空了——**别慌，更不要反复 `docker compose up/down` 或往旧磁盘写新数据！** 新容器能跑，只说明它挂上了一个全新的空目录。老数据还在不在，立刻按下面三步定位。

## 第一步：查清旧容器到底把 `PGDATA` 挂在了哪

第一时间停掉自动重启、重建脚本和可能向宿主机原盘写入的任务，然后核对旧容器的镜像版本、Compose 配置与挂载路径：
- 旧容器还在列表中：直接用 `docker inspect` 读取 `Mounts` 里的真实数据卷名或宿主机路径。
- 旧容器已被删：翻查历史 Compose 配置、CI/CD 发布记录，再对照宿主机现存 Docker 卷的标签与创建时间交叉确认。同名容器重建后极可能挂到不同卷，参考 [Docker 卷官方文档](https://docs.docker.com/engine/storage/volumes/)。

**重点避坑（PG 17 与 PG 18 默认路径差异）**：官方 `postgres` 镜像在 17 版默认 `PGDATA` 是 `/var/lib/postgresql/data`，到了 18 版改为 `/var/lib/postgresql/18/docker` 且卷声明路径在 `/var/lib/postgresql`。如果你用 PG 17 却只给 `/var/lib/postgresql` 配了卷，Docker 往往会自动给子目录 `/var/lib/postgresql/data` 建一个匿名卷——优先检查那个匿名卷是不是还安静地躺在宿主机上。详见 [PostgreSQL 官方镜像 PGDATA 说明](https://hub.docker.com/_/postgres#pgdata)。

## 第二步：搞清刚才到底删的是“容器”还是“卷”

删除容器和删除数据卷是两回事：
- 普通 `docker compose down` **不会删卷**，旧匿名卷或命名卷大概率原封不动留在宿主机里，只是新容器没挂回去。
- 如果敲了 `docker compose down -v`，Compose 管理的命名卷和匿名卷会被一并移除（但标了 `external` 的外部卷不受影响）；若采用宿主机目录绑定挂载（bind mount），则直接核查宿主机原目录是否已被清空。参考 [Docker Compose down 文档](https://docs.docker.com/reference/cli/docker/compose/down/) · [Docker 目录挂载文档](https://docs.docker.com/reference/cli/docker/compose/bind-mounts/)。

## 第三步：按现场状态果断决策

- **旧卷或宿主机原数据目录还在**：第一时间拷贝出只读副本！读取其中的 `PG_VERSION` 确认主版本（例如显示 `17` 就用隔离的 PostgreSQL 17 实例挂起验证），检查扩展与外置表空间是否齐全，隔离库验清核心数据后再切回生产。参考 [PostgreSQL 存储文件布局文档](https://www.postgresql.org/docs/17/storage-file-layout.html)。
- **旧卷与数据文件已被彻底删除且无备份**：**立刻冻结承载旧卷的宿主机磁盘或虚拟磁盘，严禁同盘写入或重建！** DUL 专家团队动用成熟的 **PostgreSQL DUL（PDU）** 商业版底层扫描引擎，可在原盘只读副本上逐页捞回尚未被覆盖的核心表数据。请按 [PostgreSQL 数据文件被删无备份抢救指南](https://dul.net.cn/postgresql-data-file-deleted/) 保住原盘并准备事故前表结构，或**直接拨打恢复专家热线 `+86 152 5185 3831`**（[在线联系](https://dul.net.cn/contact/)）第一时间止损抢救。
