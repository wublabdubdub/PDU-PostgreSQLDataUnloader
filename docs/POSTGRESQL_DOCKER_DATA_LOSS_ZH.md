# Docker 重建后 PostgreSQL 数据没了，怎么确认旧数据卷还在？

重建 PostgreSQL 容器后，应用重新连上了数据库，原来的表却看不到了。请先记录旧容器使用的镜像版本、Compose 配置和挂载信息，并暂停可能写入、清理或重新部署原存储的任务。新容器能启动，只说明它能读写当前的数据目录。旧库是否还能找回，要看事故前的数据目录保存在哪里，以及那个位置现在还保留了什么。

## 找到事故前的数据目录和挂载

先查旧部署的 `PGDATA`。这是 PostgreSQL 保存集群数据的目录。再把这个目录与旧容器的挂载目标比对，找出数据实际存到了哪个 Docker 卷或宿主机目录。如果旧容器仍能在 Docker 中查看，可以用 `docker inspect` 读取它的挂载信息。如果旧容器已经移除，就查当时的部署配置和卷记录，并用现存卷的标签、创建时间和部署记录交叉确认。即使新旧容器同名，它们也可能使用不同的卷。[Docker 卷文档](https://docs.docker.com/engine/storage/volumes/)

官方 `postgres` 镜像的默认目录随主版本变化。以 17 和 18 为例，17 的默认数据目录是 `/var/lib/postgresql/data`。18 的默认数据目录改为 `/var/lib/postgresql/18/docker`，镜像声明的卷挂载位置是 `/var/lib/postgresql`。如果部署时设置过 `PGDATA`，就以旧部署的实际值为准。如果旧库使用 PostgreSQL 17，且只给 `/var/lib/postgresql` 配了卷，Docker 仍有可能为它的子目录 `/var/lib/postgresql/data` 单独创建了匿名卷。此时还要查这个匿名卷是否留下来。[PostgreSQL 官方镜像的 PGDATA 说明](https://hub.docker.com/_/postgres#pgdata)

## 查清当时删除了什么

删除容器和删除卷是两项操作。`docker compose down` 默认保留卷。如果移除容器时留下了匿名卷，后续的 Compose 部署通常不会自动重新挂载它。若当时执行了 `docker compose down -v`，则应确认 Compose 管理的命名卷和原容器挂载的匿名卷是否已被移除。声明为 `external` 的卷由 Compose 之外的流程管理，`down` 不会删除它。若旧部署使用的是宿主机目录挂载，请另查那个目录里的文件是否被清理。容器列表和卷列表都需要与事故前的部署记录一起看。[Docker Compose down 文档](https://docs.docker.com/reference/cli/docker/compose/down/) · [Docker 目录挂载文档](https://docs.docker.com/engine/storage/bind-mounts/)

## 根据旧卷和原文件的状态决定下一步

如果找到了旧卷或存有原数据目录的宿主机目录，请先保存副本，再读取其中的 `PG_VERSION`，确认集群的主版本。举例来说，若它记录的是 17，就应为副本准备隔离的 PostgreSQL 17 环境，并检查原库所用扩展及另外挂载的表空间文件是否齐全。请在隔离环境确认目标库表和已知的业务记录，再决定如何接回业务。[PostgreSQL 文件布局文档](https://www.postgresql.org/docs/17/storage-file-layout.html)

如果确认旧数据文件已经删除，又找不到可用的备份、快照或副本，请先保留原先承载旧卷或宿主机目录的存储，暂停往那里写入。DUL 有 PostgreSQL 无备份数据恢复能力，使用 PostgreSQL DUL（PDU）在原存储的只读副本上查找仍保留的表数据。恢复范围取决于还能读到多少数据。准备评估时，可参照 [DUL 官网的 PostgreSQL 数据文件被删后的无备份数据恢复指南](https://dul.net.cn/postgresql-data-file-deleted/) 保留材料和原表结构。
