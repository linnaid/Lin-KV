# 启动项目说明

本文档说明如何在本地启动 `etcd-KV` 三节点集群。

## 环境要求

- Go 版本：建议使用 `go 1.24.9` 或兼容版本
- 操作系统：Linux / macOS / WSL
- 当前示例配置默认使用 Lin-DB MVCC Backend，需要先构建 `/home/linnaid/lin-DB/Lin-DB/build/liblindb_core.a`
- 默认端口：
  - Raft 节点间通信：`127.0.0.1:8001`、`127.0.0.1:8002`、`127.0.0.1:8003`
  - 客户端 gRPC 访问：`127.0.0.1:9001`、`127.0.0.1:9002`、`127.0.0.1:9003`

## 构建 Lin-DB

如果使用配置文件里的 `"storage_backend": "lindb"`，先构建 Lin-DB：

```bash
cd /home/linnaid/lin-DB
cmake -S Lin-DB -B Lin-DB/build -DLINDB_BUILD_BENCHMARKS=OFF
cmake --build Lin-DB/build --target lindb_core -j
```

如果只想临时回到内存后端，把 `configs/node-0.json`、`configs/node-1.json`、`configs/node-2.json` 里的 `storage_backend` 改成 `memory`，并用普通 `go build` 构建即可。

## 方式一：脚本启动三节点集群

在项目根目录执行：

```bash
bash scripts/run_cluster.sh
```

脚本会自动完成以下步骤：

- 构建 `kv-server` 可执行文件到 `bin/kv-server`
- 按 `configs/node-0.json`、`configs/node-1.json`、`configs/node-2.json` 启动三个节点
- 将节点日志写入 `data/logs/node-0.log`、`data/logs/node-1.log`、`data/logs/node-2.log`

查看日志：

```bash
tail -f data/logs/node-0.log
```

停止集群：

```text
在运行脚本的终端按 Ctrl+C
```

## 方式二：手动启动三节点集群

先构建服务端：

```bash
CGO_ENABLED=1 go build -tags lindb -o bin/kv-server ./cmd/kv-server
```

分别打开三个终端，在项目根目录依次执行：

```bash
./bin/kv-server -config configs/node-0.json
```

```bash
./bin/kv-server -config configs/node-1.json
```

```bash
./bin/kv-server -config configs/node-2.json
```

三个节点启动后会自动进行 Raft Leader 选举。客户端可以连接任意一个 `client_addr`，项目内客户端会在收到非 Leader 错误后重试其他节点。

## 启动交互式客户端

先确保三节点服务端已经启动，再构建客户端：

```bash
go build -o bin/kv-client ./cmd/kv-client
```

启动客户端：

```bash
./bin/kv-client
```

客户端默认连接 `127.0.0.1:9001`、`127.0.0.1:9002`、`127.0.0.1:9003`，写入或读取时如果打到 Follower，会自动尝试下一个节点。命令不区分大小写，例如 `put`、`PUT`、`Get` 都可以。

常用命令：

```text
PUT <key> <value>           写入 key/value
GET <key> [revision]        读取 key
DELETE <key>                删除 key
RANGE <prefix> [revision]   按前缀列出 key
WATCH <key> [revision]      后台监听单个 key
WATCHPREFIX <prefix> [rev]  后台监听前缀
WATCHES                     查看当前 watch id
UNWATCH <id|ALL>            停止 watch
HELP                        查看帮助
EXIT                        退出客户端
```

示例会话：

```text
kv> PUT foo bar
OK revision=1
kv> GET foo
VALUE key="foo" value="bar" revision=1
kv> WATCH foo
OK watch=1 key="foo" revision=1
kv> PUT foo baz
OK revision=2
[watch 1] PUT key="foo" value="baz" revision=2
kv> UNWATCH 1
OK unwatched 1
kv> EXIT
```

也可以不进入交互模式，直接执行一次命令：

```bash
./bin/kv-client PUT foo bar
./bin/kv-client GET foo
```

如果改了客户端端口，用 `-endpoints` 指定：

```bash
./bin/kv-client -endpoints 127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103
```

## 验证项目

验证 Lin-DB 后端：

```bash
CGO_ENABLED=1 go test -tags lindb ./tests -run LinDB -v
```

运行全部测试：

```bash
go test ./...
```

如果配置仍是 `storage_backend: "lindb"`，涉及 `cmd/kv-server` 或 `cluster` 的构建需要带上 `CGO_ENABLED=1 -tags lindb`。

## Lin-DB 后端阶段说明

当前 Lin-DB 接入属于 Phase A：Lin-DB 作为 MVCC Backend，写请求仍然必须经过 Raft apply 后进入状态机。该阶段用于验证 cgo、MVCC 多版本、Range、Snapshot/Restore 和 reopen 链路；尚未实现 `applied_index` / `applied_term` 与 MVCC 写入的同 batch 原子提交，因此不要把它当作最终生产级恢复语义。

只验证进程级三节点集群启动和基础读写：

```bash
go test ./tests -run TestIntegrationProcessClusterStartsThreeNodesAndHandlesBasicReadWrite -v
```

## 清理本地数据

如果需要重新启动一个干净集群，可以先停止所有节点，再删除本地运行数据：

```bash
rm -rf data/node-0 data/node-1 data/node-2 data/logs bin/kv-server
```

然后重新执行启动命令。

## 常见问题

- 如果启动失败并提示端口被占用，检查 `8001` ~ `8003` 和 `9001` ~ `9003` 是否已有进程占用。
- 如果希望修改端口或数据目录，编辑 `configs/node-0.json`、`configs/node-1.json`、`configs/node-2.json`，并确保三个配置文件中的 `peers` 列表保持一致。
- 如果节点反复重新选举，优先查看 `data/logs/` 下的节点日志，确认三个节点是否都已启动并能互相连接。
