一个基于 Raft 共识算法的分布式 KV 存储系统，支持快照、日志压缩、崩溃恢复和线性一致读写
etcd-kv/
├── cmd/
│   └── kv-server/               # 主服务入口
│       └── main.go
│
├── configs/                     # 配置文件（YAML/JSON）
│   └── config.yaml
|
├── docs/                            # 设计文档（强烈推荐）
│   ├── architecture.md              # 整体架构说明
│   ├── raft.md                      # Raft 集成设计
│   ├── mvcc.md                      # MVCC / revision 模型
│   ├── watch.md                     # Watch 语义与实现
│   └── lease.md                     # Lease / TTL 设计
│
├── pkg/                         # 可复用的库（公共逻辑）
│   ├── logger/                  # 日志封装（zap/logrus）
│   ├── httpapi/                 # 可选：HTTP API 封装
│   └── util/                    # 工具类（时间、bytes等）
│
├── internal/                    # 业务核心逻辑（不对外暴露）
|
│   ├── raft/                    # Raft 实现（你的 MIT Raft）
│   │   ├── raft.go
│   │   ├── log.go
│   │   ├── persister.go
│   │   ├── snapshot.go
│   │   └── read_index.go
|   
│   ├── command/                     # ⭐ 新增：Raft 日志协议层
│   │   ├── command.go               # Command 统一封装
│   │   ├── kv.go                    # Put / Delete 命令
│   │   ├── lease.go                 # Lease / KeepAlive 命令
│   │   └── txn.go                   # Txn 命令（可选）
│
│   │
│   ├── storage/                 # 存储层（MVCC + BoltDB）
|   |
│   │   ├── mvcc/                # ⭐ 系统核心
│   │   │   ├── kv_store.go      # 状态机(multi-version) MVCC Store(Apply)
│   │   │   ├── index.go         # key index: key -> rev list (key -> revisions)
│   │   │   ├── revision.go      # revision 定义
│   │   │   ├── event.go         # Watch Event 定义
│   │   │   ├── watcher.go       # watch 机制
│   │   │   ├── lease.go         # 租约 TTL 管理
│   │   │   ├── compact.go       # compaction 压缩(历史版本回收)
│   │   │   └── txn.go           # 原子事务（可选）
│   │   │
│   │   ├── wal/
│   │   │   ├── wal.go           # Write-Ahead Log
│   │   │   ├── segment.go
│   │   │   └── metadata.go
│   │   │
│   │   ├── snapshot/
│   │   │   ├── snapshot.go      # snapshot 保存/加载
│   │   │   └── metadata.go
│   │   │
│   │   └── storage.go           # 存储统一封装（对上提供接口）
│   
│   ├── server/                  # 服务器层（连接 Raft + Storage）
│   │   ├── node.go              # 一个节点的整体逻辑
│   │   ├── apply.go             # Raft ApplyLog → 状态机
│   │   ├── read.go              # Linearizable / Serializable Read
│   │   ├── cluster.go           # 集群管理(member add/remove)
│   │   └── heartbeat.go         # 租约与 keepalive(Lease KeepAlive 驱动)
│   
│   ├── api/                     # 对外 API（gRPC）
│   │   ├── kv/
│   │   │   ├── kv.proto
│   │   │   ├── kv.pb.go
│   │   │   └── kv_server.go     # Put/Get/Watch/Lease gRPC 实现
│   │   └── etcdctl/             # 可选：模拟一个 etcdctl
│   
│   ├── client/                  # 客户端 SDK（Go）
│   |   ├── client.go
│   |   ├── retry.go
│   |   ├── watcher.go
│   |   └── lease.go
|   
│   └── errors/                      # ⭐ 新增：语义级错误
│       ├── errors.go                 # ErrNotLeader / ErrCompacted
│       └── codes.go
│
├── scripts/                     # 脚本（启动集群、工具）
│   ├── run_cluster.sh
│   ├── build.sh
│   └── format.sh
│
├── tests/                       # 单测与集成测试
│   ├── raft_test.go
│   ├── mvcc_test.go
│   ├── watch_test.go
│   ├── lease_test.go
│   ├── snapshot_test.go
│   └── cluster_test.go
│
├── Makefile                     # make run / make test / build
├── go.mod
├── go.sum
└── README.md                    # 项目说明文档（非常重要）

必须有（重点）：
Raft（你自己的实现，不是库）
多节点 KV
Put / Get / Delete
Leader Election
Snapshot + Log Compaction
Crash + Restart 恢复
线性一致性写（强一致）

加分项（非常推荐）：
ReadIndex / lease-based read
Watch / long-poll（像 etcd watch）
TTL / lease
简单 CLI
Metrics（QPS、latency）



服务注册发现系统
service-registry/
├── cmd/
│   ├── server/
│   │   └── main.go            # 启动一个 registry 节点
│   └── client/
│       └── main.go            # 示例 / CLI
│
├── api/
│   ├── proto/
│   │   ├── registry.proto     # 注册 / 查询 / watch
│   │   ├── raft.proto         # Raft 内部 RPC
│   │   └── health.proto
│   └── generated/
│
├── internal/
│   ├── raft/
│   │   ├── node.go            # Raft Node 生命周期
│   │   ├── log.go
│   │   ├── snapshot.go
│   │   ├── transport.go
│   │   └── fsm.go             # Apply() → 状态机
│   │
│   ├── storage/
│   │   ├── kv.go              # 抽象 KV 接口
│   │   ├── mvcc.go            # revision / version
│   │   └── backend.go         # boltdb / memory
│   │
│   ├── registry/
│   │   ├── service.go         # Service / Instance 模型
│   │   ├── register.go        # 注册 / 反注册
│   │   ├── discover.go        # 查询逻辑
│   │   └── health.go
│   │
│   ├── lease/
│   │   ├── lease.go           # TTL / 心跳
│   │   └── manager.go
│   │
│   ├── watch/
│   │   ├── watcher.go         # Watch 管理
│   │   ├── event.go
│   │   └── dispatcher.go
│   │
│   ├── election/
│   │   ├── leader.go          # 服务级 leader 选举
│   │   └── campaign.go
│   │
│   ├── server/
│   │   ├── grpc.go            # gRPC Server
│   │   ├── handler.go         # API → Command
│   │   └── middleware.go
│   │
│   └── config/
│       └── config.go
│
├── client/
│   ├── registry/
│   │   ├── client.go          # SDK 对外接口
│   │   ├── watch.go
│   │   └── lease.go
│   └── discovery/
│
├── pkg/
│   ├── log/
│   ├── errors/
│   └── utils/
│
├── test/
│   ├── raft/
│   ├── registry/
│   └── integration/
│
├── deploy/
│   ├── docker/
│   └── k8s/
│
└── README.md
