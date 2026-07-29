# RaftKV

项目时间：2025.01.09 ~ 2025

## 项目简介

`RaftKV` 是一个使用 Go 从零实现的分布式强一致 Key-Value 存储系统。项目围绕自研 Raft 共识模块构建，采用共识层与 KV 状态机解耦的分层架构，通过自定义 gRPC 协议实现节点间共识通信与客户端 KV 访问。

系统完整覆盖强一致 KV 的核心链路：Leader 选举、日志复制、ReadIndex 线性一致读、`ClientID + Seq` 请求去重、MVCC 多版本存储、Revision 历史读、Range/Prefix Range、Txn 事务、Watch 流式订阅、Lease/TTL/KeepAlive、Leader 驱动的过期回收、WAL/Snapshot 双槽位持久化、manifest 原子切换、日志压缩、快照安装、崩溃重启恢复以及落后节点追赶。

## 快速开始

- [启动项目说明](docs/startup.md)

## 核心特性

- **Raft 共识算法**
  - 实现 Leader 选举、日志复制、提交推进与状态应用
  - 支持 `AppendEntries`、`RequestVote`、`InstallSnapshot`
- **线性一致性 KV API**
  - 支持 `Put`、`Get`、`Delete`、`Range`、`Txn`
  - 写请求通过 Raft 提交后应用到状态机
  - 读请求在 Leader 上通过 ReadIndex 心跳确认后读取
- **请求去重与客户端重试**
  - 基于 `ClientID + Seq` 缓存请求结果
  - 避免客户端重试导致重复写入
- **Snapshot & Log Compaction**
  - 根据 Raft 状态大小自动生成快照
  - 支持快照安装、日志截断与崩溃恢复
- **MVCC 存储引擎**
  - 支持 Revision、多版本历史读、Range、Prefix Range
  - 支持基于 Compare/Then/Else 的事务操作
- **Watch 机制**
  - 支持 key / prefix 监听
  - 支持历史事件补发与服务端流式推送
  - 采用事件分发循环与非阻塞通知，避免慢消费者阻塞整体分发
- **Lease / TTL**
  - 支持租约创建、绑定 key、KeepAlive、Revoke
  - Leader 统一扫描过期租约，并通过 Raft 提交删除操作
- **Crash & Fault Tolerance**
  - 支持节点崩溃重启后的 WAL/Snapshot 恢复
  - 支持落后节点通过日志复制或快照追赶集群状态

## 系统架构

```text
Client
  ↓
API Layer (gRPC)
  ↓
Raft Consensus Layer
  ↓
State Machine
(MVCC KV / Lease / Watch)
  ↓
WAL + Snapshot
```

## 目录结构说明

```text
internal/
├── api          # 对外 KV API 与 gRPC proto 定义
├── client       # 客户端基础封装、重试、Watch 流处理
├── raft         # Raft 共识算法实现
│   ├── raft.go
│   ├── vote.go
│   ├── append.go
│   ├── leader.go
│   ├── read_index.go
│   ├── snapshot.go
│   └── persister.go
├── server       # 节点组装、KV Server、apply/read/lease 逻辑
├── storage
│   ├── mvcc     # MVCC 存储引擎
│   ├── wal      # WAL 持久化
│   └── snapshot # 快照持久化
```

## 核心设计说明

- [Raft Design](docs/raft.md)
- [MVCC Storage](docs/mvcc.md)
- [Snapshot & Recovery](docs/architecture.md)
- [Watch Mechanism](docs/watch.md)
- [Lease & TTL](docs/lease.md)

## 当前状态说明

- 当前对外访问方式为 gRPC，暂未提供 RESTful HTTP 接口。
- 当前节点间通信使用普通 gRPC 连接，暂未接入 mTLS。
- 当前 MVCC 后端以项目内存储抽象和内存 Backend 为主，未接入 BoltDB/RocksDB 等外部嵌入式数据库。
- Lease 与 Watch 已具备支撑分布式锁、服务注册发现等上层能力的基础机制，但项目当前未单独实现锁服务或服务注册发现模块。
