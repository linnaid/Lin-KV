# Chinese(项目时间: 2025.1.9 ~ 2025.)

## 项目简介

一个使用 Go 实现的 **分布式一致性 Key-Value 存储系统**；
基于 **Raft 共识算法**，支持日志复制、快照压缩、MVCC、多版本事务、Watch 与 Lease 机制；
目标是实现一个 **类 etcd 的核心存储与一致性系统**。

## 核心特性

- **Raft 共识算法**
    - Leader 选举、日志复制、成员一致性
    - 支持 AppendEntries / RequestVote / InstallSnapshot
- **线性一致性 KV API**
  - Put / Get / Delete
  - 基于 Raft 的强一致写入
- **Snapshot & Log Compaction**
  - 自动快照生成
  - 崩溃恢复后正确重放状态
- **MVCC 存储引擎**
  - 多版本并发控制
  - Revision / Transaction 支持
- **Watch 机制**
  - 支持 key / prefix 监听
  - 事件驱动推送
- **Lease / TTL**
  - 键值绑定租约
  - 自动过期回收
- **Crash & Fault Tolerance**
  - 节点崩溃、网络不可靠情况下保证一致性


## 系统架构

Client  
  ▼  
API Layer (gRPC / HTTP)  
  ▼  
Raft Consensus Layer  
  ▼  
State Machine  
(MVCC KV / Lease / Watch)  
  ▼  
WAL + Snapshot

## 目录结构说明

internal/  
├── api          # 对外 API 定义（gRPC / KV 接口）  
├── client       # 客户端实现（重试、watch、lease）  
├── raft         # Raft 共识算法实现  
│   ├── raft.go  
│   ├── log.go  
│   ├── snapshot.go  
│   └── persister.go  
├── server       # 节点逻辑（apply、heartbeat、read）  
├── storage  
│   ├── mvcc     # MVCC 存储引擎  
│   ├── wal      # Write-Ahead Log  
│   └── snapshot # 快照存储  

## 核心设计说明

- [Raft Design](docs/raft.md)
- [MVCC Storage](docs/mvcc.md)
- [Snapshot & Recovery](docs/architecture.md)
- [Watch Mechanism](docs/watch.md)
- [Lease & TTL](docs/lease.md)# etcd-KV
