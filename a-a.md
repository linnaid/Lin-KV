第一阶段：
1. internal/command：
    codec.go，实现编码，解码；
    command.go，创建 KVCommand 结构体；
2. internal/server/kvserver:
    server.go，创建 Server 结构体，实现接口；
    apply.go，实现与 Raft 的交互
3. internal/storage/mvcc:
    kv_store.go，实现最小键值存储