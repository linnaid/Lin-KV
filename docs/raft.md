
将Command 的类型换为[]byte, 而不再是 interface{};

KV里
Put的流程:
RPC -> Raft.Start -> 等待 commit -> 返回
具体：
Client
  ↓
Put()
  ↓
构造 Command{Type: PUT, Key, Value}
  ↓
raft.Start(command)
  ↓
得到 log index
  ↓
waitCh[index] 等待 apply
  ↓
applyLoop 执行 MVCC.Put
  ↓
唤醒 waitCh
  ↓
返回 PutResponse
