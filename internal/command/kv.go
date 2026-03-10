// 定义raft日志里的命令格式
package command

type Type uint8

const (
	CmdPut Type = iota
	CmdGet
	CmdDelete
)

type KVCommand struct {
	Type Type // Put||Delete
	Key  string
	Value []byte
	ClientID int64
	Seq int64  	   // 客户端请求序号(用于幂等性和去重，防止Raft重放或客户端重试导致重复写)

	Rev int64
}

// 可序列化
// 不含业务逻辑