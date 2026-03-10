// 注册类型
package command

import "etcd-KV/internal/labgob"

func init() {
	labgob.Register(&KVCommand{})
}