package kvserver

import "etcd-KV/internal/labgob"

func init() {
	labgob.Register(&ServerSnapshot{})
}