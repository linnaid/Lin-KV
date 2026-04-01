package kvserver

import (
	"errors"
	"etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/command"
	"etcd-KV/internal/raft"
	"etcd-KV/internal/storage/mvcc"
	"sync"
)


type waitEntry struct {
	Notify 		chan struct{}
	ClientID	int64
	Seq 	    int64
	// Rev 		*mvcc.Revision
	Key 		string
	OpType 		command.Type

	Result   	Result
	// Value 		[]byte
	// Err 		error
}

type ServerSnapshot struct {
	KVSnapshot		 []byte

	ClientLastSeq	 map[int64]int64
	// ClientLastValue	 map[int64][]byte
	ClientLastResult map[int64]Result
}

type Server struct {
	id 				int
	raft 			*raft.Raft
	store 			*mvcc.KVStore
	leaseMgr		*mvcc.LeaseManager

	applyCh chan 	raft.ApplyMsg  // 全局状态机推进流

	mu 				sync.Mutex
	waitCh 			map[int64]*waitEntry // 某一个 Raft log Index 的完成通知

	LastResult 	map[int64]Result

	// 客户端去重
	clientLastSeq 	map[int64]int64  // 最后一次执行请求的Seq
	// clientLastValue map[int64][]byte  // 最后一次执行请求的返回值(only Get)
	clientLastResult map[int64]Result // (All Order)

	maxraftstate 	int

	// client registry
	watchers 		map[string][]*watcher
}

var ErrNotLeader = errors.New("Is not Leader.")
var ErrTimeout = errors.New("Is TimeOut.")

type Result struct {
	Rev  	*mvcc.Revision
	Value 	[]byte
	Err 	error
	Found 	bool

	Cmd 	command.KVCommand
}

type watcher struct {
	key      string
	prefix   bool
	ch chan *kv.Event
}