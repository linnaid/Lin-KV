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
	Notify   chan struct{}
	ClientID int64
	Seq      int64
	Kind     command.Kind
	// Key 		string
	// OpType 		command.Type

	Result Result
}

type ServerSnapshot struct {
	KVSnapshot []byte

	ClientLastSeq map[int64]int64
	// ClientLastValue	 map[int64][]byte
	ClientLastResult map[int64]Result
}

type Server struct {
	id    int
	raft  *raft.Raft
	store *mvcc.KVStore

	applyCh chan raft.ApplyMsg // 全局状态机推进流
	closeCh chan struct{}      // 关闭信号，通知后台循环退出。
	closeWg sync.WaitGroup     // 等待后台循环完全退出。

	mu     sync.Mutex
	waitCh map[int64]*waitEntry // 某一个 Raft log Index 的完成通知

	LastResult map[int64]Result

	// 客户端去重
	clientLastSeq    map[int64]int64  // 最后一次执行请求的Seq
	clientLastResult map[int64]Result // (All Order)

	maxraftstate int

	// client registry
	watchers  map[string][]*watcher
	closeOnce sync.Once
	closeErr  error
}

var ErrNotLeader = errors.New("Is not Leader.")
var ErrTimeout = errors.New("Is TimeOut.")
var ErrClosed = errors.New("Server is closed.")

type Result struct {
	Kind     command.Kind
	ClientID int64
	Seq      int64
	Err      error

	Rev   *mvcc.Revision
	Value []byte
	Found bool

	TxnSucceeded bool
	TxnResults   []*kv.KeyValue

	LeaseID  int64
	LeaseTTL int64

	RangeResults []*kv.KeyValue
}

type watcher struct {
	key    string
	prefix bool
	ch     chan *kv.Event
}
