package mvcc

import (
	"errors"
	"sync"
	"time"
)

var ErrCompacted = errors.New("压缩失败，它已被压缩")


type EventType uint8

const (
	EventPut EventType = iota
	EventDelete
)

type KVStore struct {
	mu sync.RWMutex
	
	backend Backend

	watchers map[string][]*Watcher
	watchersByID map[int64]*Watcher
	nextWatcherID int64
	prefixWatchers map[string][]*Watcher

	leaseMgr *LeaseManager

	keyLease map[string]int64

	eventCh chan Event
}

// 薄wrapper，提供更简洁的接口
func (s *KVStore) LeaseGrant(ttl int64) int64 {
	return s.leaseMgr.LeaseGrant(ttl)
}

func (s *KVStore) LeaseRevoke(id int64) error {
	return s.leaseMgr.LeaseRevoke(id)
}

func (s *KVStore) LeaseKeepAlive(id int64) (int64, error) {
	return s.leaseMgr.LeaseKeepAlive(id)
}

type Event struct {
	Type EventType
	Key string
	Value []byte
	Rev Revision
}

type Watcher struct {
	ID int64
	Key string
	Prefix bool // 是否精确监听
	StartRev int64
	Ch chan Event

	lastSentRev  int64
}

type kvSnapshot struct {
	CurrentRev int64
	CompactRev int64

	Data map[string][]ValueRevision
	Events []Event
}

type ValueRevision struct {
	Rev Revision
	Value []byte
	Deleted bool
}

type KeyValue struct {
	Key string
	Value []byte
	Rev Revision
}

// Txn
type Compare struct {
	Key string
	Op CompareOp
	Rev int64
}

type CompareOp int

const (
	CompareEqual CompareOp = iota
	CompareGreater
	CompareLess
)

type Operation struct {
	Type OpType
	Key string
	Value []byte
	LeaseID int64
}

type OpType int

const (
	OpGet OpType = iota
	OpPut
	OpDelete
)

type Txn struct {
	Compares []Compare
	ThenOps []Operation
	ElseOps []Operation
}

// Lease
type Lease struct {
	ID int64
	TTL int64
	ExpireAt time.Time
	Keys map[string]struct{}
}

type LeaseManager struct {
	// mu sync.Mutex
	leases map[int64]*Lease
	kv *KVStore
	nextLeaseID int64
}

type KV interface {
	Put(k string, v []byte)
	Get(k string) ([]byte, bool)
	Delete(k string)
}

type LeaseExpireMode uint8

const (
	LeaseExpireLocal LeaseExpireMode = iota
	LeaseExpireExternal
)

type StoreOptions struct {
	LeaseExpireMode LeaseExpireMode
}
