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
	
	data map[string][]ValueRevision

	currentRev int64
	compactRev int64
	lastCompactTime *time.Ticker

	events []Event
	watchers map[string][]*Watcher
	watchersByID map[int64]*Watcher
	nextWatcherID int64
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
	StartRev int64
	Ch chan Event
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

type KV interface {
	Put(k string, v []byte)
	Get(k string) ([]byte, bool)
	Delete(k string)
}