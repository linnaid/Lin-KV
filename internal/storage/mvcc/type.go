package mvcc

import (
	"sync"
	"time"
)


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
	lastCompactTime *time.Timer

	events []Event
}

type Event struct {
	Type EventType
	Key string
	Value []byte
	Rev Revision
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