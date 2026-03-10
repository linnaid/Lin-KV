package persister

import "sync"

type MemoryPersister struct {
	mu 		  sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *MemoryPersister {
	return &MemoryPersister{}
}

func clone(orig []byte) []byte {
	if orig == nil {
		return nil
	}
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *MemoryPersister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *MemoryPersister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *MemoryPersister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

func (ps *MemoryPersister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *MemoryPersister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
