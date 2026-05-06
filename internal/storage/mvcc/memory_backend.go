// 提供阶段二默认使用的 Go 内存 backend
// 让现有 MVCC 逻辑先跑在 Backend 抽象之上
// 保持行为不变，同时为后续替换成 LSM backend 预留接缝
package mvcc

type MemoryBackend struct {
	data  map[string][]ValueRevision
	events []Event
	currentRev int64
	compactRev int64
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		data: make(map[string][]ValueRevision),
		events: make([]Event, 0, 1024),
	}
}

func (b *MemoryBackend) CurrentRev() int64 {
	return b.currentRev
}

func (b *MemoryBackend) SetCurrentRev(rev int64) {
	b.currentRev = rev
}

func (b *MemoryBackend) CompactRev() int64 {
	return b.compactRev
}

func (b *MemoryBackend) SetCompactRev(rev int64) {
	b.compactRev = rev
}

func (b *MemoryBackend) GetRevisions(key string) ([]ValueRevision, bool) {
	revisions, ok := b.data[key]
	if !ok {
		return nil, false
	}

	return cloneValueRevisions(revisions), true
}

func (b *MemoryBackend) SetRevisions(key string, revisions []ValueRevision) {
	b.data[key] = cloneValueRevisions(revisions)
}

func (b *MemoryBackend) Keys() []string {
	keys := make([]string, 0, len(b.data))
	for key := range b.data {
		keys = append(keys, key)
	}

	return keys
}

func (b *MemoryBackend) Events() []Event {
	return cloneEvents(b.events)
}

func (b *MemoryBackend) SetEvents(events []Event) {
	b.events = cloneEvents(events)
}

func (b *MemoryBackend) AppendEvent(event Event) {
	b.events = append(b.events, cloneEvent(event))
}

func (b *MemoryBackend) SnapshotState() BackendState {
	return BackendState{
		Data: cloneData(b.data),
		Events: cloneEvents(b.events),
		CurrentRev: b.currentRev,
		CompactRev: b.compactRev,
	}
}

func (b *MemoryBackend) ReplaceState(state BackendState) {
	b.data = cloneData(state.Data)
	b.events = cloneEvents(state.Events)
	b.currentRev = state.CurrentRev
	b.compactRev = state.CompactRev
}