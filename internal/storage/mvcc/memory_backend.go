// 提供阶段二默认使用的 Go 内存 backend
// 让现有 MVCC 逻辑先跑在 Backend 抽象之上
// 保持行为不变，同时为后续替换成 LSM backend 预留接缝
package mvcc

import "sort"

type MemoryBackend struct {
	data  map[string][]ValueRevision
	sortedKeys []string
	events []Event
	currentRev int64
	compactRev int64
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		data: make(map[string][]ValueRevision),
		sortedKeys: make([]string, 0),
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
	cloned := cloneValueRevisions(revisions)
	if len(cloned) == 0 {
		delete(b.data, key)
		b.deleteKeyFromIndex(key)
		return
	}

	if _, existed := b.data[key]; !existed {
		b.insertKeyIntoIndex(key)
	}

	b.data[key] = cloned
}

func (b *MemoryBackend) RangeKeys(startKey, endKey string) []string {
	start := orderedKeyRangeStart(b.sortedKeys, startKey)
	end := orderedKeyRangeEnd(b.sortedKeys, start, endKey)

	return append([]string(nil), b.sortedKeys[start:end]...)
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

// 用快照状态恢复 backend，并重建派生索引
func (b *MemoryBackend) ReplaceState(state BackendState) {
	b.data = cloneData(state.Data)
	b.rebuildKeyIndex()
	b.events = cloneEvents(state.Events)
	b.currentRev = state.CurrentRev
	b.compactRev = state.CompactRev
}

func (b *MemoryBackend) insertKeyIntoIndex(key string) {
	pos := orderedKeyRangeStart(b.sortedKeys, key)

	if pos < len(b.sortedKeys) && b.sortedKeys[pos] == key {
		return
	}

	b.sortedKeys = append(b.sortedKeys, "")
	copy(b.sortedKeys[pos+1:], b.sortedKeys[pos:])

	b.sortedKeys[pos] = key
}

func (b *MemoryBackend) deleteKeyFromIndex(key string) {
	pos := orderedKeyRangeStart(b.sortedKeys, key)

	if pos >= len(b.sortedKeys) || b.sortedKeys[pos] != key {
		return
	}

	b.sortedKeys = append(b.sortedKeys[:pos], b.sortedKeys[pos+1:]...)
}

func (b *MemoryBackend) rebuildKeyIndex() {
	keys := make([]string, 0, len(b.data))
	for key := range b.data {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	b.sortedKeys = keys
}