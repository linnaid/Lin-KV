// 定义 MVCC 层依赖的最小 Backend 抽象
// 让 KVStore 不再直接依赖内存 map/slice
// 为后续 MemoryBackend 和 LSMBackend 提供统一接缝
package mvcc

type BackendState struct {
	Data map[string][]ValueRevision
	Events []Event
	CurrentRev int64
	CompactRev int64
}

type AppliedIndexBackend interface {
	AppliedIndex() int64
	AppliedTerm()  int64
	SetApplied(index, term int64)
}

type Backend interface {
	CurrentRev() int64
	SetCurrentRev(rev int64)

	CompactRev() int64
	SetCompactRev(rev int64)

	GetRevisions(key string) ([]ValueRevision, bool)
	SetRevisions(key string, revisions []ValueRevision)

	// Keys() []string
	// 返回有序 Key 集合
	RangeKeys(startKey, endKey string) []string

	Events() []Event
	SetEvents(events []Event)
	AppendEvent(event Event)
	SnapshotState() BackendState
	ReplaceState(state BackendState)
}

func cloneValueRevisions(src []ValueRevision) []ValueRevision {
	if src == nil {
		return nil
	}

	dst := make([]ValueRevision, len(src))
	for i, version := range src {
		valueCopy := append([]byte(nil), version.Value...)
		dst[i] = ValueRevision{
			Rev: version.Rev,
			Value: valueCopy,
			Deleted: version.Deleted,
		}
	}

	return dst
}

func cloneEvent(src Event) Event {
	return Event{
		Type: src.Type,
		Value: append([]byte(nil), src.Value...),
		Key: src.Key,
		Rev: src.Rev,
	}
}

func cloneEvents(src []Event) []Event {
	if src == nil {
		return nil
	}

	dst := make([]Event, len(src))
	for i, event := range src {
		dst[i] = cloneEvent(event)
	}

	return dst
}

func cloneData(src map[string][]ValueRevision) map[string][]ValueRevision {
	if src == nil {
		return nil
	}

	dst := make(map[string][]ValueRevision, len(src))
	for key, revisions := range src {
		dst[key] = cloneValueRevisions(revisions)
	}

	return dst
}