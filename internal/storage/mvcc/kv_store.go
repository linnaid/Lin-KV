package mvcc

import (
	"etcd-KV/Tools"
	"etcd-KV/internal/pb/mvcc"
	"sort"

	"google.golang.org/protobuf/proto"
)

func NewKVStore() *KVStore {
	kv := &KVStore{
		data: make(map[string][]ValueRevision),
		events: make([]Event, 0, 1024),
		watchers: make(map[string][]*Watcher),
		watchersByID: make(map[int64]*Watcher),
	}

	return kv
}

// 覆盖写，不区分是否存在
func (s *KVStore) Put(k string, v []byte) Revision{
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentRev++
	rev := Revision{
		Main: s.currentRev,
		Sub: 0,
	}

	// 防御性拷贝，避免外部修改影响内部状态
	value := make([]byte, len(v))
	copy(value, v)

	s.data[k] = append(s.data[k], ValueRevision{
		Rev: rev,
		Value: value,
		Deleted: false,
	})

	e_val := make([]byte, len(value))
	copy(e_val, value)

	s.events = append(s.events, Event{
		Type: EventPut,
		Key: k,
		Value: e_val,
		Rev: rev,
	})

	return rev
}

// func (s *KVStore) Get(k string) ([]byte, bool) {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	v, ok := s.kv[k]

// 	if !ok {
// 		return nil, false
// 	} else {
// 		value := make([]byte, len(v))
// 		copy(value, v)
		
// 		return value, true
// 	}
// }

func (s *KVStore) Get(k string, rev int64) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	version, ok := s.data[k]
	if !ok {
		return nil, false
	}

	if rev != 0 && rev <= s.compactRev {
		Tools.Warn("rev <= s.compactRev", rev, s.compactRev)
		return nil, false
	}

	if rev == 0 {
		rev = s.compactRev
	}

	if len(version) == 0 {
		Tools.Debug("versions的长度为0,In Get")
		return nil, false
	}

	left := 0
	right := len(version) - 1
	pos := -1

	for left <= right {
		mid := (left + right)/2

		if version[mid].Rev.Main <= rev {
			pos = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if pos == -1 {
		Tools.Warn("pos == -1")
		return nil, false
	}

	v := version[pos]
	if v.Deleted {
		Tools.Warn("v.Deleted = true")
		return nil, false
	}
	val := make([]byte, len(v.Value))
	copy(val, v.Value)

	return val, true
	// for i := len(version) - 1; i >= 0; i-- {
	// 	if version[i].Rev.Main <= rev {
	// 		if version[i].Deleted {
	// 			return nil, false
	// 		}
	// 		value := version[i].Value
	// 		return value, true
	// 	}
	// }

	// return nil, false
}

func (s *KVStore) Delete(k string) Revision{
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentRev++
	rev := Revision{
		Main: s.currentRev,
		Sub: 0,
	}

	s.data[k] = append(s.data[k], ValueRevision{
		Rev: rev,
		Deleted: true,
	})

	s.events = append(s.events, Event{
		Type: EventDelete,
		Key: k,
		Rev: rev,
		Value: nil,
	})

	return rev
}

func (s *KVStore) Restore(snapshot []byte) {
	var snap mvcc.Snapshot
	err := proto.Unmarshal(snapshot, &snap)
	if err != nil {
		panic(err)
	}
	if snap.CurrentRev < 0 {
		Tools.Error("snap.CurrentRev < 0")
		return
	}
	if snap.Entries == nil {
		Tools.Error("snap.Entires == nil")
		return 
	}
	entries := snap.Entries
	for i, e := range entries {
		if e.Key == "" {
			Tools.Warn("e.Key == ”“, 第", i)
		}
	}

	// revision连续性校验
	seen := map[int64]struct{}{}
	var maxRev int64

	for _, e := range entries {
		for _, rev := range e.Revisions {
			// if _, ok := seen[rev.Rev.Main]; ok {
			// 	Tools.Error("duplicate revision(重复Revision)", rev.Rev.Main)
			// 	return
			// }
			if rev.Rev.Main <= 0 {
				Tools.Error("invalid revision(非法Revision)", rev.Rev.Main)
			}
			seen[rev.Rev.Main] = struct{}{}
			if rev.Rev.Main > maxRev {
				maxRev = rev.Rev.Main
			}
		}
	}
	if maxRev != snap.CurrentRev {
		Tools.Error("maxRev != snap.CurrentRev", maxRev, snap.CurrentRev)
		return
	}
	for i := int64(1); i <= maxRev; i++ {
		if _, ok := seen[i]; !ok {
			Tools.Error("snapshot不连续", seen, maxRev, "missing = ", i)
			return
		}
	}

	// 构建dataMap
	dataMap := make(map[string][]ValueRevision, len(entries))
	for _, e := range entries {
		key := e.Key
		newSlice := make([]ValueRevision, len(e.Revisions))

		for i, v := range e.Revisions {
			valCopy := make([]byte, len(v.Value))
			copy(valCopy, v.Value)

			newRevision := Revision{
				Main: v.Rev.Main,
				Sub: v.Rev.Sub,
			}
			newSlice[i] = ValueRevision{
				Rev: newRevision,
				Value: valCopy,
				Deleted: v.Deleted,
			}
		}
		dataMap[key] = newSlice
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = dataMap
	s.currentRev = snap.CurrentRev

	// TODO: decode snapshot into kv state
}

func (s *KVStore) Snapshot() []byte {
	// 锁下神拷贝
	s.mu.RLock()

	newMap := make(map[string][]ValueRevision, len(s.data))

	for k, versions := range s.data {
		newSlice := make([]ValueRevision, len(versions))

		for i, v := range versions {
			// 深拷贝
			valCopy := make([]byte, len(v.Value))
			copy(valCopy, v.Value)

			newSlice[i] = ValueRevision{
				Rev: v.Rev,
				Value: valCopy,
				Deleted: v.Deleted,
			}
		}

		newMap[k] = newSlice
	}

	currentRev := s.currentRev

	s.mu.RUnlock()

	// 排序key
	keys := make([]string, 0, len(newMap))
	for k := range newMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 构造protobuf结构
	entries := make([]*mvcc.KeyEntry, 0, len(keys))

	for _, k := range keys {
		versions := newMap[k]

		sort.Slice(versions, func(i, j int) bool {
			a := versions[i].Rev
			b := versions[j].Rev

			if a.Main != b.Main {
				return a.Main < b.Main
			}
			return a.Sub < b.Sub
		})

		revs := make([]*mvcc.ValueRevision, len(versions))
		for i, v := range versions {
			re := &mvcc.Revision{
				Main: v.Rev.Main,
				Sub: v.Rev.Sub,
			}
			revs[i] = &mvcc.ValueRevision{
				Rev: re,
				Value: v.Value,
				Deleted: v.Deleted,
			}
		}

		entries = append(entries, &mvcc.KeyEntry{
			Key: k,
			Revisions: revs,
		})
	}

	snap := &mvcc.Snapshot{
		CurrentRev: currentRev,
		Entries: entries,
	}

	// 序列化
	data, err := proto.Marshal(snap)
	if err != nil {
		panic(err)
	}

	return data
}

// func (s *KVStore) Range(start , end string, rev int64) map[string][]byte {
// 	result := make(map[string][]byte, len(s.data))
// 	for k, _ := range s.data {
// 		if k >= start && k < end {
// 			v, ok := s.Get(k, rev)
// 			if ok {
// 				result[k] = v
// 			}
// 		}
// 	}
// 	return result
// }

func (s *KVStore) Range(startKey, endKey string, rev int64) []KeyValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if rev == 0 {
		rev = s.currentRev
	}
	if rev <= s.compactRev {
		return nil
	}

	result := make([]KeyValue, 0, len(s.data))

	for key, v := range s.data {
		if key >= startKey && key < endKey {
			n := len(v)
			for i := n - 1; i >= 0; i-- {
				if v[i].Rev.Main <= rev {
					if v[i].Deleted {
						break
					}

					val := make([]byte, len(v[i].Value))
					copy(val, v[i].Value)

					result = append(result, KeyValue{
						Key: key,
						Value: val,
						Rev: v[i].Rev,
					})
					break
				}
			}
		}
	}

	sort.Slice(result, func (i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

func (s *KVStore) Txn(txn Txn) []KeyValue {
	s.mu.Lock()
	defer s.mu.Unlock()

	
}