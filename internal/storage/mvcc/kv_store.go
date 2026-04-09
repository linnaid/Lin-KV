package mvcc

import (
	"etcd-KV/Tools"
	"etcd-KV/internal/pb/mvcc"
	"sort"

	"google.golang.org/protobuf/proto"
)

func NewKVStore() *KVStore {
	return NewKVStoreWithOptions(StoreOptions{
		LeaseExpireMode: LeaseExpireLocal,
	})
}

func NewKVStoreWithOptions(opts StoreOptions) *KVStore {
	kv := &KVStore{
		data: make(map[string][]ValueRevision),

		events: make([]Event, 0, 1024),

		watchers:       make(map[string][]*Watcher),
		prefixWatchers: make(map[string][]*Watcher),
		watchersByID:   make(map[int64]*Watcher),

		keyLease: make(map[string]int64),

		eventCh: make(chan Event, 1024),
	}

	leaseMgr := NewLeaseManager(kv)

	kv.leaseMgr = leaseMgr

	// 开始循环leaseMgr，自动过期清理
	if opts.LeaseExpireMode == LeaseExpireLocal {
		go leaseMgr.expirationLoop()
	}

	go kv.dispatcherLoop()

	return kv
}

// 覆盖写，不区分是否存在
func (s *KVStore) Put(k string, v []byte, leaseID int64) Revision {
	s.mu.Lock()

	s.currentRev++

	rev := Revision{
		Main: s.currentRev,
		Sub:  0,
	}

	// 防御性拷贝，避免外部修改影响内部状态
	value := make([]byte, len(v))
	copy(value, v)

	s.data[k] = append(s.data[k], ValueRevision{
		Rev:     rev,
		Value:   value,
		Deleted: false,
	})

	e_val := make([]byte, len(value))
	copy(e_val, value)

	ev := Event{
		Type:  EventPut,
		Key:   k,
		Value: e_val,
		Rev:   rev,
	}

	s.events = append(s.events, ev)

	s.mu.Unlock()

	// attach lease
	if leaseID != 0 {
		s.leaseMgr.AttachKey(k, leaseID)
	}

	// 不能非阻塞发送，可能会丢事件
	// select {
	// case s.eventCh<-ev:
	// default:
	// }

	s.eventCh <- ev

	// s.notifyWatchers(Event{
	// 	Type: EventPut,
	// 	Key: k,
	// 	Rev: rev,
	// 	Value: value,
	// })

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

func (s *KVStore) Get(k string, rev int64) ([]byte, int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	version, ok := s.data[k]
	if !ok {
		return nil, rev, false
	}

	if rev != 0 && rev <= s.compactRev {
		Tools.Warn("rev <= s.compactRev", rev, s.compactRev)
		return nil, rev, false
	}

	if rev == 0 {
		rev = s.currentRev
	}

	if len(version) == 0 {
		Tools.Debug("versions的长度为0,In Get")
		return nil, rev, false
	}

	left := 0
	right := len(version) - 1
	pos := -1

	for left <= right {
		mid := (left + right) / 2

		if version[mid].Rev.Main <= rev {
			pos = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if pos == -1 {
		Tools.Warn("pos == -1", s.currentRev)
		return nil, rev, false
	}

	v := version[pos]
	if v.Deleted {
		Tools.Warn("v.Deleted = true")
		return nil, rev, false
	}

	val := make([]byte, len(v.Value))
	copy(val, v.Value)

	return val, rev, true
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

func (s *KVStore) Delete(k string) Revision {
	s.mu.Lock()
	rev, ev := s.deleteLocked(k)
	s.mu.Unlock()

	s.eventCh <- ev

	return rev
}

func (s *KVStore) deleteLocked(k string) (Revision, Event) {
	s.currentRev++
	rev := Revision{
		Main: s.currentRev,
		Sub:  0,
	}

	s.data[k] = append(s.data[k], ValueRevision{
		Rev:     rev,
		Deleted: true,
	})

	ev := Event{
		Type:  EventDelete,
		Key:   k,
		Rev:   rev,
		Value: nil,
	}

	s.events = append(s.events, ev)
	delete(s.keyLease, k)

	return rev, ev
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
				Sub:  v.Rev.Sub,
			}
			newSlice[i] = ValueRevision{
				Rev:     newRevision,
				Value:   valCopy,
				Deleted: v.Deleted,
			}
		}
		dataMap[key] = newSlice
	}

	events := make([]Event, 0, len(snap.Events))
	for _, ev := range snap.Events {
		if ev == nil || ev.Rev == nil {
			continue
		}
		events = append(events, Event{
			Type: EventType(ev.Type),
			Key: ev.Key,
			Value: append([]byte(nil), ev.Value...),
			Rev: Revision{
				Main: ev.Rev.Main,
				Sub: ev.Rev.Sub,
			},
		})
	}

	keylease := make(map[string]int64, len(snap.KeyLease))
	for k, leaseID := range snap.KeyLease {
		keylease[k] = leaseID
	}

	leases := s.leaseMgr.restore(snap.Leases)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = dataMap
	s.currentRev = snap.CurrentRev
	s.compactRev = snap.CompactRev
	s.events = events
	s.keyLease = keylease
	s.leaseMgr.nextLeaseID = snap.NextLeaseId
	s.leaseMgr.leases = leases

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
				Rev:     v.Rev,
				Value:   valCopy,
				Deleted: v.Deleted,
			}
		}

		newMap[k] = newSlice
	}

	events := make([]*mvcc.Event, 0, len(s.events))
	for _, ev := range s.events {
		events = append(events, &mvcc.Event{
			Type: mvcc.EventType(ev.Type),
			Key: ev.Key,
			Value: append([]byte(nil), ev.Value...),
			Rev: &mvcc.Revision{
				Main: ev.Rev.Main,
				Sub: ev.Rev.Sub,
			},
		})
	}
	sort.Slice(events, func(i, j int) bool {
		a := events[i].Rev
		b := events[j].Rev

		if a.Main != b.Main {
			return a.Main < b.Main
		}
		return a.Sub < b.Sub
	})

	keylease := make(map[string]int64, len(s.keyLease))
	for k, leaseID := range s.keyLease {
		keylease[k] = leaseID
	}

	leases, nextLeaseID := s.leaseMgr.snapshot()

	currentRev := s.currentRev
	compactRev := s.compactRev

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
				Sub:  v.Rev.Sub,
			}
			revs[i] = &mvcc.ValueRevision{
				Rev:     re,
				Value:   v.Value,
				Deleted: v.Deleted,
			}
		}

		entries = append(entries, &mvcc.KeyEntry{
			Key:       k,
			Revisions: revs,
		})
	}

	snap := &mvcc.Snapshot{
		CurrentRev: currentRev,
		CompactRev: compactRev,
		Entries:    entries,
		Events: 	events,
		Leases: 	leases,
		KeyLease: 	keylease,
		NextLeaseId: nextLeaseID,
	}

	// 序列化
	data, err := proto.Marshal(snap)
	if err != nil {
		panic(err)
	}

	return data
}
