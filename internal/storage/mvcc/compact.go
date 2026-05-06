package mvcc

import (
	"etcd-KV/Tools"
)


// 1
func (s *KVStore) Compact(rev int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	compactRev := s.backend.CompactRev()
	currentRev := s.backend.CurrentRev()
	if rev <= compactRev || rev > currentRev {
		Tools.Warn("压缩不正确", rev, compactRev, currentRev)
		return ErrCompacted
	}

	for _, key := range s.backend.Keys() {
		versions, ok := s.backend.GetRevisions(key)
		if !ok {
			continue
		}

		var index int
		index = -1

		if len(versions) <= 1 {
			continue
		}

		for i, v := range versions {
			if v.Rev.Main > rev {
				index = i
				break
			}
		}

		var l int
		l = len(versions) - 1

		if index == 0 {
			continue
		} else if index == -1 {
			s.makeSlice(l+1, versions, key)
		} else {
			s.makeSlice(index, versions, key)
		}
	}

	events := s.backend.Events()
	newEvents := make([]Event, 0, len(events))

	for _, e := range events {
		if e.Rev.Main > rev {
			newEvents = append(newEvents, e)
		}
	}

	s.backend.SetEvents(newEvents)
	s.backend.SetCompactRev(rev)
	
	return nil
}

func (s *KVStore) makeSlice(index int, versions []ValueRevision, key string) {

	keep := versions[index-1:]
	newSlice := make([]ValueRevision, len(keep))
	copy(newSlice, keep)
	s.backend.SetRevisions(key, newSlice)
}
