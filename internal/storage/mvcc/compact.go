package mvcc

import (
	"errors"
	"etcd-KV/Tools"
)

var ErrCompacted = errors.New("压缩失败，它已被压缩")

// 1
func (s *KVStore) Compact(rev int64) error {
	if rev <= s.compactRev || rev > s.currentRev {
		Tools.Warn("压缩不正确", rev, s.compactRev, s.currentRev)
		return ErrCompacted
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for key, versions := range s.data {
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

	s.compactRev =  rev
	return nil
}

func (s *KVStore) makeSlice(index int, versions []ValueRevision, key string) {
	e_keep := s.events[index-1:]
	new_e_Slice := make([]Event, len(e_keep))
	copy(new_e_Slice, e_keep)
	s.events = e_keep
	
	keep := versions[index-1:]
	newSlice := make([]ValueRevision, len(keep))
	copy(newSlice, keep)
	s.data[key] = newSlice
}