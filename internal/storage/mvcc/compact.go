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

	for _, key := range s.backend.RangeKeys("", "") {
		versions, ok := s.backend.GetRevisions(key)
		if !ok || len(versions) <= 1 {
			continue
		}

		firstAfter := firstRevisionAfter(versions, rev)
		s.rewriteCompactedHistory(key, versions, firstAfter)
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

func (s *KVStore) rewriteCompactedHistory(key string, versions []ValueRevision, firstAfter int) {
	if firstAfter == 0 {
		return
	}

	keepFrom := len(versions) - 1
	if firstAfter > 0 {
		keepFrom = firstAfter - 1
	}

	kept := cloneValueRevisions(versions[keepFrom:])
	s.backend.SetRevisions(key, kept)
}
