package mvcc


func (s *KVStore) Range(startKey, endKey string, rev int64) []KeyValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if rev == 0 {
		rev = s.backend.CurrentRev()
	}
	if rev <= s.backend.CompactRev() {
		return nil
	}

	keys := s.backend.RangeKeys(startKey, endKey)
	result := make([]KeyValue, 0, len(keys))

	for _, key := range keys {
		if key >= startKey && key < endKey {
			v, ok := s.backend.GetRevisions(key)
			if !ok {
				continue
			}

			visible, ok := latestVisibleRevision(v, rev)
			if !ok {
				continue
			}

			value := append([]byte(nil), visible.Value...)
			result = append(result, KeyValue{
				Key: key,
				Value: value,
				Rev: visible.Rev,
			})
		}
	}

	return result
}

func (s *KVStore) PrefixRange(prefix string, rev int64) []KeyValue {
	
	end := prefixEnd(prefix)

	return s.Range(prefix, end, rev)
}

func prefixEnd(prefix string) string {
	b := []byte(prefix)

	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}

	return ""
}
