package mvcc

import "sort"


func (s *KVStore) Range(startKey, endKey string, rev int64) []KeyValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if rev == 0 {
		rev = s.backend.CurrentRev()
	}
	if rev <= s.backend.CompactRev() {
		return nil
	}

	keys := s.backend.Keys()
	result := make([]KeyValue, 0, len(keys))

	for _, key := range keys {
		if key >= startKey && key < endKey {
			v, ok := s.backend.GetRevisions(key)
			if !ok {
				continue
			}

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

func (s *KVStore) PrefixRange(prefix string, rev int64) []KeyValue {
	if rev == 0 {
		rev = s.backend.CurrentRev()
	}
	
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
