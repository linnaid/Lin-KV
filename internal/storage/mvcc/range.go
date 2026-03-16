package mvcc

import "sort"

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

func (s *KVStore) PrefixRange(prefix string, rev int64) []KeyValue {
	if rev == 0 {
		rev = s.currentRev
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