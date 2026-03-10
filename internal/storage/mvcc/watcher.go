package mvcc

// 2
func (s *KVStore) Watch(key string, fromRev int64) []Event {
	var result []Event
	for _, event := range s.events {
		if event.Rev.Main >= fromRev && event.Key == key {
			result = append(result, event)
		}
	}

	return result
}