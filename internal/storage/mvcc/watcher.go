package mvcc

// 2

// func (s *KVStore) Watcsh(key string, fromRev int64) []Event {
// 	s.mu.RLock()
// 	defer s.mu.RUnlock()

// 	var result []Event
// 	for _, event := range s.events {
// 		if event.Rev.Main >= fromRev && event.Key == key {
// 			result = append(result, event)
// 		}
// 	}

// 	return result
// }

func (s *KVStore) Watch(key string, fromRev int64) (<-chan Event, int64) {
	s.mu.Lock()

	id := s.nextWatcherID
	s.nextWatcherID++

	ch := make(chan Event, 16)

	events := make([]Event, len(s.events))
	for _, e := range s.events {
		if e.Key == key && e.Rev.Main >= fromRev {
			events = append(events, e)
		}
	}

	w := &Watcher{
		ID: id,
		Key: key,
		StartRev: fromRev,
		Ch: ch,
	}

	s.watchers[key] = append(s.watchers[key], w)
	s.watchersByID[id] = w

	s.mu.Unlock()
	for _, e := range events {
		select {
		case ch<-e:
		default:
		}
	}

	return ch, id
}

func (s *KVStore) notifyWacthers(ev Event) {
	s.mu.RLock()
	watchers := append([]*Watcher(nil), s.watchers[ev.Key]...)
	s.mu.RUnlock()

	for _, w := range watchers {
		if ev.Rev.Main < w.StartRev {
			continue
		}

		select {
		case w.Ch<-ev:
		default:
		}
	}

}