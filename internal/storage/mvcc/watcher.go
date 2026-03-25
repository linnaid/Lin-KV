package mvcc

import (
	"etcd-KV/Tools"
	"strings"
)

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

func (s *KVStore) Watch(key string, fromRev int64, prefix bool) (<-chan Event, int64, error) {
	s.mu.Lock()

	if fromRev <= s.compactRev {
		s.mu.Unlock()
		Tools.Error("请求的是已被压缩的版本", fromRev, s.compactRev)
		return nil, -1, ErrCompacted
	}
	
	id := s.nextWatcherID
	s.nextWatcherID++

	ch := make(chan Event, 16)

	backlog := make([]Event, 0, len(s.events))
	for _, e := range s.events {
		if prefix {
			if strings.HasPrefix(e.Key, key) && e.Rev.Main >= fromRev {
				backlog = append(backlog, e)
			}
		} else {
			if e.Key == key && e.Rev.Main >= fromRev {
				backlog = append(backlog, e)
			}
		}
	}

	w := &Watcher{
		ID: id,
		Key: key,
		StartRev: fromRev,
		Prefix: prefix,
		Ch: ch,

		lastSentRev: fromRev - 1,
	}

	if prefix {
		s.prefixWatchers[key] = append(s.prefixWatchers[key], w)
	} else {
		s.watchers[key] = append(s.watchers[key], w)
	}
	
	s.watchersByID[id] = w

	s.mu.Unlock()
	go func() {
		for _, e := range backlog {

			select {
			case ch<-e:
			default:
			}
		}
	}()

	return ch, id, nil
}

func (s *KVStore) dispatcherLoop() {

	for {
		ev, ok := <-s.eventCh
		if !ok {
			Tools.Debug("dispatcherLoop not found eventCh")
			return
		}

		s.mu.RLock()
		watchers := append([]*Watcher(nil), s.watchers[ev.Key]...)
		s.mu.RUnlock()

		for _, w := range watchers {
			// if ev.Rev.Main < w.StartRev {
			// 	continue
			// }
			if ev.Rev.Main <= w.lastSentRev {
				continue
			}

			select {
			case w.Ch<-ev:
				w.lastSentRev = ev.Rev.Main
			default:
				s.CancelWatcher(w.ID)
			}
		}

		s.mu.RLock()
		prefixWatchers := make(map[string][]*Watcher, len(s.prefixWatchers))

		for k, v := range s.prefixWatchers {
			prefixWatchers[k] = append([]*Watcher(nil), v...)
		}
		s.mu.RUnlock()

		for prefix, ws := range prefixWatchers {

			if strings.HasPrefix(ev.Key, prefix) {
				for _, w := range ws {

					// if ev.Rev.Main < w.StartRev {
					// 	continue
					// }
					if ev.Rev.Main <= w.lastSentRev {
						continue
					}

					select {
					case w.Ch<-ev:
						w.lastSentRev = ev.Rev.Main
					default:
						s.CancelWatcher(w.ID)
					}
				}
			}
		}
	}

}

func (s *KVStore) CancelWatcher(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, ok := s.watchersByID[id]
	if !ok {
		Tools.Debug("CancelWatcher id %d not found", id)
		return
	}

	if w.Prefix {
		ws := s.prefixWatchers[w.Key]

		for i, x := range ws {

			if x.ID == id {
				ws = append(ws[:i], ws[i+1:]...)
				break
			}
		}

		if len(ws) == 0 {
			delete(s.prefixWatchers, w.Key)
		} else {
			s.prefixWatchers[w.Key] = ws
		}

	} else {
		ws := s.watchers[w.Key]

		for i, x := range ws {

			if x.ID == id {
				ws = append(ws[:i], ws[i+1:]...)
				break
			}
		}

		if len(ws) == 0 {
			delete(s.watchers, w.Key)
		} else {
			s.watchers[w.Key] = ws
		}
	}

	delete(s.watchersByID, id)

	close(w.Ch)
}
