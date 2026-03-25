package kvserver


func (s *Server) addWatcher(w *watcher) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.watchers[w.key] = append(s.watchers[w.key], w)
}
