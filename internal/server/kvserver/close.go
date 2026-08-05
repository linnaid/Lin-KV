package kvserver

func (s *Server) Close() error {
	if s == nil {
		return nil
	}

	s.closeOnce.Do(func() {
		close(s.closeCh)
		s.clearWaitCh(ErrClosed)
		s.closeWg.Wait()
		if s.store != nil {
			s.closeErr = s.store.Close()
		}
	})

	return s.closeErr
}
