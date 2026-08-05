package mvcc

func (s *KVStore) Close() error {
	if s == nil {
		return nil
	}

	s.closeOnce.Do(func() {
		close(s.closeCh)
		s.closeWg.Wait()
		if closer, ok := s.backend.(interface{ Close() error }); ok {
			s.closeErr = closer.Close()
		}
	})

	return s.closeErr
}
