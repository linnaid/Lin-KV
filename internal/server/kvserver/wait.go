// 超时等待处理
package kvserver


func (s *Server) clearWaitCh(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, ch := range s.waitCh {
		ch.Result.Err = err
		select {
		case <-ch.Notify:

		default:
			close(ch.Notify)
		}
		
		delete(s.waitCh, i)
	}
}