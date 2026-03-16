package mvcc

// 3

import "etcd-KV/Tools"

// 不可再Txn函数内调用API，避免造成死锁
func (s *KVStore) Txn(txn Txn) []KeyValue {
	s.mu.Lock()
	defer s.mu.Unlock()

	compareResult := true
	for _, t := range txn.Compares {
		var currentRev int64
		versions, ok := s.data[t.Key]
		if !ok {
			currentRev = 0
		} else {
			l := len(versions) - 1
			currentRev = versions[l].Rev.Main
		}

		switch t.Op {
		case CompareEqual:
			if currentRev != t.Rev {
				Tools.Error("Compare Error", t.Op, currentRev, t.Rev)
				compareResult = false
			}
		case CompareGreater:
			if currentRev <= t.Rev {
				Tools.Error("Compare Error", t.Op, currentRev, t.Rev)
				compareResult = false
			}
		case CompareLess:
			if currentRev >= t.Rev {
				Tools.Error("Compare Error", t.Op, currentRev, t.Rev)
				compareResult = false
			}
		}

		if !compareResult {
			break
		}
	}

	var ops []Operation
	if compareResult {
		ops = txn.ThenOps
	} else {
		ops = txn.ElseOps
	}

	result := make([]KeyValue, 0, len(ops))
	for _, op := range ops {

		switch op.Type {
		case OpGet:
			versions, ok := s.data[op.Key]
			if !ok {
				Tools.Debug("OpGet返回错误，Txn", op.Key)
			} else {
				l := len(versions) - 1
				latest := versions[l]
				
				if latest.Deleted {
					result = append(result, KeyValue{
						Value: latest.Value,
						Rev: latest.Rev,
					})
				} else {
					result = append(result, KeyValue{
						Key: op.Key,
						Value: latest.Value,
						Rev: latest.Rev,
					})
				}
				
			}
		case OpPut:
			s.currentRev++

			rev := Revision {
				Main: s.currentRev,
				Sub: 0,
			}

			value := make([]byte, len(op.Value))
			copy(value, op.Value)

			s.data[op.Key] = append(s.data[op.Key], ValueRevision{
				Rev: rev,
				Value: value,
				Deleted: false,
			})

			e_val := make([]byte, len(value))
			copy(e_val, value)

			ev :=Event{
				Type: EventPut,
				Key: op.Key,
				Value: e_val,
				Rev: rev,
			}

			s.events = append(s.events, ev)

			// s.mu.Unlock()
			// s.notifyWatchers(ev)
			// s.mu.Lock()

		case OpDelete:
			s.currentRev++

			rev := Revision{
				Main: s.currentRev,
				Sub: 0,
			}

			s.data[op.Key] = append(s.data[op.Key], ValueRevision{
				Rev: rev,
				Deleted: true,
			})

			ev := Event{
				Key: op.Key,
				Type: EventDelete,
				Rev: rev,
				Value: nil,
			}
			s.events = append(s.events, ev)
			
			select {
			case s.eventCh<-ev:
			default:
			}
			// s.mu.Unlock()
			// s.notifyWatchers(ev)
			// s.mu.Lock()
		}
	}

	return result
}