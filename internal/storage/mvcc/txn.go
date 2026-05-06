package mvcc

// 3

import "etcd-KV/Tools"

// 不可再Txn函数内调用API，避免造成死锁
func (s *KVStore) Txn(txn Txn) (bool, []KeyValue, error) {
	s.mu.Lock()

	compareResult := true
	for _, t := range txn.Compares {
		var currentRev int64
		versions, ok := s.backend.GetRevisions(t.Key)
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

	for _, op := range ops {
		if op.Type != OpPut || op.LeaseID == 0 {
			continue
		}

		if _, err := s.leaseMgr.lookupLeaseLocked(op.LeaseID); err != nil {
			s.mu.Unlock()
			return false, nil, err
		}
	}

	result := make([]KeyValue, 0, len(ops))
	events := make([]Event, 0, len(ops))
	for _, op := range ops {

		switch op.Type {
		case OpGet:
			versions, ok := s.backend.GetRevisions(op.Key)
			if !ok {
				Tools.Debug("OpGet返回错误，Txn", op.Key)
			} else {
				l := len(versions) - 1
				latest := versions[l]

				value := append([]byte(nil), latest.Value...)
				if latest.Deleted {
					result = append(result, KeyValue{
						Value: value,
						Rev:   latest.Rev,
					})
				} else {
					result = append(result, KeyValue{
						Key:   op.Key,
						Value: value,
						Rev:   latest.Rev,
					})
				}

			}
		case OpPut:
			_, ev := s.putLocked(op.Key, op.Value)
			if op.LeaseID != 0 {
				s.leaseMgr.bindKeyToLeaseLocked(op.Key, op.LeaseID, s.leaseMgr.leases[op.LeaseID])
			} else {
				s.leaseMgr.detachKeyLocked(op.Key)
			}
			events = append(events, ev)

		case OpDelete:
			_, ev := s.deleteLocked(op.Key)
			events = append(events, ev)
		}
	}

	s.mu.Unlock()

	for _, ev := range events {
		s.eventCh <- ev
	}

	return compareResult, result, nil
}
