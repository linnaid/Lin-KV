// 整个系统的心脏
package kvserver

import (
	"etcd-KV/Tools"
	"etcd-KV/internal/command"
	"fmt"
)

func (s *Server) applyLoop() {
	Tools.Info("APPLY LOOP GOT MSG")
	for msg := range s.applyCh {
		// Tools.Info("APPLY INDEX", msg.CommandIndex)
		if msg.SnapshotValid {
			s.restoreSnapshot(msg.Snapshot)
			continue
		}

		if !msg.CommandValid {
			continue
		}

		data:= msg.Command

		var env command.Command

		err := command.Decode(data, &env)
		if err != nil {
			Tools.Debug("Decode error in applyLoop.")
			panic(err)
		}

		index := change(msg.CommandIndex)
		clientID := env.ClientID()
		seq := env.Seq()

		// Dedup
		s.mu.Lock()
		lastSeq := s.clientLastSeq[clientID]

		if seq < lastSeq {
			s.mu.Unlock()
			continue
		}

		if seq == lastSeq {
			result := s.clientLastResult[clientID]

			if ch, ok := s.waitCh[index]; ok {
				if match(ch, &env) {
					ch.Result = result
					close(ch.Notify)
				}
				delete(s.waitCh, index)
			}
			s.mu.Unlock()
			continue
		}
		
		s.mu.Unlock()

		result := s.applyCommand(&env)
		result.Kind = env.Kind
		result.ClientID = clientID
		result.Seq = seq

		s.mu.Lock()
		s.clientLastSeq[clientID] = seq
		s.clientLastResult[clientID] = result
		// Tools.Info("lastClientID", cmd.ClientID)

		if ch, ok := s.waitCh[index]; ok {

			if match(ch, &env) {
				ch.Result = result
				close(ch.Notify)
			}
			delete(s.waitCh, index)
		} else {
			// 🔥 关键：缓存结果（防止先 apply 后注册）
			s.LastResult[index] = result
		}
		s.mu.Unlock()
		
		if s.needSnapshot() {
			snapshot := s.makeSnapshot()
			s.raft.Snapshot(msg.CommandIndex, snapshot)
		}

	}

}

func (s *Server) restoreSnapshot(data []byte) {
	var snap ServerSnapshot

	err := command.Decode(data, &snap)
	if err != nil {
		Tools.Debug("SnapshotRestore error")
		panic(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.store.Restore(snap.KVSnapshot)
	s.clientLastSeq = snap.ClientLastSeq
	s.clientLastResult = snap.ClientLastResult
}

func (s *Server) applyCommand(env *command.Command) Result {
	switch env.Kind {
	case command.KindKV:
		return s.applyKV(env.KV)
	case command.KindTxn:
		return s.applyTxn(env.Txn)
	case command.KindLeaseGrant:
		return s.applyLeaseGrant(env.LeaseGrant)
	case command.KindLeaseRevoke:
		return s.applyLeaseRevoke(env.LeaseRevoke)
	case command.KindLeaseKeepAlive:
		return s.applyLeaseKeepAlive(env.LeaseKeepAlive)
	default:
		return Result{
			Err: fmt.Errorf("Unknown command kind: %d", env.Kind),
		}
	}
}

func (s *Server) applyKV(cmd *command.KVCommand) Result {
	if cmd == nil {
		return Result{
			Err: fmt.Errorf("nil kv command"),
		}
	}

	switch cmd.Type {
	case command.CmdPut:
		rev := s.store.Put(cmd.Key, cmd.Value, cmd.LeaseID)
		return Result{
			Rev: &rev,
		}

	case command.CmdDelete:
		rev := s.store.Delete(cmd.Key)
		return Result{
			Rev: &rev,
		}

	default:
		return Result{
			Err: fmt.Errorf("unsupported kv command type %d", cmd.Type),
		}
	}
}

func (s *Server) applyTxn(cmd *command.TxnCommand) Result {
	if cmd == nil {
		return Result{
			Err: fmt.Errorf("nil txn command"),
		}
	}

	succeeded, kvs := s.store.Txn(toMVCCTxn(cmd))
	return Result{
		TxnSucceeded: succeeded,
		TxnResults: fromMVCCKeyValues(kvs),
	}
}

func (s *Server) applyLeaseGrant(cmd *command.LeaseGrantCommand) Result {
	if cmd == nil {
		return Result{
			Err: fmt.Errorf("nil lease grant command"),
		}
	}

	id := s.store.LeaseGrant(cmd.TTL)
	return Result{
		LeaseID: id,
		LeaseTTL: cmd.TTL,
	}
}

func (s *Server) applyLeaseRevoke(cmd *command.LeaseRevokeCommand) Result {
	if cmd == nil {
		return Result{
			Err: fmt.Errorf("nil lease revoke command"),
		}
	}

	err := s.store.LeaseRevoke(cmd.ID)
	if err != nil {
		return Result{
			Err: err,
		}
	}

	return Result{}
}

func (s *Server) applyLeaseKeepAlive(cmd *command.LeaseKeepAliveCommand) Result {
	if cmd == nil {
		return Result{
			Err: fmt.Errorf("nil lease keepalive command"),
		}
	}

	ttl, err := s.store.LeaseKeepAlive(cmd.ID) 
	if err != nil {
		return Result{
			Err: err,
		}
	}

	return Result{
		LeaseTTL: ttl,
		LeaseID: cmd.ID,
	}
}