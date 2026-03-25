// 整个系统的心脏
package kvserver

import (
	"etcd-KV/Tools"
	"etcd-KV/internal/command"
	"etcd-KV/internal/storage/mvcc"
)

func (s *Server) applyLoop() {
	Tools.Info("APPLY LOOP GOT MSG")
	for msg := range s.applyCh {
		// Tools.Info("APPLY INDEX", msg.CommandIndex)
		if msg.SnapshotValid {
			// s.mu.Lock()
			// s.store.Restore(msg.Snapshot)
			// // TODO: restore clientLastSeq + clientLastValue
			// s.mu.Unlock()
			// continue

			var snap ServerSnapshot

			err := command.Decode(msg.Command, &snap)
			if err != nil {
				Tools.Debug("SnapshotRestore error")
				panic(err)
			}

			s.mu.Lock()

			s.store.Restore(msg.Snapshot)
			s.clientLastSeq = snap.ClientLastSeq
			// s.clientLastValue = snap.ClientLastValue
			s.clientLastResult = snap.ClientLastResult

			s.mu.Unlock()
		}

		if !msg.CommandValid {
			continue
		}

		data:= msg.Command

		var cmd command.KVCommand

		err := command.Decode(data, &cmd)
		if err != nil {
			Tools.Debug("Decode error in applyLoop.")
			panic(err)
		}
// Tools.Debug("put", cmd.Key, cmd.Value, cmd.Type)

		// Dedup
		s.mu.Lock()

		lastSeq := s.clientLastSeq[cmd.ClientID]
		if cmd.Seq < lastSeq {
			s.mu.Unlock()
			continue
		}

		if cmd.Seq == lastSeq {

			commandIndex := change(msg.CommandIndex)

			if ch, ok := s.waitCh[commandIndex]; ok {
				if match(ch, &cmd) {
					// ch.Result = Result{
					// 	Cmd: cmd,
					// 	Value: s.clientLastValue[cmd.ClientID],
					// 	Err: nil,
					// 	Rev: &mvcc.Revision{
					// 		Main: cmd.Rev,
					// 	},
					// }
					ch.Result = s.clientLastResult[cmd.ClientID]
					close(ch.Notify)
					delete(s.waitCh, commandIndex)
				} else {
					delete(s.waitCh, commandIndex)
				}
			}

			s.mu.Unlock()
			continue
		}
		
		s.mu.Unlock()

		var value []byte
		var ok_value bool
		ok_get := false
		
		var rev mvcc.Revision

		switch cmd.Type {
		case command.CmdPut:
			// Tools.Debug("put", cmd.Key, cmd.Value)
			// lease 稍后实现
			rev = s.store.Put(cmd.Key, cmd.Value, 0)

		case command.CmdDelete:
			rev = s.store.Delete(cmd.Key)

		case command.CmdGet:
			ok_get = true
			value, _, ok_value = s.store.Get(cmd.Key, cmd.Rev)
			if !ok_value {
				value = nil
			}
		}
		
		s.mu.Lock()
		result := Result{
			lastSeq
		}
		index := change(msg.CommandIndex)
		s.clientLastSeq[cmd.ClientID] = cmd.Seq
		// Tools.Info("lastClientID", cmd.ClientID)
		if ch, ok := s.waitCh[index]; ok {

			if match(ch, &cmd) {

				if ok_get {
					ch.Result.Value = value
					s.clientLastValue[cmd.ClientID] = value
				} else {
					ch.Result.Rev = &rev
				}
				
				ch.Result.Err = nil
				close(ch.Notify)
				delete(s.waitCh, index)

			   } else {
				delete(s.waitCh, index)
			   }

		} else {
			// 🔥 关键：缓存结果（防止先 apply 后注册）
			s.LastResult[index] = Result{
				Rev: &rev,
				Value: value,
				Err: nil,
			}
		}
		s.mu.Unlock()
		
		if s.needSnapshot() {
			snapshot := s.makeSnapshot()
			s.raft.Snapshot(msg.CommandIndex, snapshot)
		}

	}

}