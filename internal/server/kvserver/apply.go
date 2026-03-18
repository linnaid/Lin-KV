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
			s.clientLastValue = snap.ClientLastValue

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
		if cmd.Seq <= s.clientLastSeq[cmd.ClientID] {

			commandIndex := change(msg.CommandIndex)

			if ch, ok := s.waitCh[commandIndex]; ok {

				if cmd.Type == command.CmdGet {
					ch.Value = s.clientLastValue[cmd.ClientID] 
				}

				ch.Err = nil
				close(ch.Notify)
				delete(s.waitCh, commandIndex)
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
		index := change(msg.CommandIndex)
		s.clientLastSeq[cmd.ClientID] = cmd.Seq
		// Tools.Info("lastClientID", cmd.ClientID)
		if ch, ok := s.waitCh[index]; ok {

			if ch.ClientID == cmd.ClientID && 
			   ch.Seq == cmd.Seq {

				if ok_get {
					ch.Value = value
					s.clientLastValue[cmd.ClientID] = value
				} else {
					ch.Rev = &rev
				}
				
				ch.Err = nil
				close(ch.Notify)
				delete(s.waitCh, index)
			   } 
		}
		s.mu.Unlock()
		
		if s.needSnapshot() {
			snapshot := s.makeSnapshot()
			s.raft.Snapshot(msg.CommandIndex, snapshot)
		}

	}

}