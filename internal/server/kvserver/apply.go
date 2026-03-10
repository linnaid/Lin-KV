// 整个系统的心脏
package kvserver

import (
	"etcd-KV/Tools"
	"etcd-KV/internal/command"
)

func (s *Server) applyLoop() {
	for msg := range s.applyCh {
		// Tools.Debug("111")
		if msg.SnapshotValid {
			s.mu.Lock()
			s.kv.Restore(msg.Snapshot)
			// TODO: restore clientLastSeq + clientLastValue
			s.mu.Unlock()
			continue
		}
		if !msg.CommandValid {
			continue
		}
		data:= msg.Command

		cmd, err := command.Decode(data)
		if err != nil {
			panic(err)
		}
// Tools.Debug("put", cmd.Key, cmd.Value, cmd.Type)

		// Dedup
		s.mu.Lock()
		if cmd.Seq <= s.clientLastSeq[cmd.ClientID] {

			if ch, ok := s.waitCh[msg.CommandIndex]; ok {

				if cmd.Type == command.CmdGet {
					ch.Value = s.clientLastValue[cmd.ClientID] 
				}

				ch.Err = nil
				close(ch.Notify)
				delete(s.waitCh, msg.CommandIndex)
			}

			s.mu.Unlock()
			continue
		}
		
		s.mu.Unlock()

		var value []byte
		var ok_value bool
		ok_get := false
		
		switch cmd.Type {
		case command.CmdPut:
			Tools.Debug("put", cmd.Key, cmd.Value)
			s.kv.Put(cmd.Key, cmd.Value)

		case command.CmdDelete:
			s.kv.Delete(cmd.Key)

		case command.CmdGet:
			ok_get = true
			value, ok_value = s.kv.Get(cmd.Key, cmd.Rev)
			if !ok_value {
				value = nil
			}
		}
		
		s.mu.Lock()
		index := msg.CommandIndex
		s.clientLastSeq[cmd.ClientID] = cmd.Seq
		Tools.Info("lastClientID", cmd.ClientID)
		if ch, ok := s.waitCh[index]; ok {

			if ch.ClientID == cmd.ClientID && 
			   ch.Seq == cmd.Seq {

				if ok_get {
					ch.Value = value
					s.clientLastValue[cmd.ClientID] = value
				}
				
				ch.Err = nil
				close(ch.Notify)
				delete(s.waitCh, index)
			   } 
		}
		s.mu.Unlock()
	}
}