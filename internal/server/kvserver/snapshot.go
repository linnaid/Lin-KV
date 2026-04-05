package kvserver

import (
	"etcd-KV/internal/command"

)

func (s *Server) makeSnapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	kvSnap := s.store.Snapshot()

	snap := ServerSnapshot{
		KVSnapshot: kvSnap,
		ClientLastSeq: s.clientLastSeq,
		ClientLastResult: s.clientLastResult,
	}

	data, err := command.Encode(&snap)
	if err != nil {
		panic(err)
	}

	return  data
}

func (s *Server) needSnapshot() bool {
	return s.raft.RaftStateSize() > s.maxraftstate
}