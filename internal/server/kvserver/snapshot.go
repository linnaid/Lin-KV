package kvserver

import (
	"google.golang.org/protobuf/proto"
)

func (s *Server) makeSnapshot() []byte {
	s.mu.Lock()

	kvSnap := s.store.Snapshot()

	snap := ServerSnapshot{
		KVSnapshot: append([]byte(nil), kvSnap...),
		ClientLastSeq: cloneSeqMap(s.clientLastSeq),
		ClientLastResult: cloneResultMap(s.clientLastResult),
	}

	s.mu.Unlock()

	data, err := proto.Marshal(toPBServerSnapshot(snap))
	if err != nil {
		panic(err)
	}

	return  data
}

func (s *Server) needSnapshot() bool {
	return s.raft.RaftStateSize() > s.maxraftstate
}