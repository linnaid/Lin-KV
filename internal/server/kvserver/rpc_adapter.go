package kvserver

import "etcd-KV/internal/api/kv/pb"

type RPCAdapter struct {
	pb.UnimplementedKVServer

	server *Server
}