package client

import (
	"etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/api/kv/pb"
	"etcd-KV/internal/labrpc"
	"sync"

	"google.golang.org/grpc"
)

var globalClientID int64

type Client struct {
	mu 			sync.Mutex

	// servers []*labrpc.ClientEnd
	servers		[]RPCClient
	leader 		int
	clientID 	int64
	seq 		int64
}

type Txn struct {
	client   *Client
	ops 	[]*kv.Op
}


// 用于Server push
type RPCStream interface {
	Recv(resp interface{})  error 
	Close() 				error
}

type RPCClient interface{
	Call(method string, req interface{}, resp interface{}) error

	// streaming
	Stream(method string, req interface{}) (RPCStream, error)
}

type rpcFunc func(srv int) error

type LabrpcClient struct {
	end *labrpc.ClientEnd
}

type GrpcClient struct {
	cli		 pb.KVClient
	conn 	*grpc.ClientConn
}

type GrpcStream struct {
	stream   pb.KV_WatchClient
}