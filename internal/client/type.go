package client

import (
	"context"
	"etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/api/kv/pb"
	"sync"

	"google.golang.org/grpc"
)

var globalClientID int64

type Client struct {
	mu 			sync.Mutex

	servers		[]RPCClient
	leader 		int
	clientID 	int64
	seq 		int64
}

type Txn struct {
	client   *Client
	compares []*kv.Compare
	thenOps  []*kv.Op
	elseOps  []*kv.Op
}


// 用于Server push
type RPCStream interface {
	Recv(resp interface{})  error 
	Close() 				error
}

type RPCClient interface{
	Call(ctx context.Context, method string, req interface{}, resp interface{}) error

	// streaming
	Stream(ctx context.Context, method string, req interface{}) (RPCStream, error)
}

type rpcFunc func(srv int) error

type GrpcClient struct {
	cli		 pb.KVClient
	conn 	*grpc.ClientConn
}

type GrpcStream struct {
	stream   pb.KV_WatchClient
	cancel   context.CancelFunc
}