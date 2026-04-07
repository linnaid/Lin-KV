// KV api
package kv

import "context"

type WatchStream interface {
	Send(*WatchResponse) error 
	Context() context.Context
}

type KVService interface{
	Put(ctx context.Context, req *PutRequest) (*PutResponse, error)
	Get(ctx context.Context, req *GetRequest) (*GetResponse, error)
	Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error)

	Watch(req *WatchRequest, stream WatchStream) error
	Txn(ctx context.Context, req *TxnRequest) (*TxnResponse, error)

	LeaseGrant(ctx context.Context, 
		req *LeaseGrantRequest) (*LeaseGrantResponse, error)
	LeaseRevoke(ctx context.Context, 
		req *LeaseRevokeRequest) (*LeaseRevokeResponse, error)
	LeaseKeepAlive(ctx context.Context, 
		req *LeaseKeepAliveRequest) (*LeaseKeepAliveResponse, error)
}

