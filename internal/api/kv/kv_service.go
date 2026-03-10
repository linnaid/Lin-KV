// KV api
package kv

import "context"

type KVService interface{
	Put(ctx context.Context, req *PutRequest) (*PutResponse, error)
	Get(ctx context.Context, req *GetRequest) (*GetResponse, error)
	Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error)
}

