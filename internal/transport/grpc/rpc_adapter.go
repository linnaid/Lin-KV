package grpc

import (
	"context"
	"etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/server/kvserver"
	"time"
)

type RPCAdapter struct {
	core *kvserver.Server
}

func NewRPCAdapter(core *kvserver.Server) *RPCAdapter {
	return &RPCAdapter{
		core: core,
	}
}

func (r *RPCAdapter) Put (
	req *kv.PutRequest,
	res *kv.PutResponse,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	result, err := r.core.Put(ctx, req)

	if err != nil {
		res.Err = err.Error()
		return 
	}

	*res = *result
}

func (r *RPCAdapter) Get (
	req *kv.GetRequest,
	res *kv.GetResponse,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	result, err := r.core.Get(ctx, req)

	if err != nil {
		res.Err = err.Error()
		return
	}

	*res = *result
}

func (r *RPCAdapter) Delete (
	req *kv.DeleteRequest,
	res *kv.DeleteResponse,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	result, err := r.core.Delete(ctx, req)

	if err != nil {
		res.Err = err.Error()
		return
	}

	*res = *result
}