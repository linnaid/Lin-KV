package kvserver

import (
	"context"
	"errors"
	"etcd-KV/Tools"
	"etcd-KV/internal/api/kv/pb"
	"etcd-KV/internal/storage/mvcc"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RPCAdapter struct {
	pb.UnimplementedKVServer

	server *Server
}


func NewRPCAdapter(server *Server) *RPCAdapter {
	return &RPCAdapter{
		server: server,
	}
}

func (r *RPCAdapter) Put(ctx context.Context, 
	req *pb.PutRequest) (*pb.PutResponse, error) {
		resp, err := r.server.Put(ctx, fromPBPutRequest(req))
		out := toPBPutResponse(resp)

		if err != nil {
			out.Err = err.Error()
			return out, nil
		}
		return out, nil
	}

func (r *RPCAdapter) Get(ctx context.Context, 
	req *pb.GetRequest) (*pb.GetResponse, error) {
		resp, err := r.server.Get(ctx, fromPBGetRequest(req))
		out := toPBGetResponse(resp)

		if err != nil {
			out.Err = err.Error()
			return out, nil
		}
		return out, nil
	}

func (r *RPCAdapter) Delete(ctx context.Context, 
	req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
		resp, err := r.server.Delete(ctx, fromPBDeleteRequest(req))
		out := toPBDeleteResponse(resp)

		if err !=nil {
			out.Err = err.Error()
			return out, nil
		}
		return out, nil
	}


func (r *RPCAdapter) Watch(req *pb.WatchRequest, 
	stream pb.KV_WatchServer) error {

	mvcc_ch, id, err := r.server.store.Watch(req.Key, req.Revision, req.Prefix)
	if err != nil {
		Tools.Debug("rpc_Adapter Watch error", err.Error())

		if errors.Is(err, mvcc.ErrCompacted) {
			return status.Error(codes.FailedPrecondition, err.Error())
		}

		return status.Error(codes.Internal, err.Error())
	}

	ch := convert(mvcc_ch)

	defer r.server.store.CancelWatcher(id)

	// w := &watcher{
	// 	key: req.Key,
	// 	prefix: req.Prefix,
	// 	ch: ch,
	// }

	for {
		select {
		case ev, ok := <-ch:

			if !ok {
				Tools.Debug("RPC_Adapter Watch ch error")
				return nil
			}
			
			resp := &pb.WatchResponse{
				Events: []*pb.Event{toPBEvent(ev)},
				Revision: ev.Rev,
			}

			if err := stream.Send(resp); err != nil {
				return err
			}

		case <-stream.Context().Done():
			return nil
		}
	}
}

func (r *RPCAdapter) Txn(ctx context.Context, 
	req *pb.TxnRequest) (*pb.TxnResponse, error) {
		resp, err := r.server.Txn(ctx, fromPBTxnRequest(req))
		out := toPBTxnResponse(resp)
		if err != nil {
			out.Err = err.Error()
		}
		return out, nil
	}

func (r *RPCAdapter) LeaseGrant(ctx context.Context, 
	req *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
		resp, err := r.server.LeaseGrant(ctx, fromPBLeaseGrantRequest(req))
		out := toPBLeaseGrantResponse(resp)
		if err != nil {
			out.Err = err.Error()
		}
		return out, nil
	}

func (r *RPCAdapter) LeaseRevoke(ctx context.Context, 
	req *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
		resp, err := r.server.LeaseRevoke(ctx, fromPBLeaseRevokeRequest(req))
		out := toPBLeaseRevokeResponse(resp)
		if err != nil {
			out.Err = err.Error()
		}
		return out, nil
	}

func (r *RPCAdapter) LeaseKeepAlive(ctx context.Context, 
	req *pb.LeaseKeepAliveRequest) (*pb.LeaseKeepAliveResponse, error) {
		resp, err := r.server.LeaseKeepAlive(ctx, fromPBLeaseKeepAliveRequest(req))
		out := toPBLeaseKeepAliveResponse(resp)
		if err != nil {
			out.Err = err.Error()
		}
		return out, nil
	}