package kvserver

import (
	"etcd-KV/Tools"
	kv "etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/api/kv/pb"
)

type RPCAdapter struct {
	pb.UnimplementedKVServer

	server *Server
}


func (r *RPCAdapter) Watch(req *pb.WatchRequest, stream pb.KV_WatchServer) error {

	mvcc_ch, id, err := r.server.store.Watch(req.Key, req.Revision, req.Prefix)
	if err != nil {
		Tools.Debug("rpc_Adapter Watch error", err.Error())
		return err
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

			var t string
			if ev.Type == kv.OpDelete {
				t = "DELETE"
			} else {
				t = "PUT"
			}
			
			resp := &pb.WatchResponse{
				Events: []*pb.Event{
					{
						Type: t,
						Key: ev.Key,
						Value: ev.Value,
					},
				},
				Revision: ev.Rev,
			}

			if err := stream.Send(resp); err != nil {
				return err
			}

		case <-stream.Context().Done():
			return nil
		}
	}
	// r.server.addWatcher(w)

	// for {
	// 	select {
	// 	case ev := <-w.ch:

			// var t string
			// if ev.Type == kv.OpDelete {
			// 	t = "DELETE"
			// } else {
			// 	t = "PUT"
			// }
			
			// resp := &pb.WatchResponse{
			// 	Events: []*pb.Event{
			// 		{
			// 			Type: t,
			// 			Key: ev.Key,
			// 			Value: ev.Value,
			// 		},
			// 	},
			// 	// revision 暂不处理
			// }

			// if err := stream.Send(resp); err != nil {
			// 	return err
			// }

	// 	case <-stream.Context().Done():
	// 		return nil
	// 	}
	// }
}