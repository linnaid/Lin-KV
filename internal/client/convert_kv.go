package client

import (
	kv "etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/api/kv/pb"
)

func toPBPutRequest(req *kv.PutRequest) *pb.PutRequest {
	if req == nil {
		return nil
	}

	return &pb.PutRequest{
		Key:      req.Key,
		Value:    append([]byte(nil), req.Value...),
		ClientId: req.ClientID,
		Seq:      req.Seq,
		LeaseId:  req.LeaseID,
	}
}

func fillPutResponseFromPB(dst *kv.PutResponse, src *pb.PutResponse) {
	if dst == nil || src == nil {
		return
	}

	dst.Err = src.Err
	dst.Revision = src.Revision
}

func toPBGetRequest(req *kv.GetRequest) *pb.GetRequest {
	if req == nil {
		return nil
	}

	return &pb.GetRequest{
		Key:      req.Key,
		ClientId: req.ClientID,
		Seq:      req.Seq,
		Revision: req.Revision,
	}
}

func fillGetResponseFromPB(dst *kv.GetResponse, src *pb.GetResponse) {
	if dst == nil || src == nil {
		return
	}

	dst.Value = append([]byte(nil), src.Value...)
	dst.Err = src.Err
	dst.Revision = src.Revision
	dst.Found = src.Found
}

func toPBDeleteRequest(req *kv.DeleteRequest) *pb.DeleteRequest {
	if req == nil {
		return nil
	}

	return &pb.DeleteRequest{
		Key:      req.Key,
		ClientId: req.ClientID,
		Seq:      req.Seq,
		LeaseId:  req.LeaseID,
	}
}

func fillDeleteResponseFromPB(dst *kv.DeleteResponse, src *pb.DeleteResponse) {
	if dst == nil || src == nil {
		return
	}

	dst.Deleted = src.Deleted
	dst.Revision = src.Revision
	dst.Err = src.Err
}

func toPBWatchRequest(req *kv.WatchRequest) *pb.WatchRequest {
	if req == nil {
		return nil
	}

	return &pb.WatchRequest{
		Key:      req.Key,
		Revision: req.Revision,
		ClientId: req.ClientID,
		Prefix:   req.Prefix,
		Seq:      req.Seq,
	}
}

func fillWatchResponseFromPB(dst *kv.WatchResponse, src *pb.WatchResponse) {
	if dst == nil || src == nil {
		return
	}

	dst.Err = src.Err
	dst.Revision = src.Revision
	dst.Events = fromPBEvents(src.Events)
}

func fromPBEvents(events []*pb.Event) []*kv.Event {
	if len(events) == 0 {
		return nil
	}

	out := make([]*kv.Event, 0, len(events))
	for _, event := range events {
		if event == nil {
			continue
		}
		out = append(out, fromPBEvent(event))
	}
	return out
}

func fromPBEvent(event *pb.Event) *kv.Event {
	if event == nil {
		return nil
	}

	opType := kv.OpPut
	if event.Type == "DELETE" {
		opType = kv.OpDelete
	}

	return &kv.Event{
		Type:  opType,
		Key:   event.Key,
		Value: append([]byte(nil), event.Value...),
		Rev:   event.Revision,
	}
}
