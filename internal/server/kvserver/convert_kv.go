package kvserver

import (
	kv "etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/api/kv/pb"
)

func fromPBPutRequest(req *pb.PutRequest) *kv.PutRequest {
	if req == nil {
		return nil
	}

	return &kv.PutRequest{
		Key: req.Key,
		Value: req.Value,
		ClientID: req.ClientId,
		Seq: req.Seq,

		LeaseID: req.LeaseId,
	}
}

func toPBPutResponse(resp *kv.PutResponse) *pb.PutResponse {
	if resp == nil {
		return &pb.PutResponse{}
	}

	return &pb.PutResponse{
		Err: resp.Err,
		Revision: resp.Revision,
	}
}


func fromPBGetRequest(req *pb.GetRequest) *kv.GetRequest {
	if req == nil {
		return nil
	}

	return &kv.GetRequest{
		Key: req.Key,
		Revision: req.Revision,
		ClientID: req.ClientId,
		Seq: req.Seq,
	}
}

func toPBGetResponse(resp *kv.GetResponse) *pb.GetResponse {
	if resp == nil {
		return &pb.GetResponse{}
	}

	return &pb.GetResponse{
		Value: resp.Value,
		Err: resp.Err,
		Revision: resp.Revision,
		Found: resp.Found,
	}
}


func fromPBDeleteRequest(req *pb.DeleteRequest) *kv.DeleteRequest {
	if req == nil {
		return  nil
	}

	return &kv.DeleteRequest{
		Key: req.Key,
		ClientID: req.ClientId,
		Seq: req.Seq,

		LeaseID: req.LeaseId,
	}
}

func toPBDeleteResponse(resp *kv.DeleteResponse) *pb.DeleteResponse {
	if resp == nil {
		return  &pb.DeleteResponse{}
	}

	return &pb.DeleteResponse{
		Deleted: resp.Deleted,
		Revision: resp.Revision,
		Err: resp.Err,
	}
}

func toPBEvent(ev *kv.Event) *pb.Event {
	if ev == nil {
		return nil
	}

	t := "PUT"
	if ev.Type == kv.OpDelete {
		t = "DELETE"
	}

	return &pb.Event{
		Key: ev.Key,
		Value: append([]byte(nil), ev.Value...),
		Type: t,
		Revision: ev.Rev,
	}
}