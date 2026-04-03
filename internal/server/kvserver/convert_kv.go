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

func fromPBCompareOp(op pb.CompareOp) kv.CompareOp {
	switch op {
	case pb.CompareOp_COMPARE_EQUAL:
		return kv.CompareEqual
	case pb.CompareOp_COMPARE_GREATER:
		return kv.CompareGreater
	default:
		return kv.CompareLess
	}
}

func fromPBOpType(op pb.OpType) kv.OpType {
	switch op {
	case pb.OpType_OP_PUT:
		return kv.OpPut
	case pb.OpType_OP_GET:
		return kv.OpGet
	default:
		return kv.OpDelete
	}
}

func toPBKeyValue(item *kv.KeyValue) *pb.KeyValue {
	if item == nil {
		return nil
	}

	return &pb.KeyValue{
		Key: item.Key,
		Value: append([]byte(nil), item.Value...),
		Revision: item.Revision,
	}
}

func toPBKeyValues(items []*kv.KeyValue) []*pb.KeyValue {
	if len(items) == 0 {
		return nil
	}

	out := make([]*pb.KeyValue, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		out = append(out, toPBKeyValue(item))
	}
	return out
}

func fromPBTxnRequest(req *pb.TxnRequest) *kv.TxnRequest {
	if req == nil {
		return nil
	}

	out := &kv.TxnRequest{
		ClientID: req.ClientId,
		Seq: req.Seq,
		Compares: make([]*kv.Compare, 0, len(req.Compares)),
		ThenOps: make([]*kv.Op, 0, len(req.ThenOps)),
		ElseOps: make([]*kv.Op, 0, len(req.ElseOps)),
	}

	for _, item := range req.Compares {
		if item == nil {
			continue
		}
		out.Compares = append(out.Compares, &kv.Compare{
			Key: item.Key,
			Op: fromPBCompareOp(item.Op),
			Revision: item.Rev,
		})
	}

	for _, item := range req.ThenOps {
		if item == nil {
			continue
		}

		out.ThenOps = append(out.ThenOps, &kv.Op{
			Type: fromPBOpType(item.Type),
			Key: item.Key,
			Value: append([]byte(nil), item.Value...),
			LeaseID: item.LeaseId,
		})
	}

	for _, item := range req.ElseOps {
		if item == nil {
			continue
		}

		out.ElseOps = append(out.ElseOps, &kv.Op{
			Type: fromPBOpType(item.Type),
			Key: item.Key,
			Value: append([]byte(nil), item.Value...),
			LeaseID: item.LeaseId,
		})
	}

	return out
}

func toPBTxnResponse(resp *kv.TxnResponse) *pb.TxnResponse {
	if resp == nil {
		return &pb.TxnResponse{}
	}

	return &pb.TxnResponse{
		Succeeded: resp.Succeeded,
		Results: toPBKeyValues(resp.Results),
		Err: resp.Err,
	}
}

func fromPBLeaseGrantRequest(req *pb.LeaseGrantRequest) *kv.LeaseGrantRequest {
	if req == nil {
		return nil
	}

	return &kv.LeaseGrantRequest{
		TTL: req.Ttl,
		ClientID: req.ClientId,
		Seq: req.Seq,
	}
}
func toPBLeaseGrantResponse(resp *kv.LeaseGrantResponse) *pb.LeaseGrantResponse {
	if resp == nil {
		return &pb.LeaseGrantResponse{}
	}

	return &pb.LeaseGrantResponse{
		Id: resp.ID,
		Ttl: resp.TTL,
		Err: resp.Err,
	}
}

func fromPBLeaseRevokeRequest(req *pb.LeaseRevokeRequest) *kv.LeaseRevokeRequest {
	if req == nil {
		return nil
	}

	return &kv.LeaseRevokeRequest{
		ID: req.Id,
		ClientID: req.ClientId,
		Seq: req.Seq,
	}
}
func toPBLeaseRevokeResponse(resp *kv.LeaseRevokeResponse) *pb.LeaseRevokeResponse {
	if resp == nil {
		return &pb.LeaseRevokeResponse{}
	}

	return &pb.LeaseRevokeResponse{
		Err: resp.Err,
	}
}

func fromPBLeaseKeepAliveRequest(req *pb.LeaseKeepAliveRequest) *kv.LeaseKeepAliveRequest {
	if req == nil {
		return nil
	}

	return &kv.LeaseKeepAliveRequest{
		ID: req.Id,
		ClientID: req.ClientId,
		Seq: req.Seq,
	}
}
func toPBLeaseKeepAliveResponse(resp *kv.LeaseKeepAliveResponse) *pb.LeaseKeepAliveResponse {
	if resp == nil {
		return &pb.LeaseKeepAliveResponse{}
	}

	return &pb.LeaseKeepAliveResponse{
		Id: resp.ID,
		Ttl: resp.TTL,
		Err: resp.Err,
	}
}