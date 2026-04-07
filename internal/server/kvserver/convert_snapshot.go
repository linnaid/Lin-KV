package kvserver

import (
	"errors"
	kv "etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/command"
	"etcd-KV/internal/pb/kvserverpb"
	"etcd-KV/internal/storage/mvcc"
)


func cloneSeqMap(src map[int64]int64) map[int64]int64 {
	out := make(map[int64]int64, len(src))
	for k, v := range src {
		out[k] = v
	}

	return out
}

func cloneRevision(rev *mvcc.Revision) *mvcc.Revision {
	if rev == nil {
		return nil
	}
	out := *rev
	return &out 
}

func cloneResult(result Result) Result {
	out := Result{
		Kind: result.Kind,
		ClientID: result.ClientID,
		Seq: result.Seq,
		Rev: result.Rev,
		Value: result.Value,
		Found: result.Found,
		TxnSucceeded: result.TxnSucceeded,
		TxnResults: result.TxnResults,
		LeaseID: result.LeaseID,
		LeaseTTL: result.LeaseTTL,
	}

	if result.Err != nil {
		out.Err = errors.New(result.Err.Error())
	}

	return out
}

func cloneResultMap(src map[int64]Result) map[int64]Result {
	out := make(map[int64]Result, len(src))
	for clientID, result := range src {
		out[clientID] = cloneResult(result)
	}
	return out
}

func toPBServerSnapshot(snap ServerSnapshot) *kvserverpb.ServerSnapshot {
	out := &kvserverpb.ServerSnapshot{
		KvSnapshot: append([]byte(nil), snap.KVSnapshot...),
		ClientLastSeq: make(map[int64]int64, len(snap.ClientLastSeq)),
		ClientLastResult: make(map[int64]*kvserverpb.Result, len(snap.ClientLastResult)),
	}

	for clientID, seq := range snap.ClientLastSeq {
		out.ClientLastSeq[clientID] = seq
	}

	for clientID, result := range snap.ClientLastResult {
		out.ClientLastResult[clientID] = toPBResult(result)
	}

	return out
}

func fromPBServerSnapshot(in *kvserverpb.ServerSnapshot) ServerSnapshot {
	if in == nil {
		return ServerSnapshot{
			ClientLastSeq: map[int64]int64{},
			ClientLastResult: map[int64]Result{},
		}
	}

	out := ServerSnapshot{
		KVSnapshot: append([]byte(nil), in.KvSnapshot...),
		ClientLastSeq: make(map[int64]int64, len(in.ClientLastSeq)),
		ClientLastResult: make(map[int64]Result, len(in.ClientLastResult)),
	}

	for clientID, seq := range in.ClientLastSeq {
		out.ClientLastSeq[clientID] = seq
	}

	for clientID, result := range in.ClientLastResult {
		out.ClientLastResult[clientID] = fromPBResult(result)
	}

	return out
}

func toPBResult(result Result) *kvserverpb.Result {
	out := &kvserverpb.Result{
		Kind: kvserverpb.Kind(result.Kind),
		ClientId: result.ClientID,
		Seq: result.Seq,
		Value: append([]byte(nil), result.Value...),
		Found: result.Found,
		TxnSucceeded: result.TxnSucceeded,
		TxnResults: snapshotToPBKeyValues(result.TxnResults),
		LeaseId: result.LeaseID,
		LeaseTtl: result.LeaseTTL,
	}

	if result.Err != nil {
		out.Err = result.Err.Error()
	}

	if result.Rev != nil {
		out.Rev = toPBRevision(result.Rev)
	}

	return out
}

func fromPBResult(in *kvserverpb.Result) Result {
	if in == nil {
		return Result{}
	}

	out := Result{
		Kind: command.Kind(in.Kind),
		ClientID: in.ClientId,
		Seq: in.Seq,
		Rev: fromPBRevision(in.Rev),
		Value: append([]byte(nil), in.Value...),
		Found: in.Found,
		TxnSucceeded: in.TxnSucceeded,
		TxnResults: snapshotFromPBKeyValues(in.TxnResults),
		LeaseID: in.LeaseId,
		LeaseTTL: in.LeaseTtl,
	}

	if in.Err != "" {
		out.Err = errors.New(in.Err)
	}

	return out
}

func toPBRevision(rev *mvcc.Revision) *kvserverpb.Revision {
	if rev == nil {
		return nil
	}
	return &kvserverpb.Revision{
		Main: rev.Main,
		Sub: rev.Sub,
	}
}

func fromPBRevision(in *kvserverpb.Revision) *mvcc.Revision {
	if in == nil {
		return nil
	}
	return &mvcc.Revision{
		Main: in.Main,
		Sub: in.Sub,
	}
}

func snapshotToPBKeyValues(items []*kv.KeyValue) []*kvserverpb.KeyValue {
	if len(items) == 0 {
		return nil
	}

	out := make([]*kvserverpb.KeyValue, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		out = append(out, &kvserverpb.KeyValue{
			Key: item.Key,
			Value: append([]byte(nil), item.Value...),
			Revision: item.Revision,
		})
	}

	return out
}

func snapshotFromPBKeyValues(items []*kvserverpb.KeyValue) []*kv.KeyValue {
	if len(items) == 0 {
		return nil
	}

	out := make([]*kv.KeyValue, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		out = append(out, &kv.KeyValue{
			Key: item.Key,
			Value: append([]byte(nil), item.Value...),
			Revision: item.Revision,
		})
	}

	return out
}