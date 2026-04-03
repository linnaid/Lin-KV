package kvserver

import (
	kv "etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/command"
	"etcd-KV/internal/storage/mvcc"
)

func change(n int) int64 {
	return int64(n)
}

// channel wrapper
// 把 <-chan mvcc.Event 转换成 chan *kv.Event
func convert(ch <-chan mvcc.Event) chan *kv.Event {
	out := make(chan *kv.Event)

	go func() {
		for ev := range ch {

			var t kv.OpType
			switch ev.Type {
			case mvcc.EventDelete:
				t = kv.OpDelete
			case mvcc.EventPut:
				t = kv.OpPut
			default:
				t = kv.OpGet
			}

			e := &kv.Event{
				Key: ev.Key,
				Value: ev.Value,
				Type: t,
				Rev: ev.Rev.Main,
			}

			out <- e
		}
		close(out)
	}()

	return out
}

func match(ch *waitEntry, env *command.Command) bool {
	return ch.ClientID == env.ClientID() &&
		   ch.Seq == env.Seq() && 
		   ch.Kind == env.Kind
}

func toTxnCommand(req *kv.TxnRequest) *command.TxnCommand {
	if req == nil {
		return nil
	}

	out := &command.TxnCommand{
		ClientID: req.ClientID,
		Seq: req.Seq,

		Compares: make([]*command.TxnCompare, 0, len(req.Compares)),
		ThenOps: make([]*command.TxnOp, 0, len(req.ThenOps)),
		ElseOps: make([]*command.TxnOp, 0, len(req.ElseOps)),
	}

	for _, item := range req.Compares {
		if item == nil {
			continue
		}

		out.Compares = append(out.Compares, &command.TxnCompare{
			Key: item.Key,
			Op: command.TxnCompareOp(item.Op),
			Revision: item.Revision,
		})
	}

	for _, item := range req.ThenOps {
		if item == nil {
			continue
		}

		out.ThenOps = append(out.ThenOps, &command.TxnOp{
			Type: command.TxnOpType(item.Type),
			Key: item.Key,
			Value: append([]byte(nil), item.Value...),
			LeaseID: item.LeaseID,
		})
	}

	for _, item := range req.ElseOps {
		if item == nil {
			continue
		}

		out.ElseOps = append(out.ElseOps, &command.TxnOp{
			Type: command.TxnOpType(item.Type),
			Key: item.Key,
			Value: append([]byte(nil), item.Value...),
			LeaseID: item.LeaseID,
		})
	}

	return out
}

func cloneKeyValues(items []*kv.KeyValue) []*kv.KeyValue {
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

func toMVCCTxn(cmd *command.TxnCommand) mvcc.Txn {
	out := mvcc.Txn{
		Compares: make([]mvcc.Compare, 0, len(cmd.Compares)),
		ThenOps: make([]mvcc.Operation, 0, len(cmd.ThenOps)),
		ElseOps: make([]mvcc.Operation, 0, len(cmd.ElseOps)),
	}

	for _, item := range cmd.Compares {
		if item == nil {
			continue
		}

		out.Compares = append(out.Compares, mvcc.Compare{
			Key: item.Key,
			Op: mvcc.CompareOp(item.Op),
			Rev: item.Revision,
		})
	}

	for _, item := range cmd.ThenOps {
		if item == nil {
			continue
		}

		out.ThenOps = append(out.ThenOps, mvcc.Operation{
			Type: mvcc.OpType(item.Type),
			Key: item.Key,
			Value: append([]byte(nil), item.Value...),
			LeaseID: item.LeaseID,
		})
	}

	for _, item := range cmd.ElseOps {
		if item == nil {
			continue
		}

		out.ElseOps = append(out.ElseOps, mvcc.Operation{
			Type: mvcc.OpType(item.Type),
			Key: item.Key,
			Value: append([]byte(nil), item.Value...),
			LeaseID: item.LeaseID,
		})
	}

	return out
}

func fromMVCCKeyValues(items []mvcc.KeyValue) []*kv.KeyValue {
	if len(items) == 0 {
		return nil
	}

	out := make([]*kv.KeyValue, 0, len(items))
	for _, item := range items {
		out = append(out, &kv.KeyValue{
			Key: item.Key,
			Value: append([]byte(nil), item.Value...),
			Revision: item.Rev.Main,
		})
	}

	return out
}