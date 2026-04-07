package command

import (
	commandpb "etcd-KV/internal/pb/command"
)

func toPBCommand(cmd *Command) *commandpb.Command {
	if cmd == nil {
		return nil
	}

	out := &commandpb.Command{
		Kind: commandpb.Kind(cmd.Kind),
	}

	switch cmd.Kind {
	case KindKV:
		out.Body = &commandpb.Command_Kv{
			Kv: toPBKVCommand(cmd.KV),
		}
	case KindTxn:
		out.Body = &commandpb.Command_Txn{
			Txn: toPBTxnCommand(cmd.Txn),
		}
	case KindLeaseGrant:
		out.Body = &commandpb.Command_LeaseGrant{
			LeaseGrant: toPBLeaseGrantCommand(cmd.LeaseGrant),
		}
	case KindLeaseRevoke:
		out.Body = &commandpb.Command_LeaseRevoke{
			LeaseRevoke: toPBLeaseRevokeCommand(cmd.LeaseRevoke),
		}
	case KindLeaseKeepAlive:
		out.Body = &commandpb.Command_LeaseKeepalive{
			LeaseKeepalive: toPBLeaseKeepAliveCommand(cmd.LeaseKeepAlive),
		}
	}

	return out
}

func fromPBCommand(in *commandpb.Command) *Command {
	if in == nil {
		return nil
	}

	out := &Command{
		Kind: Kind(in.Kind),
	}

	switch body := in.Body.(type) {
	case *commandpb.Command_Kv:
		out.KV = fromPBKVCommand(body.Kv)
	case *commandpb.Command_Txn:
		out.Txn = fromPBTxnCommand(body.Txn)
	case *commandpb.Command_LeaseGrant:
		out.LeaseGrant = fromPBLeaseGrantCommand(body.LeaseGrant)
	case *commandpb.Command_LeaseRevoke:
		out.LeaseRevoke = fromPBLeaseRevokeCommand(body.LeaseRevoke)
	case *commandpb.Command_LeaseKeepalive:
		out.LeaseKeepAlive = fromPBLeaseKeepAliveCommand(body.LeaseKeepalive)
	}

	return out
}

func toPBKVCommand(cmd *KVCommand) *commandpb.KVCommand {
	if cmd == nil {
		return nil
	}

	return &commandpb.KVCommand{
		Type: commandpb.KVType(cmd.Type),
		Key: cmd.Key,
		Value: append([]byte(nil), cmd.Value...),
		ClientId: cmd.ClientID,
		Seq: cmd.Seq,
		Rev: cmd.Rev,
		LeaseId: cmd.LeaseID,
	}
}

func fromPBKVCommand(in *commandpb.KVCommand) *KVCommand {
	if in == nil {
		return nil
	}

	return &KVCommand{
		Type: Type(in.Type),
		Key: in.Key,
		Value: append([]byte(nil), in.Value...),
		ClientID: in.ClientId,
		Seq: in.Seq, 
		Rev: in.Rev,
		LeaseID: in.LeaseId,
	}
}

func toPBTxnCommand(cmd *TxnCommand) *commandpb.TxnCommand {
	if cmd == nil {
		return nil
	}

	out := &commandpb.TxnCommand{
		Compares: make([]*commandpb.TxnCompare, 0, len(cmd.Compares)),
		ThenOps: make([]*commandpb.TxnOp, 0, len(cmd.ThenOps)),
		ElseOps: make([]*commandpb.TxnOp, 0, len(cmd.ElseOps)),
		ClientId: cmd.ClientID,
		Seq: cmd.Seq,
	}

	for _, item := range cmd.Compares {
		if item == nil {
			continue
		}

		out.Compares = append(out.Compares, &commandpb.TxnCompare{
			Key: item.Key,
			Op: commandpb.TxnCompareOp(item.Op),
			Revision: item.Revision,
		})
	}

	for _, item := range cmd.ThenOps {
		if item == nil {
			continue
		}

		out.ThenOps = append(out.ThenOps, &commandpb.TxnOp{
			Type: commandpb.TxnOpType(item.Type),
			Key: item.Key,
			Value: append([]byte(nil), item.Value...),
			LeaseId: item.LeaseID,
		})
	}

	for _, item := range cmd.ElseOps {
		if item == nil {
			continue
		}

		out.ElseOps = append(out.ElseOps, &commandpb.TxnOp{
			Type: commandpb.TxnOpType(item.Type),
			Key: item.Key,
			Value: append([]byte(nil), item.Value...),
			LeaseId: item.LeaseID,
		})
	}

	return out
}

func fromPBTxnCommand(in *commandpb.TxnCommand) *TxnCommand {
	if in == nil {
		return nil
	}

	out := &TxnCommand{
		Compares: make([]*TxnCompare, 0, len(in.Compares)),
		ThenOps:  make([]*TxnOp, 0, len(in.ThenOps)),
		ElseOps:  make([]*TxnOp, 0, len(in.ElseOps)),
		ClientID: in.ClientId,
		Seq:      in.Seq,
	}

	for _, item := range in.Compares {
		if item == nil {
			continue
		}
		out.Compares = append(out.Compares, &TxnCompare{
			Key:      item.Key,
			Op:       TxnCompareOp(item.Op),
			Revision: item.Revision,
		})
	}

	for _, item := range in.ThenOps {
		if item == nil {
			continue
		}
		out.ThenOps = append(out.ThenOps, &TxnOp{
			Type:    TxnOpType(item.Type),
			Key:     item.Key,
			Value:   append([]byte(nil), item.Value...),
			LeaseID: item.LeaseId,
		})
	}

	for _, item := range in.ElseOps {
		if item == nil {
			continue
		}
		out.ElseOps = append(out.ElseOps, &TxnOp{
			Type:    TxnOpType(item.Type),
			Key:     item.Key,
			Value:   append([]byte(nil), item.Value...),
			LeaseID: item.LeaseId,
		})
	}

	return out
}

func toPBLeaseGrantCommand(cmd *LeaseGrantCommand) *commandpb.LeaseGrantCommand {
	if cmd == nil {
		return nil
	}

	return &commandpb.LeaseGrantCommand{
		Ttl: cmd.TTL,
		ClientId: cmd.ClientID,
		Seq: cmd.Seq,
	}
}

func fromPBLeaseGrantCommand(in *commandpb.LeaseGrantCommand) *LeaseGrantCommand {
	if in == nil {
		return nil
	}

	return &LeaseGrantCommand{
		TTL: in.Ttl,
		ClientID: in.ClientId,
		Seq: in.Seq,
	}
}

func toPBLeaseRevokeCommand(cmd *LeaseRevokeCommand) *commandpb.LeaseRevokeCommand {
	if cmd == nil {
		return nil
	}

	return &commandpb.LeaseRevokeCommand{
		Id: cmd.ID,
		ClientId: cmd.ClientID,
		Seq: cmd.Seq,
	}
}

func fromPBLeaseRevokeCommand(in *commandpb.LeaseRevokeCommand) *LeaseRevokeCommand {
	if in == nil {
		return nil
	}

	return &LeaseRevokeCommand{
		ID: in.Id,
		ClientID: in.ClientId,
		Seq: in.Seq,
	}
}

func toPBLeaseKeepAliveCommand(cmd *LeaseKeepAliveCommand) *commandpb.LeaseKeepAliveCommand {
	if cmd == nil {
		return nil
	}

	return &commandpb.LeaseKeepAliveCommand{
		Id: cmd.ID,
		ClientId: cmd.ClientID,
		Seq: cmd.Seq,
	}
}

func fromPBLeaseKeepAliveCommand(in *commandpb.LeaseKeepAliveCommand) *LeaseKeepAliveCommand {
	if in == nil {
		return nil
	}

	return &LeaseKeepAliveCommand{
		ID: in.Id,
		ClientID: in.ClientId,
		Seq: in.Seq,
	}
}