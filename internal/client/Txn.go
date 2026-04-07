package client

import (
	"context"
	"etcd-KV/internal/api/kv/model"
)

func (c *Client) BeginTxn() *Txn {
	return &Txn{
		client: c,
		thenOps: make([]*kv.Op, 0),
		elseOps: make([]*kv.Op, 0),
	}
}

func (t *Txn) Put(key string, value []byte) {
	t.thenOps = append(t.thenOps, &kv.Op{
		Type: kv.OpPut,
		Key: key,
		Value: value,
	})
}

func (t *Txn) Get(key string) {
	t.thenOps = append(t.thenOps, &kv.Op{
		Type: kv.OpGet,
		Key: key,
	})
}

func (t *Txn) Commit(ctx context.Context) (*kv.TxnResult, error) {
	seq := t.client.getSeq()

	req := &kv.TxnRequest{
		ThenOps: t.thenOps,
		ElseOps: t.elseOps,
		ClientID: t.client.clientID,
		Seq: seq,
	}

	var finalReply *kv.TxnResponse

	err := t.client.callWithRetry(ctx, func(srv int) error {
		reply := &kv.TxnResponse{}

		ok := t.client.callOnce(ctx, srv, "RPCAdapter.Txn", req, reply)
		if !ok {
			return ErrRPC
		}

		err := parseErr(reply.Err)
		if err != nil {
			return err
		}

		finalReply = reply
		return nil
		
	})

	if err != nil {
		return nil, err
	}

	if finalReply == nil {
		return nil, ErrRPC
	}
	
	return &kv.TxnResult{
		Succeeded: finalReply.Succeeded,
		Results: finalReply.Results,
	}, nil
}
