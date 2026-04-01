package client

import (
	"context"
	"etcd-KV/internal/api/kv/model"
)

func (c *Client) BeginTxn() *Txn {
	return &Txn{
		client: c,
		ops: make([]*kv.Op, 0),
	}
}

func (t *Txn) Put(key string, value []byte) {
	t.ops = append(t.ops, &kv.Op{
		Type: kv.OpPut,
		Key: key,
		Value: value,
	})
}

func (t *Txn) Get(key string) {
	t.ops = append(t.ops, &kv.Op{
		Type: kv.OpGet,
		Key: key,
	})
}

func (t *Txn) Commit(ctx context.Context) (*kv.TxnResult, error) {
	seq := t.client.getSeq()

	req := &kv.TxnRequest{
		Ops: t.ops,
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
		Results: finalReply.Results,
	}, nil
}
