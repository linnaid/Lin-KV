package client

import (
	"context"
	"errors"
	"etcd-KV/internal/api/kv/model"
)


func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	seq := c.getSeq()

	return c.callWithRetry(ctx, func(srv int) error {
		req := &kv.PutRequest{
			Key:      key,
			Value:    value,
			ClientID: c.clientID,
			Seq:      seq,
		}

		reply := &kv.PutResponse{}

		ok := c.callOnce(ctx, srv, "RPCAdapter.Put", req, reply)
		if !ok {
			return ErrRPC
		}

		return parseErr(reply.Err)
	})
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	var result []byte
	seq := c.getSeq()

	err := c.callWithRetry(ctx, func(srv int) error {
		req := &kv.GetRequest{
			Key:      key,
			ClientID: c.clientID,
			Seq:      seq,
		}

		reply := &kv.GetResponse{}

		ok := c.callOnce(ctx, srv, "RPCAdapter.Get", req, reply)
		if !ok {
			return ErrRPC
		}

		err := parseErr(reply.Err)
		if err != nil {
			return err
		}

		result = reply.Value
		return nil
	})

	return result, err
}

func parseErr(errStr string) error {
	if errStr == "" {
		return nil
	}

	if errStr == ErrNotLeader.Error() {
		return ErrNotLeader
	}

	return errors.New(errStr)
}

func (c *Client) RangePrefix(ctx context.Context, prefix string) ([]*kv.KeyValue, int64, error) {
	return c.RangePrefixAtRevision(ctx, prefix, 0)
}

func (c *Client) RangePrefixAtRevision(ctx context.Context, prefix string, rev int64) ([]*kv.KeyValue, int64, error) {
	seq := c.getSeq()
	var finalReply *kv.RangeResponse

	err := c.callWithRetry(ctx, func(srv int) error {
		req := &kv.RangeRequest{
			Key: prefix,
			Prefix: true,
			Revision: rev,
			ClientID: c.clientID,
			Seq: seq,
		}

		reply := &kv.RangeResponse{}

		ok := c.callOnce(ctx, srv, "RPCAdapter.Range", req, reply)
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
		return nil, 0, err
	}

	if finalReply == nil {
		return nil, 0, ErrRPC
	}

	return finalReply.KVs, finalReply.Revision, nil
}