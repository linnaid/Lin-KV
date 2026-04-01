package client

import (
	"context"
	"errors"
	"etcd-KV/internal/api/kv/model"
)

// func (c *Client) Put(ctx context.Context, key string, value []byte) error {
// 	seq := c.getSeq()
// 	// Tools.Debug("?")
// 	retry := 0
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return ctx.Err()
// 		default:
// 		}

// 		// Tools.Debug("?", len(c.servers))
// 		for i := 0; i < len(c.servers); i++ {
// 			// Tools.Debug("?", len(c.servers))
// 			req := &kv.PutRequest{
// 				Key: key,
// 				Value: value,
// 				ClientID: c.clientID,
// 				Seq: seq,
// 			}
// 			reply := &kv.PutResponse{}

// 			srv := (c.leader + i) % len(c.servers)

// 			ok := c.callOnce(srv, "RPCAdapter.Put", req, reply)

// 			if !ok {
// 				Tools.Debug("client/command: Put RPC error", srv)

// 				c.updateLeader((srv + 1) % len(c.servers))
// 				continue
// 			}
// 			// if reply.Err != "" {
// 			// 	// Tools.Debug("rrr?")
// 			// 	Tools.Error("reply.Err", reply.Err)
// 			// 	continue
// 			// }
// 			err := parseErr(reply.Err)
// 			if err != nil {
// 				if err == ErrNotLeader {
// 					continue
// 				}
// 				// 其他错误后续返回给上层
// 				Tools.Error("Put error", err)
// 				return err
// 			}

// 			c.updateLeader(srv)
// 			return nil
// 		}

// 		retry++
// 		if retry > 3 {
// 			return ErrRPC
// 		}

// 		select {
// 		case <-time.After(20 * time.Millisecond):
// 		case <-ctx.Done():
// 			return ctx.Err()
// 		}
// 		// time.Sleep(20 * time.Millisecond)
// 	}
// }

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

		ok := c.callOnce(srv, "RPCAdapter.Put", req, reply)
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

		ok := c.callOnce(srv, "RPCAdapter.Get", req, reply)
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

// func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
// 	seq := c.getSeq()
// 	retry := 0

// 	for {
// 		select {
// 		case <- ctx.Done():
// 			return nil, ctx.Err()
// 		default:
// 		}

// 		for i := 0; i < len(c.servers); i++ {
// 			req := &kv.GetRequest{
// 				Key: key,
// 				ClientID: c.clientID,
// 				Seq: seq,
// 			}
// 			reply := &kv.GetResponse{}

// 			srv := (c.leader + i) % len(c.servers)

// 			ok := c.callOnce(srv, "RPCAdapter.Get", req, reply)

// 			if !ok {
// 				Tools.Debug("client/command: Get RPC error", srv)

// 				c.updateLeader((srv + 1) % len(c.servers))
// 				continue
// 			}
// 			// if reply.Err == ErrNotLeader.Error() {
// 			// 	continue
// 			// }
// 			// if reply.Err != "" {
// 			// 	Tools.Debug("111")
// 			// 	Tools.Error("reply.Err", reply.Err)
// 			// 	return nil
// 			// }
// 			err := parseErr(reply.Err)
// 			if err != nil {
// 				if err == ErrNotLeader {
// 					continue
// 				}
// 				// 其他错误后续返回给上层
// 				Tools.Error("Get error", err)
// 				return  nil, err
// 			}

// 			c.updateLeader(srv)
// 			return reply.Value, nil
// 		}

// 		retry++
// 		if retry > 3 {
// 			return nil, ErrRPC
// 		}

// 		select {
// 		case <-time.After(20 * time.Millisecond):
// 		case <-ctx.Done():
// 			return nil, ctx.Err()
// 		}
// 	}
// }

func parseErr(errStr string) error {
	if errStr == "" {
		return nil
	}

	if errStr == ErrNotLeader.Error() {
		return ErrNotLeader
	}

	return errors.New(errStr)
}
