package client

import (
	"etcd-KV/Tools"
	"etcd-KV/internal/api/kv"
	"time"
)

func (c *Client) Put(key string, value []byte) {
	seq := c.getSeq()
	// Tools.Debug("?")
	for {
		// Tools.Debug("?", len(c.servers))
		for i := 0; i < len(c.servers); i++ {
			Tools.Debug("?", len(c.servers))
			req := &kv.PutRequest {
				Key: []byte(key),
				Value: value,
				ClientID: c.clientID,
				Seq: seq,
			}
			reply := &kv.PutResponse{}

			srv := (c.leader + i) % len(c.servers)
			ok := c.servers[srv].Call(
				"RPCAdapter.Put",
				req,
				reply,
			)

			if !ok {
				Tools.Debug("client/command: Put RPC error", srv)
				c.mu.Lock()
				c.leader = (srv + 1) % len(c.servers)
				c.mu.Unlock()
				continue
			}
			if reply.Err != "" {
				Tools.Error("reply.Err", reply.Err)
				continue
			}
			c.mu.Lock()
			c.leader = srv
			c.mu.Unlock()
			return 
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func (c *Client) Get(key string) ([]byte) {
	seq := c.getSeq()

	for {
		for i := 0; i < len(c.servers); i++ {
			req := &kv.GetRequest{
				Key: []byte(key),
				ClientID: c.clientID,
				Seq: seq,
			}
			reply := &kv.GetResponse{}

			srv := (c.leader + i) % len(c.servers)
			ok := c.servers[srv].Call(
				"RPCAdapter.Get",
				req,
				reply,
			)

			if !ok {
				Tools.Debug("client/command: Get RPC error", srv)
				c.mu.Lock()
				c.leader = (srv + 1) % len(c.servers)
				c.mu.Unlock()
				continue
			}
			if reply.Err == ErrNotLeader.Error() {
				continue
			}
			if reply.Err != "" {
				Tools.Debug("111")
				Tools.Error("reply.Err", reply.Err)
				return nil
			}
			c.mu.Lock()
			c.leader = srv
			c.mu.Unlock()
			return reply.Value
		}

		time.Sleep(20 * time.Millisecond)
	}
}