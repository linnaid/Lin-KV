package client

import (
	"errors"
	"etcd-KV/internal/api/kv"
	"etcd-KV/internal/labrpc"
	"sync"
	"sync/atomic"
)

var globalClientID int64

type Client struct {
	mu sync.Mutex

	servers []*labrpc.ClientEnd
	leader int
	clientID int64
	seq int64
}

func Make(servers []*labrpc.ClientEnd) *Client {
	// Tools.Debug("servers", len(servers))
	c := &Client{}
	c.servers = servers
	clientID := atomic.AddInt64(&globalClientID, 1)
	c.clientID = clientID
	c.leader = 0
	c.seq = 0

	return c
}

func (c *Client) getSeq() int64{
	c.mu.Lock()
	defer c.mu.Unlock()

	seq := c.seq
	seq++
	c.seq = seq
	return seq
}

var ErrRPC = errors.New("RPC Failed")
var ErrNotLeader = errors.New("Is not Leader.")
var ErrTimeout = errors.New("Is TimeOut.")

func (c *Client) callPut(i int, req *kv.PutRequest) (*kv.PutResponse, error) {
	reply := &kv.PutResponse{}
	ok := c.servers[i].Call(
		"RPCAdapter.Put",
		req,
		reply,
	)

	if !ok {
		return nil, ErrRPC
	}

	if reply.Err != "" {
		return reply, errors.New(reply.Err)
	}

	return reply, nil
}