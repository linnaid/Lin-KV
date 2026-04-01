package client

import (
	"context"
	"etcd-KV/Tools"
	"etcd-KV/internal/raft"
	"time"
)

func (c *Client) callRPC(ctx context.Context, method string, req interface{}, reply interface{}) error {
	numServers := len(c.servers)

	for {
		c.mu.Lock()
		leader := c.leader
		c.mu.Unlock()

		for i := 0; i < numServers; i++ {
			srv := (leader + i) % numServers

			err := c.servers[srv].Call(ctx, method, req, reply)
			if err != nil {
				c.updateLeader((srv + 1) % numServers)
				continue
			}
			// ok := c.servers[srv].Call(method, req, reply)
			// if !ok {
			// 	c.updateLeader((srv + 1) % numServers)
			// 	continue
			// }

			if errStr := raft.GetReplyError(reply); errStr != nil {
				if errStr == ErrNotLeader {
					continue
				}
				return errStr
			}

			c.updateLeader(srv)
			return nil
		}

		time.Sleep(10 * time.Millisecond)
		// return ErrRPC
	}
	
}

func (c *Client) updateLeader(leader int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.leader = leader
}

func (c *Client) callOnce(ctx context.Context, srv int, method string, req interface{}, reply interface{}) bool {
	// return c.servers[srv].Call(method, req, reply)
	err := c.servers[srv].Call(ctx, method, req, reply)
	if err != nil {
		Tools.Debug("callOnce err is: ", err)
		return false
	}

	return true
}

func (c *Client) callWithRetry(ctx context.Context, f rpcFunc) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		for i := 0; i < len(c.servers); i++ {
			srv := (c.leader + i) % len(c.servers)

			err := f(srv)

			if err == nil {
				c.updateLeader(srv)
				return  nil
			}

			if err == ErrNotLeader {
				continue
			}

			c.updateLeader((srv+1) % len(c.servers))
		}

		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}