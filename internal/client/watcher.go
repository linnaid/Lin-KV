package client

import (
	"context"
	"etcd-KV/Tools"
	"etcd-KV/internal/api/kv/model"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// /////////////////////////////////////////////////
func (c *Client) Watch(ctx context.Context, key string) <-chan *kv.Event {
	return  c.watchStream(ctx, key, false, 1)
}

func (c *Client) WatchPrefix(ctx context.Context, prefix string) <-chan *kv.Event {
	return c.watchStream(ctx, prefix, true, 1)
}

func (c *Client) WatchFrom(ctx context.Context, key string, fromRev int64) <-chan *kv.Event {
	return c.watchStream(ctx, key, false, fromRev)
}

// 后面要改成leader routing
func (c *Client) watchStream(ctx context.Context, key string, prefix bool, startRev int64) <-chan *kv.Event {
	ch := make(chan *kv.Event, 100)
	rev := startRev
	if rev <= 0 {
		rev = 1
	}

	go func() {
		defer close(ch)

		// var rev int64 = 0
		backoff := 100 * time.Millisecond

		for {

			select {
			case <-ctx.Done():
				return 
			default:
			}

			req := &kv.WatchRequest{
				Key:      key,
				Prefix:   prefix,
				ClientID: c.clientID,
				Revision: rev,
				Seq: 	  c.getSeq(),
			}

			stream, err := c.openWatchStream(ctx, req)
			if err != nil {
				if ctx.Err() != nil {
					return 
				}
				if isWatchCompacted(err) {
					Tools.Error("watch revision compacted", err)

					return 
				}
				Tools.Debug("RPCAdapter.Watch err not nil in watchStream", err.Error())

				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return
				}
				if backoff < time.Second {
					backoff *= 2
				}
				continue
			}
			backoff = 100 * time.Millisecond

			// defer stream.Close()

			for {
				// select {
				// case <-ctx.Done():
				// 	stream.Close()
				// 	return
				// default:
				// }

				var resp kv.WatchResponse

				err := stream.Recv(&resp)
				if err != nil {
					if ctx.Err() != nil {
						Tools.Debug("ctx.Err != nil in watchStream", ctx.Err().Error())
						return
					}
					if isWatchCompacted(err) {
						Tools.Error("watch revision compacted in watchStream", err)
						return 
					}

					Tools.Debug("stream.recv err not nil in watchStream", err)
					break
				}

				if resp.Err != "" {
					_ = stream.Close()
					Tools.Debug("watch response err not empty in watchStream", resp.Err)

					break
				}

				if resp.Revision >= rev {
					rev = resp.Revision + 1
				}

				for _, ev := range resp.Events {
					if ev != nil && ev.Rev >= rev {
						rev = ev.Rev + 1
					}

					select {
					case ch <- ev:
					case <-ctx.Done():
						_ = stream.Close()
						return
					}
				}
			}
		}
	}()

	return ch
}

func (c * Client) openWatchStream(ctx context.Context, 
	req *kv.WatchRequest) (RPCStream, error) {
		c.mu.Lock()
		leader := c.leader
		c.mu.Unlock()

		var lastErr error
		for i := 0; i < len(c.servers); i++ {
			srv := (leader + i) % len(c.servers)

			stream, err := c.servers[srv].Stream(ctx, "RPCAdapter.Watch", req)
			if err == nil {
				c.updateLeader(srv)
				return stream, nil
			}
			lastErr = err
		}

		if lastErr == nil {
			lastErr = ErrRPC
			Tools.Debug("openWatchStream error", "lastErr is nil, set to ErrRPC")
		}
		return nil, lastErr
	}

func isWatchCompacted(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}