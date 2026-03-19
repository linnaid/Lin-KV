package client

import (
	"context"
	"etcd-KV/Tools"
	"etcd-KV/internal/api/kv/model"
	"time"
)

func (c *Client) watchInternal(ctx context.Context, key string, prefix bool) <-chan *kv.Event {
	ch := make(chan *kv.Event, 100)

	go func() {
		defer close(ch)

		var rev int64 = 0

		for {
			select {
			case <-ctx.Done():
				return 
			default:
			}

			seq := c.getSeq()

			var newRev int64
			var events []*kv.Event

			err := c.callWithRetry(ctx, func(srv int) error {
				req := &kv.WatchRequest{
					Key: key,
					Revision: rev,
					ClientID: c.clientID,
					Prefix: prefix,
					Seq: seq,
				}

				reply := &kv.WatchResponse{}

				ok := c.callOnce(srv, "RPCAdapter.Watch", req, reply)
				if !ok {
					return ErrRPC
				}

				err := parseErr(reply.Err)
				if err != nil {
					return err
				}

				events = reply.Events
				newRev = reply.Revision

				return nil
			})

			if err != nil {
				
				// if err == context.Canceled || err == context.DeadlineExceeded {
				// 	Tools.Error("err == context.Canceled || DeadlineExceded.", err)
				// 	return 
				// }
				if ctx.Err() != nil {
					Tools.Error("err == context.Canceled || DeadlineExceded.", err)
					return
				}

				Tools.Debug("Watch callWithRetry is not nil", err.Error())
				continue
			}

			rev = newRev

			for _, ev := range events {
				select {
				case ch <-ev:
				case <-ctx.Done():
					return 
				}
			}

			delay := 50 * time.Millisecond

			if len(events) == 0 {
				delay = 100 * time.Millisecond
			}
			select {
			case <-time.After(delay):
			case <- ctx.Done():
				return
			}
		
		}
	}()
	return ch 
}

///////////////////////////////////////////////////
func (c *Client) Watch(ctx context.Context, key string) <-chan *kv.Event {
	return c.watchInternal(ctx, key, false)
}

func (c *Client) WatchPrefix(ctx context.Context, prefix string) <-chan *kv.Event {
	return c.watchInternal(ctx, prefix, true)
}