package client

import (
	"context"
	"etcd-KV/Tools"
	"etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/api/kv/pb"
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
	ch := make(chan *kv.Event, 100)
	
	go func ()  {
		defer close(ch)

		streamCh := c.watchStream(ctx, key, false)

		select {
		case ev, ok := <-streamCh:
			if !ok {
				// 因为 streaming失败，所以fallback
				for ev := range c.watchInternal(ctx, key, false) {
					select {
					case ch <-ev:
					case <-ctx.Done():
						return 
					}
				}
				return
			}

			select {
			case ch <-ev:
			case <-ctx.Done():
				return 
			}

			for ev := range streamCh {
				select{
				case ch <-ev:
				case <-ctx.Done():
					return 
				}
			}

		case <-ctx.Done():
			return
		}
	}()

	return ch
}

func (c *Client) WatchPrefix(ctx context.Context, prefix string) <-chan *kv.Event {
	return c.watchInternal(ctx, prefix, true)
}

// 后面要改成leader routing
func (c *Client) watchStream(ctx context.Context, key string, prefix bool) <-chan *kv.Event {
	ch :=make(chan *kv.Event, 100)

	go func ()  {
		defer close(ch)

		var rev int64 = 0

		for {
			req := &pb.WatchRequest{
				Key: key,
				Prefix: prefix,
				ClientId: c.clientID,
				Revision: rev,
			}

			// 节点不只一个，目前假定为一个节点
			deleay := 100 * time.Millisecond

			stream, err := c.servers[0].Stream("RPCAdapter.Watch", req)
			if err != nil {
				Tools.Debug("RPCAdapter.Watch err not nil in watchStream", err.Error())

				select {
				case <-time.After(deleay):
					if deleay < time.Second {
						deleay *= 2
					}
					continue
				case <-ctx.Done():
					return 
				}
			}
			deleay = 100 * time.Millisecond

			// defer stream.Close()

			for {
				select {
				case <-ctx.Done():
					stream.Close()
					return
				default:
				}

				var resp pb.WatchResponse

				err := stream.Recv(&resp)
				if err != nil {
					if ctx.Err() != nil {
						Tools.Debug("ctx.Err != nil in watchStream", ctx.Err().Error())
						return
					}

					Tools.Debug("stream.recv err not nil in watchStream", err)
					stream.Close()
					break
				}

				rev = resp.Revision
			
				for _, e := range resp.Events {

					var t kv.OpType

					switch e.Type {
					case "PUT":
						t = kv.OpPut
					case "DELETE":
						t = kv.OpDelete
					default:
						continue
					}

					ev := &kv.Event{
						Type: t,
						Key: e.Key,
						Value: e.Value,
					}

					select {
					case ch <-ev:
					case <-ctx.Done():
						return
					}
				}
			}	
		}	
	}()

	return ch
}