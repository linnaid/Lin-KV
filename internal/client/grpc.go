package client

import (
	"context"
	kv "etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/api/kv/pb"
	"fmt"

	"google.golang.org/grpc"
)


func NewGrpcClient(conn *grpc.ClientConn) *GrpcClient {
	cli := pb.NewKVClient(conn)
	return &GrpcClient{
		cli:  cli,
		conn: conn,
	}
}

func (c *GrpcClient) Call(ctx context.Context, method string, 
	req interface{}, resp interface{}) error {
	switch method {

	case "RPCAdapter.Get":
		r := toPBGetRequest(req.(*kv.GetRequest))
		rep := resp.(*kv.GetResponse)

		res, err := c.cli.Get(ctx, r)
		if err != nil {
			return err
		}

		fillGetResponseFromPB(rep, res)
		return nil

	case "RPCAdapter.Put":
		r := toPBPutRequest(req.(*kv.PutRequest))
		rep := resp.(*kv.PutResponse)

		res, err := c.cli.Put(ctx, r)
		if err != nil {
			return err
		}

		fillPutResponseFromPB(rep, res)
		return nil

	case "RPCAdapter.Delete":
		r := toPBDeleteRequest(req.(*kv.DeleteRequest))
		rep := resp.(*kv.DeleteResponse)

		res, err := c.cli.Delete(ctx, r)
		if err != nil {
			return err
		}

		fillDeleteResponseFromPB(rep, res)
		return nil

	default:
		return fmt.Errorf("Unknown method %s", method)
	}
}

// 谁实现了KV proto的函数？

func (c *GrpcClient) Stream(ctx context.Context, method string, 
	req interface{}) (RPCStream, error) {
	switch method {

	case "RPCAdapter.Watch":
		r := toPBWatchRequest(req.(*kv.WatchRequest))

		streamCtx, cancel := context.WithCancel(ctx)
		stream, err := c.cli.Watch(streamCtx, r)
		if err != nil {
			cancel()
			return nil, err
		}

		return &GrpcStream{
			stream: stream,
			cancel: cancel,
		}, nil

	default:
		return nil, fmt.Errorf("Unknown stream method %s", method)
	}
}

func (s *GrpcStream) Recv(resp interface{}) error {
	res, err := s.stream.Recv()
	if err != nil {
		return err
	}

	r := resp.(*kv.WatchResponse)
	fillWatchResponseFromPB(r, res)

	return nil
}

func (s *GrpcStream) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}
