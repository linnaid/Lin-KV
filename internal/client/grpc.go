package client

import (
	"context"
	"etcd-KV/internal/api/kv/pb"
	"fmt"

	"google.golang.org/grpc"
)


func (c *LabrpcClient) Call(method string, req interface{}, reply interface{}) error {
	ok := c.end.Call(method, req, reply)
	if !ok {
		return fmt.Errorf("rpc call failed: %s", method)
	}
	return nil
}

func (c *LabrpcClient) Stream(method string, req interface{}) (RPCStream, error) {
	return nil, fmt.Errorf("Stream not supported in labrpc")
}

func NewGrpcClient(conn *grpc.ClientConn) *GrpcClient {
	cli := pb.NewKVClient(conn)
	return &GrpcClient{
		cli: cli,
	}
}

// 只支持 Get 版本
func (c *GrpcClient) Call(method string, req interface{}, resp interface{}) error {
	switch method {

	case "RPCAdapter.Get":
		// 1. 类型断言
		r := req.(*pb.GetRequest)
		rep := resp.(*pb.GetResponse)

		// 2. 调用rpc
		res, err := c.cli.Get(context.Background(), r)
		if err != nil {
			return err
		}

		rep.Err = res.Err
		rep.Value = res.Value

		return nil

	case "RPCAdapter.Put":
		r := req.(*pb.PutRequest)
		rep := resp.(*pb.PutResponse)

		res, err := c.cli.Put(context.Background(), r)
		if err != nil {
			return err
		}

		rep.Err = res.Err
		
		return nil
		
	default:
		return fmt.Errorf("Unknown method %s", method)
	}
}
// 谁实现了KV proto的函数？

func (c *GrpcClient) Stream(method string, req interface{}) (RPCStream, error) {
	switch method {
		
	case "RPCAdapter.Watch":
		r := req.(*pb.WatchRequest)

		stream, err := c.cli.Watch(context.Background(), r)
		if err != nil {
			return nil, err
		}

		return &GrpcStream{
			stream: stream,
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

	r := resp.(*pb.WatchResponse)
	r.Err = res.Err
	r.Events = res.Events
	r.Revision = res.Revision

	return nil
}

func (s *GrpcStream) Close() error {
	return  nil
}