package client

import (
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

func NewGrpcClient(conn *grpc.ClientConn)