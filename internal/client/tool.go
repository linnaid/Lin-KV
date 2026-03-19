package client

import "etcd-KV/internal/labrpc"

func MakeClientFromLabrpc(servers []*labrpc.ClientEnd) *Client {
	clients := make([]RPCClient, len(servers))

	for i, s := range servers {
		clients[i] = &LabrpcClient{end: s}
	}

	return  Make(clients)
}