package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"etcd-KV/Tools"
	"etcd-KV/internal/client"
	"etcd-KV/internal/labrpc"
	"etcd-KV/internal/pb/raftpb"
	"etcd-KV/internal/raft"
	"etcd-KV/internal/server/kvserver"
	"etcd-KV/internal/storage/mvcc"
	"etcd-KV/internal/storage/persister"
	grpctransport "etcd-KV/internal/transport/grpc"
	labrpctransport "etcd-KV/internal/transport/labrpc"
	kvpb "etcd-KV/internal/api/kv/pb"

	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	const (
		useGRPCPeers = true
		useGRPCKV 	 = true
	)

	peerAddrs := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}

	clientAddrs := []string{
		"127.0.0.1:9001",
		"127.0.0.1:9002",
		"127.0.0.1:9003",
	}

	n := len(peerAddrs)
	network := labrpc.MakeNetwork()

	// 初始化数组
	peers := make([][]raft.Peer, n)
	servers := make([]*labrpc.Server, n)
	rafts := make([]*raft.Raft, n)
	clientEnds := make([][]*labrpc.ClientEnd, n)
	applyChs := make([]chan raft.ApplyMsg, n)
	stores := make([]*mvcc.KVStore, n)
	KVservers := make([]*kvserver.Server, n)

	for i := 0; i < n; i++ {
		servers[i] = labrpc.MakeServer()
		applyChs[i] = make(chan raft.ApplyMsg, 1000) // buffer 避免阻塞
	}

	for i := 0; i < n; i++ {
		ps := persister.MakePersister()
		rafts[i] = raft.Make(peers[i], i, ps, applyChs[i])
		if !useGRPCPeers {
			servers[i].AddService(labrpc.MakeService(rafts[i]))
		}
	}

	if useGRPCPeers {
		peerListeners := make([]net.Listener, n)
		peerServers := make([]*gogrpc.Server, n)
		peerConns := make([][]*gogrpc.ClientConn, n)

		for i := 0; i < n; i++ {
			lis, err := net.Listen("tcp", peerAddrs[i])
			if err != nil {
				panic(err)
			}

			srv := gogrpc.NewServer()
			raftpb.RegisterRaftServer(srv, grpctransport.NewRaftServer(rafts[i]))

			peerListeners[i] = lis
			peerServers[i] = srv

			go func(s *gogrpc.Server, l net.Listener) {
				if err := s.Serve(l); err != nil {
					Tools.Error("raft grpc server stopped", err)
				}
			}(srv, lis)
		}

		for i := range peers {
			peers[i] = make([]raft.Peer, n)
			peerConns[i] = make([]*gogrpc.ClientConn, n)
			for j := 0; j < n; j++ {
				conn, err := gogrpc.Dial(
					peerAddrs[j],
					gogrpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				if err != nil {
					panic(err)
				}
				peerConns[i][j] = conn
				peers[i][j] = grpctransport.NewGrpcPeer(conn)
			}
		}

		defer func() {
			for i := range peerConns {
				for j := range peerConns[i] {
					if peerConns[i][j] != nil {
						_ = peerConns[i][j].Close()
					}
				}
			}
		}()

		defer func() {
			for i := range peerServers {
				if peerServers[i] != nil {
					peerServers[i].Stop()
				}
				if peerListeners[i] != nil {
					_ = peerListeners[i].Close()
				}
			}
		}()

	} else {
		peerEnds := make([][]*labrpc.ClientEnd, n)
		for i := range peerEnds {
			peerEnds[i] = make([]*labrpc.ClientEnd, n)
			peers[i] = make([]raft.Peer, n)
			for j := 0; j < n; j++ {
				end := network.MakeEnd(fmt.Sprintf("peer-%d-%d", i, j))
				peerEnds[i][j] = end
				peers[i][j] = labrpctransport.NewPeer(end)
			}
		}

		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				network.Connect(
					fmt.Sprintf("peer-%d-%d", i, j),
					fmt.Sprintf("server-%d", j),
				)
				network.Enable(fmt.Sprintf("peer-%d-%d", i, j), true)
			}
		}
	}

	for i := 0; i < n; i++ {
		stores[i] = mvcc.NewKVStore()
		core := kvserver.NewServer(i, rafts[i], stores[i], applyChs[i])
		KVservers[i] = core

		if !useGRPCKV || !useGRPCPeers {
			network.AddServer(fmt.Sprintf("server-%d", i), servers[i])
		}

		if !useGRPCKV {
			adapter := grpctransport.NewRPCAdapter(core)
			servers[i].AddService(labrpc.MakeService(adapter))
		}
	}

	if useGRPCKV {
		kvListeners := make([]net.Listener, n)
		kvRPCServers := make([]*gogrpc.Server, n)

		for i := 0; i < n; i++ {
			lis, err := net.Listen("tcp", clientAddrs[i])
			if err != nil {
				panic(err)
			}

			srv := gogrpc.NewServer()
			kvpb.RegisterKVServer(srv, kvserver.NewRPCAdapter(KVservers[i]))
			
			kvListeners[i] = lis
			kvRPCServers[i] = srv

			go func(s *gogrpc.Server, l net.Listener) {
				if err := s.Serve(l); err != nil {
					Tools.Error("kv grpc server stopped", err)
				}
			}(srv, lis)
 		}

		defer func() {
			for i := range kvRPCServers {
				if kvRPCServers[i] != nil {
					kvRPCServers[i].Stop()
				}
				if kvListeners[i] != nil {
					_ = kvListeners[i].Close()
				}
			}
		}()
	}

	if !useGRPCKV {
		for i := 0; i < n; i++ {
			clientEnds[i] = make([]*labrpc.ClientEnd, n)
			for j := 0; j < n; j++ {
				name := fmt.Sprintf("client-%d-%d", i, j)
				clientEnds[i][j] = network.MakeEnd(name)
				network.Connect(name, fmt.Sprintf("server-%d", j))
				network.Enable(name, true)
			}
		}
	}

	// 等待 leader 选举完成
	leader := findLeader(rafts, n)
	Tools.Debug(fmt.Sprintf("Leader Success, leader = %d", leader))

	var ck *client.Client

	if useGRPCKV {
		kvConns := make([]*gogrpc.ClientConn, n)
		rpcClients := make([]client.RPCClient, n)

		for i := 0; i < n; i++ {
			conn, err := gogrpc.Dial(
				clientAddrs[i],
				gogrpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				panic(err)
			}
			kvConns[i] = conn
			rpcClients[i] = client.NewGrpcClient(conn)
		}

		defer func() {
			for i := range kvConns {
				if kvConns[i] != nil {
					_ = kvConns[i].Close()
				}
			}
		}()

		ck = client.Make(rpcClients)
	} else {
		ck = client.MakeClientFromLabrpc(clientEnds[leader])
	}
	// 压测函数
	runStressTest := func(numClients, numOps int) {
		doneCh := make(chan struct{})
		for c := 0; c < numClients; c++ {
			go func(cid int) {

				for i := 0; i < numOps; i++ {
					key := fmt.Sprintf("k-%d", i)
					val := []byte(fmt.Sprintf("v-%d-%d", i, cid))
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer cancel()
					err := ck.Put(ctx, key, val)
					if err != nil {
						Tools.Error("压测函数的Put error", err)
					}
					v, err := ck.Get(ctx, key)
					if err != nil {
						Tools.Error("压测函数的Get error", err)
					}
					Tools.RIGHT("Get = ", v)
				}
				doneCh <- struct{}{}
			}(c)
		}
		for i := 0; i < numClients; i++ {
			<-doneCh
		}
		Tools.Debug("Stress test finished")
	}

	// 运行压测：20 个客户端，每个客户端 50 次操作
	runStressTest(20, 50)
}

func findLeader(rafts []*raft.Raft, n int) int {
	for {
		for i := 0; i < n; i++ {
			if id, isLeader := rafts[i].GetState(); isLeader {
				return id
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}
