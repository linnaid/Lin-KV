// 主函数
package main

import (
	"time"

	"etcd-KV/Tools"
	"etcd-KV/internal/client"
	"etcd-KV/internal/labrpc"
	"etcd-KV/internal/raft"
	"etcd-KV/internal/server/kvserver"
	"etcd-KV/internal/storage/mvcc"
	"etcd-KV/internal/storage/persister"
	"etcd-KV/internal/transport/grpc"
	"fmt"
)

func main() {
	// 1. 解析配置
	peerAddrs := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}
	// Tools.Info("len", len(peerAddrs))
	n := len(peerAddrs)
	network := labrpc.MakeNetwork()
	
	peers := make([][]*labrpc.ClientEnd, n)
	servers := make([]*labrpc.Server, n)
	rafts := make([]*raft.Raft, n)

	clientEnds := make([][]*labrpc.ClientEnd, n)
	applyChs := make([]chan raft.ApplyMsg, n)
	stores := make([]*mvcc.KVStore, n)
	KVservers := make([]*kvserver.Server, n)

	for i := 0; i < n; i++ {
		servers[i] = labrpc.MakeServer()
		applyChs[i] = make(chan raft.ApplyMsg)
	}

	for i := range peers {
		peers[i] = make([]*labrpc.ClientEnd, n)
		for j := 0; j < n; j++ {
			peers[i][j] = network.MakeEnd(fmt.Sprintf("peer-%d-%d", i, j))
		}
	}
	
	for i := 0; i < n; i++ {
		ps := persister.MakePersister()
		var p persister.Persister
		p = ps

		rafts[i] = raft.Make(peers[i], i, p, applyChs[i])

		servers[i].AddService(labrpc.MakeService(rafts[i]))
		network.AddServer(fmt.Sprintf("server-%d", i), servers[i])
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

	// raftNode := raft.Make(peers, id, p, applyCh)
	// for {
	// 	_, isLeader := raftNode.GetState()
	// 	if isLeader {
	// 		break
	// 	}
	// 	time.Sleep(100 * time.Microsecond)
	// }

	Tools.Debug("next")
	///////////////////////////////////////////////////
	
	// for i := range servers {
	// 	servers[i] = labrpc.MakeServer()
	// 	servers[i].AddService(labrpc.MakeService(raftNode))
	// 	network.AddServer(fmt.Sprintf("peer-%d", i), servers[i])
	// }
	// for i := range peers {
	// 	network.Connect(fmt.Sprintf("peer-%d", i), fmt.Sprintf("peer-%d", i))
	// 	network.Enable(fmt.Sprintf("peer-%d", i), true)
	// }

	for i := 0; i < n; i++ {
		
		stores[i] = mvcc.NewKVStore()
		core := kvserver.NewServer(i, rafts[i], stores[i], applyChs[i])
		adapter := grpc.NewRPCAdapter(core)

		servers[i].AddService(labrpc.MakeService(adapter))

		KVservers[i] = core
	}

	for i := 0; i < n; i++ {
		clientEnds[i] = make([]*labrpc.ClientEnd, n)

		for j := 0; j < n; j++ {
			name := fmt.Sprintf("client-%d-%d", i, j)
			clientEnds[i][j] = network.MakeEnd(name)
			network.Connect(
				name,
				fmt.Sprintf("server-%d", j),
			)
			network.Enable(name, true)
		}
		
	}

	// leader_id := findLeader(rafts, n)
	findLeader(rafts, n)
	Tools.Debug("Leader Success")

	ck := client.Make(clientEnds[0])
	ck.Put("a", []byte("3"))
	Tools.Debug("Put Success")
	v := ck.Get("a")
	fmt.Printf("value = %s\n",v)

	// select{}
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