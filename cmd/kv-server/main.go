// 主函数
// package main

// import (
// 	"time"

// 	"etcd-KV/Tools"
// 	"etcd-KV/internal/client"
// 	"etcd-KV/internal/labrpc"
// 	"etcd-KV/internal/raft"
// 	"etcd-KV/internal/server/kvserver"
// 	"etcd-KV/internal/storage/mvcc"
// 	"etcd-KV/internal/storage/persister"
// 	"etcd-KV/internal/transport/grpc"
// 	"fmt"
// )

// func main() {
// 	// 1. 解析配置
// 	peerAddrs := []string{
// 		"127.0.0.1:8001",
// 		"127.0.0.1:8002",
// 		"127.0.0.1:8003",
// 	}
// 	// Tools.Info("len", len(peerAddrs))
// 	n := len(peerAddrs)
// 	network := labrpc.MakeNetwork()

// 	peers := make([][]*labrpc.ClientEnd, n)
// 	servers := make([]*labrpc.Server, n)
// 	rafts := make([]*raft.Raft, n)

// 	clientEnds := make([][]*labrpc.ClientEnd, n)
// 	applyChs := make([]chan raft.ApplyMsg, n)
// 	stores := make([]*mvcc.KVStore, n)
// 	KVservers := make([]*kvserver.Server, n)

// 	for i := 0; i < n; i++ {
// 		servers[i] = labrpc.MakeServer()
// 		applyChs[i] = make(chan raft.ApplyMsg)
// 	}

// 	for i := range peers {
// 		peers[i] = make([]*labrpc.ClientEnd, n)
// 		for j := 0; j < n; j++ {
// 			peers[i][j] = network.MakeEnd(fmt.Sprintf("peer-%d-%d", i, j))
// 		}
// 	}

// 	for i := 0; i < n; i++ {
// 		ps := persister.MakePersister()
// 		var p persister.Persister
// 		p = ps

// 		rafts[i] = raft.Make(peers[i], i, p, applyChs[i])

// 		servers[i].AddService(labrpc.MakeService(rafts[i]))
// 		network.AddServer(fmt.Sprintf("server-%d", i), servers[i])
// 	}

// 	for i := 0; i < n; i++ {
// 		for j := 0; j < n; j++ {
// 			network.Connect(
// 				fmt.Sprintf("peer-%d-%d", i, j),
// 				fmt.Sprintf("server-%d", j),
// 			)
// 			network.Enable(fmt.Sprintf("peer-%d-%d", i, j), true)
// 		}
// 	}

// 	// raftNode := raft.Make(peers, id, p, applyCh)
// 	// for {
// 	// 	_, isLeader := raftNode.GetState()
// 	// 	if isLeader {
// 	// 		break
// 	// 	}
// 	// 	time.Sleep(100 * time.Microsecond)
// 	// }

// 	Tools.Debug("next")
// 	///////////////////////////////////////////////////

// 	// for i := range servers {
// 	// 	servers[i] = labrpc.MakeServer()
// 	// 	servers[i].AddService(labrpc.MakeService(raftNode))
// 	// 	network.AddServer(fmt.Sprintf("peer-%d", i), servers[i])
// 	// }
// 	// for i := range peers {
// 	// 	network.Connect(fmt.Sprintf("peer-%d", i), fmt.Sprintf("peer-%d", i))
// 	// 	network.Enable(fmt.Sprintf("peer-%d", i), true)
// 	// }

// 	for i := 0; i < n; i++ {

// 		stores[i] = mvcc.NewKVStore()
// 		core := kvserver.NewServer(i, rafts[i], stores[i], applyChs[i])
// 		adapter := grpc.NewRPCAdapter(core)

// 		servers[i].AddService(labrpc.MakeService(adapter))

// 		KVservers[i] = core
// 	}

// 	for i := 0; i < n; i++ {
// 		clientEnds[i] = make([]*labrpc.ClientEnd, n)

// 		for j := 0; j < n; j++ {
// 			name := fmt.Sprintf("client-%d-%d", i, j)
// 			clientEnds[i][j] = network.MakeEnd(name)
// 			network.Connect(
// 				name,
// 				fmt.Sprintf("server-%d", j),
// 			)
// 			network.Enable(name, true)
// 		}

// 	}

// 	// leader_id := findLeader(rafts, n)
// 	findLeader(rafts, n)
// 	Tools.Debug("Leader Success")

// 	ck := client.Make(clientEnds[0])
// 	ck.Put("a", []byte("3"))
// 	Tools.Debug("Put Success")
// 	v := ck.Get("a")
// 	fmt.Printf("value = %s\n",v)

// 	// select{}
// }

package main

import (
	"context"
	"fmt"
	"time"

	"etcd-KV/Tools"
	"etcd-KV/internal/client"
	"etcd-KV/internal/labrpc"
	"etcd-KV/internal/raft"
	"etcd-KV/internal/server/kvserver"
	"etcd-KV/internal/storage/mvcc"
	"etcd-KV/internal/storage/persister"
	"etcd-KV/internal/transport/grpc"
)

func main() {
	peerAddrs := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}
	n := len(peerAddrs)
	network := labrpc.MakeNetwork()

	// 初始化数组
	peers := make([][]*labrpc.ClientEnd, n)
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

	for i := range peers {
		peers[i] = make([]*labrpc.ClientEnd, n)
		for j := 0; j < n; j++ {
			peers[i][j] = network.MakeEnd(fmt.Sprintf("peer-%d-%d", i, j))
		}
	}

	for i := 0; i < n; i++ {
		ps := persister.MakePersister()
		rafts[i] = raft.Make(peers[i], i, ps, applyChs[i])
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
			network.Connect(name, fmt.Sprintf("server-%d", j))
			network.Enable(name, true)
		}
	}

	// 等待 leader 选举完成
	findLeader := func(rafts []*raft.Raft, n int) int {
		for {
			for i, r := range rafts {
				_, isLeader := r.GetState()
				if isLeader {
					return i
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	leader := findLeader(rafts, n)
	Tools.Debug(fmt.Sprintf("Leader Success, leader = %d", leader))

	ck := client.Make(clientEnds[leader]) // 防越界
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
