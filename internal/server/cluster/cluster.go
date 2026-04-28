// 负责把一个 Raft 节点组装成一个可独立运行的进程节点
package cluster

import (
	"encoding/json"
	"etcd-KV/Tools"
	kvpb "etcd-KV/internal/api/kv/pb"
	"etcd-KV/internal/pb/raftpb"
	"etcd-KV/internal/raft"
	"etcd-KV/internal/server/kvserver"
	"etcd-KV/internal/storage/mvcc"
	"etcd-KV/internal/storage/persister"
	grpctransport "etcd-KV/internal/transport/grpc"
	"fmt"
	"net"
	"os"
	"sync"

	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const applyBufferSize = 1000

// 描述集群中一个 raft peer 的静态地址信息
type PeerConfig struct {
	ID         int    `json:"id"`          // ID 当前等价于 raft.Make 的 peers 下标，从 0 递增
	PeerAddr   string `json:"peer_addr"`   // 节点间 Raft RPC 使用的监听地址
	ClientAddr string `json:"client_addr"` // 客户端访问 KV API 的监听地址
}

// 描述“当前进程要启动的这一个节点”
type NodeConfig struct {
	ID         int          `json:"id"`
	PeerAddr   string       `json:"peer_addr"`
	ClientAddr string       `json:"client_addr"`
	DataDir    string       `json:"data_dir"` // 当前节点持久化 raft/snapshot 数据的目录
	Peers      []PeerConfig `json:"peers"`    // 完整集群成员列表，所有节点必须一致
}

// 表示一个已经启动或正在关闭的单节点进程
type Node struct {
	cfg      NodeConfig
	raftNode *raft.Raft
	kvCore   *kvserver.Server

	peerHandler  *grpctransport.RaftServer
	peerListener net.Listener
	peerServer   *gogrpc.Server

	clientListener net.Listener
	clientServer   *gogrpc.Server
	peerConns      []*gogrpc.ClientConn

	closeOnce sync.Once
	closeErr  error
}

// 从配置文件加载 NodeConfig
func LoadNodeConfig(path string) (NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return NodeConfig{}, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg NodeConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return NodeConfig{}, fmt.Errorf("decode config %q: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return NodeConfig{}, fmt.Errorf("validate config %q: %w", path, err)
	}

	return cfg, nil
}

// 校验当前 raft 实现所需的单节点配置约束
func (c NodeConfig) Validate() error {
	if c.ID < 0 {
		return fmt.Errorf("node id must be >= 0: %d", c.ID)
	}

	if c.PeerAddr == "" {
		return fmt.Errorf("peer_addr is required")
	}

	if c.ClientAddr == "" {
		return fmt.Errorf("client_addr is required")
	}

	if c.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}

	if c.ID >= len(c.Peers) {
		return fmt.Errorf("node is %d is outside peers length %d", c.ID, len(c.Peers))
	}

	for i, peer := range c.Peers {
		if peer.ID != i {
			return fmt.Errorf("peer[%d].id = %d, want %d", i, peer.ID, i)
		}

		if peer.PeerAddr == "" {
			return fmt.Errorf("peer[%d].peer_addr is required", i)
		}

		if peer.ClientAddr == "" {
			return fmt.Errorf("peer[%d].client_addr is required", i)
		}
	}

	if c.Peers[c.ID].PeerAddr != c.PeerAddr {
		return fmt.Errorf("self peer_addr %q does not match peers[%d].peer_addr %q", c.PeerAddr, c.ID, c.Peers[c.ID].PeerAddr)
	}

	if c.Peers[c.ID].ClientAddr != c.ClientAddr {
		return fmt.Errorf("self client_addr %q does not match peers[%d].client_addr %q", c.ClientAddr, c.ID, c.Peers[c.ID].ClientAddr)
	}

	return nil
}

// 根据配置启动一个完整的单节点进程
func StartNode(cfg NodeConfig) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	node := &Node{cfg: cfg}
	applyCh := make(chan raft.ApplyMsg, applyBufferSize)

	peerListener, err := net.Listen("tcp", cfg.PeerAddr)
	if err != nil {
		return nil, fmt.Errorf("listen peer %q: %w", cfg.PeerAddr, err)
	}

	node.peerListener = peerListener
	node.peerServer = gogrpc.NewServer()
	node.peerHandler = grpctransport.NewRaftServer(nil)
	raftpb.RegisterRaftServer(node.peerServer, node.peerHandler)

	go serveGRPC("raft", node.peerServer, node.peerListener)

	node.peerConns = make([]*gogrpc.ClientConn, len(cfg.Peers))
	peers := make([]raft.Peer, len(cfg.Peers))

	for i, peerCfg := range cfg.Peers {
		conn, err := gogrpc.Dial(peerCfg.PeerAddr, gogrpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial peer %d at %q: %w", peerCfg.ID, peerCfg.PeerAddr, err)
		}

		node.peerConns[i] = conn
		peers[i] = grpctransport.NewGrpcPeer(conn)
	}

	ps, err := persister.MakeDiskPersister(cfg.DataDir)
	if err != nil {
		_ = node.Close()

		return nil, fmt.Errorf("open persister %q: %w", cfg.DataDir, err)
	}

	node.raftNode = raft.Make(peers, cfg.ID, ps, applyCh)
	node.peerHandler.SetRaft(node.raftNode)

	store := mvcc.NewKVStoreWithOptions(mvcc.StoreOptions{LeaseExpireMode: mvcc.LeaseExpireExternal})

	node.kvCore = kvserver.NewServer(cfg.ID, node.raftNode, store, applyCh)
	clientListener, err := net.Listen("tcp", cfg.ClientAddr)
	if err != nil {
		_ = node.Close()

		return nil, fmt.Errorf("listen client%q: %w", cfg.ClientAddr, err)
	}

	node.clientListener = clientListener
	node.clientServer = gogrpc.NewServer()
	kvpb.RegisterKVServer(node.clientServer, kvserver.NewRPCAdapter(node.kvCore))
	go serveGRPC("kv", node.clientServer, node.clientListener)

	Tools.Info("node %d started: peer=%s client=%s data=%s", cfg.ID, cfg.PeerAddr, cfg.ClientAddr, cfg.DataDir)
	return node, nil

}

// 统一启动一个 gRPC server 并记录退出原因
func serveGRPC(name string, srv *gogrpc.Server, lis net.Listener) {
	if err := srv.Serve(lis); err != nil {
		Tools.Warn("%s grpc server stopped: %v", name, err)
	}
}

// 关闭节点拥有的 raft, gRPC, server, listener 和 peer 连接
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		if n.raftNode != nil {
			n.raftNode.Kill()
		}

		if n.clientServer != nil {
			n.clientServer.Stop()
		}

		if n.peerServer != nil {
			n.peerServer.Stop()
		}

		if n.clientListener != nil {
			_ = n.clientListener.Close()
		}

		if n.peerListener != nil {
			_ = n.peerListener.Close()
		}

		for _, conn := range n.peerConns {
			if conn == nil {
				continue
			}

			if err := conn.Close(); err != nil && n.closeErr == nil {
				n.closeErr = err
			}
		}
	})

	return n.closeErr
}
