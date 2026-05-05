//go:build integration

package tests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	kvpb "etcd-KV/internal/api/kv/pb"
	"etcd-KV/internal/pb/raftpb"
	"etcd-KV/internal/raft"
	"etcd-KV/internal/server/kvserver"
	"etcd-KV/internal/storage/mvcc"
	"etcd-KV/internal/storage/persister"
	grpctransport "etcd-KV/internal/transport/grpc"

	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Keep the gRPC endpoints stable while swapping node implementations during restart tests.
type kvServerProxy struct {
	kvpb.UnimplementedKVServer

	mu       sync.RWMutex
	delegate kvpb.KVServer
}

func (p *kvServerProxy) setServer(server kvpb.KVServer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.delegate = server
}

func (p *kvServerProxy) getServer() (kvpb.KVServer, error) {
	p.mu.RLock()
	server := p.delegate
	p.mu.RUnlock()

	if server == nil {
		return nil, status.Error(codes.Unavailable, "kv server not ready")
	}

	return server, nil
}

func (p *kvServerProxy) Put(
	ctx context.Context,
	req *kvpb.PutRequest,
) (*kvpb.PutResponse, error) {
	server, err := p.getServer()
	if err != nil {
		return nil, err
	}

	return server.Put(ctx, req)
}

func (p *kvServerProxy) Get(
	ctx context.Context,
	req *kvpb.GetRequest,
) (*kvpb.GetResponse, error) {
	server, err := p.getServer()
	if err != nil {
		return nil, err
	}

	return server.Get(ctx, req)
}

type restartableKVGRPCHarness struct {
	rafts     []*raft.Raft
	applyChs  []chan raft.ApplyMsg
	dataRoot  string
	dataDirs  []string
	raftConns []*gogrpc.ClientConn
	kvConns   []*gogrpc.ClientConn
	kvClients []kvpb.KVClient

	raftServers []*gogrpc.Server
	kvServers   []*gogrpc.Server
	raftLis     []*bufconn.Listener
	kvLis       []*bufconn.Listener
	raftProxies []*raftServerProxy
	kvProxies   []*kvServerProxy
}

func (h *restartableKVGRPCHarness) Close() {
	h.stopAllNodes()

	for _, conn := range h.kvConns {
		if conn != nil {
			_ = conn.Close()
		}
	}

	for _, conn := range h.raftConns {
		if conn != nil {
			_ = conn.Close()
		}
	}

	for _, srv := range h.kvServers {
		if srv != nil {
			srv.Stop()
		}
	}

	for _, srv := range h.raftServers {
		if srv != nil {
			srv.Stop()
		}
	}

	for _, lis := range h.kvLis {
		if lis != nil {
			_ = lis.Close()
		}
	}

	for _, lis := range h.raftLis {
		if lis != nil {
			_ = lis.Close()
		}
	}

	// Keep the data directory alive for a short grace period so any in-flight
	// Raft goroutines finish before the on-disk state is removed.
	time.Sleep(300 * time.Millisecond)

	if h.dataRoot != "" {
		_ = os.RemoveAll(h.dataRoot)
	}
}

func (h *restartableKVGRPCHarness) stopNode(i int) {
	if i < 0 || i >= len(h.rafts) {
		return
	}

	if h.kvProxies[i] != nil {
		h.kvProxies[i].setServer(nil)
	}
	if h.raftProxies[i] != nil {
		h.raftProxies[i].setRaft(nil)
	}
	if h.rafts[i] != nil {
		h.rafts[i].Kill()
		h.rafts[i] = nil
	}

	h.applyChs[i] = nil
}

func (h *restartableKVGRPCHarness) stopAllNodes() {
	for i := range h.rafts {
		h.stopNode(i)
	}
}

func (h *restartableKVGRPCHarness) restartNode(t *testing.T, i int) {
	t.Helper()

	if h.rafts[i] != nil {
		t.Fatalf("node %d is already running", i)
	}

	applyCh := make(chan raft.ApplyMsg, 128)
	peers := make([]raft.Peer, len(h.raftConns))
	for j, conn := range h.raftConns {
		peers[j] = grpctransport.NewGrpcPeer(conn)
	}

	ps, err := persister.MakeDiskPersister(h.dataDirs[i])
	if err != nil {
		t.Fatalf("MakeDiskPersister(node=%d) error = %v", i, err)
	}

	rf := raft.Make(peers, i, ps, applyCh)
	store := mvcc.NewKVStoreWithOptions(mvcc.StoreOptions{
		LeaseExpireMode: mvcc.LeaseExpireExternal,
	})
	core := kvserver.NewServer(i, rf, store, applyCh)

	h.applyChs[i] = applyCh
	h.rafts[i] = rf
	h.raftProxies[i].setRaft(rf)
	h.kvProxies[i].setServer(kvserver.NewRPCAdapter(core))
}

func (h *restartableKVGRPCHarness) restartAllNodes(t *testing.T) {
	t.Helper()

	for i := range h.rafts {
		if h.rafts[i] == nil {
			h.restartNode(t, i)
		}
	}
}

func TestIntegrationFollowerCrashRestartCatchesUpAndSupportsQuorum(t *testing.T) {
	h := startRestartableKVGRPCHarness(t, 3)
	defer h.Close()

	leader := waitForHarnessLeader(t, h, 5*time.Second)
	crashedFollower := (leader + 1) % 3

	const putClientID = int64(5101)
	var putSeq int64
	nextPutSeq := func() int64 {
		putSeq++
		return putSeq
	}

	if _, err := putViaAnyNode(
		h.kvClients,
		"restart/follower/base",
		[]byte("base"),
		putClientID,
		nextPutSeq(),
		5*time.Second,
	); err != nil {
		t.Fatalf("baseline Put failed: %v", err)
	}

	h.stopNode(crashedFollower)

	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("restart/follower/offline-%02d", i)
		value := []byte(fmt.Sprintf("value-%02d", i))
		if _, err := putViaAnyNode(
			h.kvClients,
			key,
			value,
			putClientID,
			nextPutSeq(),
			5*time.Second,
		); err != nil {
			t.Fatalf("Put while follower %d is down failed at step %d: %v", crashedFollower, i, err)
		}
	}

	h.restartNode(t, crashedFollower)

	currentLeader := waitForHarnessLeader(t, h, 5*time.Second)
	otherNode := findRemainingNode(len(h.rafts), currentLeader, crashedFollower)
	h.stopNode(otherNode)

	finalKey := "restart/follower/after-restart"
	finalValue := []byte("quorum-through-restarted-follower")
	if _, err := putViaAnyNode(
		h.kvClients,
		finalKey,
		finalValue,
		putClientID,
		nextPutSeq(),
		8*time.Second,
	); err != nil {
		t.Fatalf("Put requiring restarted follower quorum failed: %v", err)
	}

	waitForValueViaAnyNode(t, h.kvClients, finalKey, finalValue, 5201, 5*time.Second)
}

func TestIntegrationLeaderCrashRestartReelectsAndRecovers(t *testing.T) {
	h := startRestartableKVGRPCHarness(t, 3)
	defer h.Close()

	originalLeader := waitForHarnessLeader(t, h, 5*time.Second)

	const putClientID = int64(6101)
	var putSeq int64
	nextPutSeq := func() int64 {
		putSeq++
		return putSeq
	}

	if _, err := putViaAnyNode(
		h.kvClients,
		"restart/leader/before-crash",
		[]byte("before-crash"),
		putClientID,
		nextPutSeq(),
		5*time.Second,
	); err != nil {
		t.Fatalf("baseline Put failed: %v", err)
	}

	h.stopNode(originalLeader)

	replacementLeader := waitForHarnessLeader(t, h, 5*time.Second)
	if replacementLeader == originalLeader {
		t.Fatalf("leader did not change after stopping node %d", originalLeader)
	}

	duringCrashKey := "restart/leader/during-crash"
	duringCrashValue := []byte("written-with-replacement-leader")
	if _, err := putViaAnyNode(
		h.kvClients,
		duringCrashKey,
		duringCrashValue,
		putClientID,
		nextPutSeq(),
		8*time.Second,
	); err != nil {
		t.Fatalf("Put with original leader down failed: %v", err)
	}

	h.restartNode(t, originalLeader)
	waitForHarnessLeader(t, h, 5*time.Second)

	otherNode := findRemainingNode(len(h.rafts), originalLeader, replacementLeader)
	h.stopNode(otherNode)

	finalKey := "restart/leader/after-restart"
	finalValue := []byte("restarted-leader-rejoined")
	if _, err := putViaAnyNode(
		h.kvClients,
		finalKey,
		finalValue,
		putClientID,
		nextPutSeq(),
		8*time.Second,
	); err != nil {
		t.Fatalf("Put requiring the restarted leader to rejoin quorum failed: %v", err)
	}

	waitForValueViaAnyNode(t, h.kvClients, duringCrashKey, duringCrashValue, 6201, 5*time.Second)
	waitForValueViaAnyNode(t, h.kvClients, finalKey, finalValue, 6202, 5*time.Second)
}

func startRestartableKVGRPCHarness(t *testing.T, n int) *restartableKVGRPCHarness {
	t.Helper()

	h := &restartableKVGRPCHarness{
		rafts:       make([]*raft.Raft, n),
		applyChs:    make([]chan raft.ApplyMsg, n),
		dataDirs:    make([]string, n),
		raftConns:   make([]*gogrpc.ClientConn, n),
		kvConns:     make([]*gogrpc.ClientConn, n),
		kvClients:   make([]kvpb.KVClient, n),
		raftServers: make([]*gogrpc.Server, n),
		kvServers:   make([]*gogrpc.Server, n),
		raftLis:     make([]*bufconn.Listener, n),
		kvLis:       make([]*bufconn.Listener, n),
		raftProxies: make([]*raftServerProxy, n),
		kvProxies:   make([]*kvServerProxy, n),
	}

	dataRoot, err := os.MkdirTemp("", "restartable-kv-grpc-*")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	h.dataRoot = dataRoot

	for i := 0; i < n; i++ {
		h.dataDirs[i] = filepath.Join(dataRoot, fmt.Sprintf("node-%d", i))
	}

	for i := 0; i < n; i++ {
		h.raftLis[i] = bufconn.Listen(testBufConnSize)
		h.raftServers[i] = gogrpc.NewServer()
		h.raftProxies[i] = &raftServerProxy{}
		raftpb.RegisterRaftServer(h.raftServers[i], h.raftProxies[i])

		go func(srv *gogrpc.Server, lis *bufconn.Listener) {
			_ = srv.Serve(lis)
		}(h.raftServers[i], h.raftLis[i])
	}

	for i := 0; i < n; i++ {
		h.kvLis[i] = bufconn.Listen(testBufConnSize)
		h.kvServers[i] = gogrpc.NewServer()
		h.kvProxies[i] = &kvServerProxy{}
		kvpb.RegisterKVServer(h.kvServers[i], h.kvProxies[i])

		go func(srv *gogrpc.Server, lis *bufconn.Listener) {
			_ = srv.Serve(lis)
		}(h.kvServers[i], h.kvLis[i])
	}

	for i := 0; i < n; i++ {
		h.raftConns[i] = dialBufConn(t, h.raftLis[i])
		h.kvConns[i] = dialBufConn(t, h.kvLis[i])
		h.kvClients[i] = kvpb.NewKVClient(h.kvConns[i])
	}

	for i := 0; i < n; i++ {
		h.restartNode(t, i)
	}

	return h
}

func waitForHarnessLeader(
	t *testing.T,
	h *restartableKVGRPCHarness,
	timeout time.Duration,
) int {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := -1

		for i, rf := range h.rafts {
			if rf == nil {
				continue
			}

			if _, isLeader := rf.GetState(); isLeader {
				if leader != -1 {
					leader = -1
					break
				}
				leader = i
			}
		}

		if leader != -1 {
			return leader
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("timed out waiting for leader election in restartable harness")
	return -1
}

func waitForValueViaAnyNode(
	t *testing.T,
	clients []kvpb.KVClient,
	key string,
	want []byte,
	clientID int64,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var seq int64
	var lastValue []byte
	var lastErr error

	for time.Now().Before(deadline) {
		seq++

		resp, err := getResponseViaAnyNode(clients, key, clientID, seq, 500*time.Millisecond)
		if err == nil {
			lastValue = append(lastValue[:0], resp.Value...)
			if resp.Found && bytes.Equal(resp.Value, want) {
				return
			}
		} else {
			lastErr = err
		}

		time.Sleep(30 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("timed out waiting for key %q to reach value %q: last error: %v", key, want, lastErr)
	}
	t.Fatalf("timed out waiting for key %q to reach value %q: last value %q", key, want, lastValue)
}

func findRemainingNode(total int, excludeA int, excludeB int) int {
	for i := 0; i < total; i++ {
		if i != excludeA && i != excludeB {
			return i
		}
	}

	return -1
}
