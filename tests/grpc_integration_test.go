//go:build integration

package tests

import (
	"bytes"
	"context"
	"errors"
	"net"
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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testBufConnSize = 1024 * 1024

type raftServerProxy struct {
	raftpb.UnimplementedRaftServer

	delegate raftpb.RaftServer
}

func (p *raftServerProxy) setRaft(rf *raft.Raft) {
	p.delegate = grpctransport.NewRaftServer(rf)
}

func (p *raftServerProxy) AppendEntries(
	ctx context.Context,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesReply, error) {
	if p.delegate == nil {
		return nil, status.Error(codes.Unavailable, "raft server not ready")
	}
	return p.delegate.AppendEntries(ctx, req)
}

func (p *raftServerProxy) RequestVote(
	ctx context.Context,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteReply, error) {
	if p.delegate == nil {
		return nil, status.Error(codes.Unavailable, "raft server not ready")
	}
	return p.delegate.RequestVote(ctx, req)
}

func (p *raftServerProxy) InstallSnapshot(
	ctx context.Context,
	req *raftpb.InstallSnapshotRequest,
) (*raftpb.InstallSnapshotReply, error) {
	if p.delegate == nil {
		return nil, status.Error(codes.Unavailable, "raft server not ready")
	}
	return p.delegate.InstallSnapshot(ctx, req)
}

type raftGRPCHarness struct {
	rafts     []*raft.Raft
	applyChs  []chan raft.ApplyMsg
	conns     []*gogrpc.ClientConn
	servers   []*gogrpc.Server
	listeners []*bufconn.Listener
}

func (h *raftGRPCHarness) Close() {
	for _, rf := range h.rafts {
		if rf != nil {
			rf.Kill()
		}
	}

	for _, conn := range h.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}

	for _, srv := range h.servers {
		if srv != nil {
			srv.Stop()
		}
	}

	for _, lis := range h.listeners {
		if lis != nil {
			_ = lis.Close()
		}
	}
}

type kvGRPCHarness struct {
	rafts       []*raft.Raft
	kvClients   []kvpb.KVClient
	raftConns   []*gogrpc.ClientConn
	kvConns     []*gogrpc.ClientConn
	raftServers []*gogrpc.Server
	kvServers   []*gogrpc.Server
	raftLis     []*bufconn.Listener
	kvLis       []*bufconn.Listener
}

func (h *kvGRPCHarness) Close() {
	for _, rf := range h.rafts {
		if rf != nil {
			rf.Kill()
		}
	}

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
}

func TestIntegrationRaftGRPCElectsLeaderAndReplicates(t *testing.T) {
	h := startRaftGRPCHarness(t, 3)
	defer h.Close()

	leader := waitForLeader(t, h.rafts, 5*time.Second)
	payload := []byte("raft-grpc-integration")

	index, _, ok := h.rafts[leader].Start(payload)
	if !ok {
		t.Fatalf("server %d rejected Start even though it was elected leader", leader)
	}

	for i, ch := range h.applyChs {
		msg := waitForApplyMsg(t, ch, 5*time.Second)
		if !msg.CommandValid {
			t.Fatalf("server %d delivered non-command apply message: %+v", i, msg)
		}
		if msg.CommandIndex != index {
			t.Fatalf("server %d applied index %d, want %d", i, msg.CommandIndex, index)
		}
		if !bytes.Equal(msg.Command, payload) {
			t.Fatalf("server %d applied payload %q, want %q", i, msg.Command, payload)
		}
	}
}

func TestIntegrationKVGRPCRoundTripWithPBClient(t *testing.T) {
	h := startKVGRPCHarness(t, 3)
	defer h.Close()

	const (
		clientID = int64(42)
		putSeq   = int64(1)
		getSeq   = int64(2)
	)

	key := "grpc/alpha"
	value := []byte("beta")

	if _, err := putViaAnyNode(h.kvClients, key, value, clientID, putSeq, 5*time.Second); err != nil {
		t.Fatalf("Put over gRPC failed: %v", err)
	}

	got, err := getViaAnyNode(h.kvClients, key, clientID, getSeq, 5*time.Second)
	if err != nil {
		t.Fatalf("Get over gRPC failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Fatalf("Get returned %q, want %q", got, value)
	}
}

func TestIntegrationKVGRPCRejectsPutWithMissingLease(t *testing.T) {
	h := startKVGRPCHarness(t, 3)
	defer h.Close()

	const (
		clientID = int64(77)
		putSeq   = int64(1)
		getSeq   = int64(2)
	)

	key := "grpc/missing-lease"
	err := putWithLeaseViaAnyNode(h.kvClients, key, []byte("value"), 999, clientID, putSeq, 5*time.Second)
	if err == nil {
		t.Fatal("expected Put with missing lease to fail")
	}

	resp, err := getResponseViaAnyNode(h.kvClients, key, clientID, getSeq, 5*time.Second)
	if err != nil {
		t.Fatalf("Get after failed Put returned error: %v", err)
	}
	if resp.Found {
		t.Fatalf("expected key %q to be absent after failed Put", key)
	}
}

func TestIntegrationKVGRPCLeaseExpiresThroughLeaderRaftPath(t *testing.T) {
	h := startKVGRPCHarness(t, 3)
	defer h.Close()

	const (
		clientID = int64(88)
		grantSeq = int64(1)
		putSeq   = int64(2)
		getSeq   = int64(3)
	)

	key := "grpc/lease-expire"
	value := []byte("ttl-value")

	leaseID, err := leaseGrantViaAnyNode(h.kvClients, 1, clientID, grantSeq, 5*time.Second)
	if err != nil {
		t.Fatalf("LeaseGrant over gRPC failed: %v", err)
	}

	if err := putWithLeaseViaAnyNode(h.kvClients, key, value, leaseID, clientID, putSeq, 5*time.Second); err != nil {
		t.Fatalf("Put with lease over gRPC failed: %v", err)
	}

	resp, err := getResponseViaAnyNode(h.kvClients, key, clientID, getSeq, 5*time.Second)
	if err != nil {
		t.Fatalf("Get before expiration failed: %v", err)
	}
	if !resp.Found || !bytes.Equal(resp.Value, value) {
		t.Fatalf("Get before expiration = found:%v value:%q, want found:true value:%q", resp.Found, resp.Value, value)
	}

	deadline := time.Now().Add(6 * time.Second)
	pollSeq := getSeq + 1
	for time.Now().Before(deadline) {
		resp, err = getResponseViaAnyNode(h.kvClients, key, clientID, pollSeq, 500*time.Millisecond)
		if err == nil && !resp.Found {
			return
		}
		pollSeq++
		time.Sleep(50 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("Get after expiration returned error: %v", err)
	}
	t.Fatalf("expected leased key %q to expire through leader-driven revoke", key)
}

func TestIntegrationKVGRPCTxnRejectsPutWithMissingLease(t *testing.T) {
	h := startKVGRPCHarness(t, 3)
	defer h.Close()

	const (
		clientID = int64(91)
		txnSeq   = int64(1)
		getSeq   = int64(2)
	)

	key := "grpc/txn-missing-lease"
	_, err := txnViaAnyNode(h.kvClients, &kvpb.TxnRequest{
		ClientId: clientID,
		Seq:      txnSeq,
		ThenOps: []*kvpb.Op{{
			Type:    kvpb.OpType_OP_PUT,
			Key:     key,
			Value:   []byte("value"),
			LeaseId: 999,
		}},
	}, 5*time.Second)
	if err == nil {
		t.Fatal("expected txn put with missing lease to fail")
	}

	resp, err := getResponseViaAnyNode(h.kvClients, key, clientID, getSeq, 5*time.Second)
	if err != nil {
		t.Fatalf("Get after failed txn returned error: %v", err)
	}
	if resp.Found {
		t.Fatalf("expected key %q to remain absent after failed txn", key)
	}
}

func TestIntegrationKVGRPCTxnPutWithLeaseExpiresThroughLeaderRaftPath(t *testing.T) {
	h := startKVGRPCHarness(t, 3)
	defer h.Close()

	const (
		clientID = int64(92)
		grantSeq = int64(1)
		txnSeq   = int64(2)
		getSeq   = int64(3)
	)

	key := "grpc/txn-lease-expire"
	value := []byte("txn-ttl-value")

	leaseID, err := leaseGrantViaAnyNode(h.kvClients, 1, clientID, grantSeq, 5*time.Second)
	if err != nil {
		t.Fatalf("LeaseGrant over gRPC failed: %v", err)
	}

	resp, err := txnViaAnyNode(h.kvClients, &kvpb.TxnRequest{
		ClientId: clientID,
		Seq:      txnSeq,
		ThenOps: []*kvpb.Op{{
			Type:    kvpb.OpType_OP_PUT,
			Key:     key,
			Value:   value,
			LeaseId: leaseID,
		}},
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("Txn put with lease over gRPC failed: %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("expected txn put with lease to succeed")
	}

	getResp, err := getResponseViaAnyNode(h.kvClients, key, clientID, getSeq, 5*time.Second)
	if err != nil {
		t.Fatalf("Get before txn lease expiration failed: %v", err)
	}
	if !getResp.Found || !bytes.Equal(getResp.Value, value) {
		t.Fatalf("Get before expiration = found:%v value:%q, want found:true value:%q", getResp.Found, getResp.Value, value)
	}

	deadline := time.Now().Add(6 * time.Second)
	pollSeq := getSeq + 1
	for time.Now().Before(deadline) {
		getResp, err = getResponseViaAnyNode(h.kvClients, key, clientID, pollSeq, 500*time.Millisecond)
		if err == nil && !getResp.Found {
			return
		}
		pollSeq++
		time.Sleep(50 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("Get after txn lease expiration returned error: %v", err)
	}
	t.Fatalf("expected txn-leased key %q to expire through leader-driven revoke", key)
}

func startRaftGRPCHarness(t *testing.T, n int) *raftGRPCHarness {
	t.Helper()

	listeners := make([]*bufconn.Listener, n)
	servers := make([]*gogrpc.Server, n)
	proxies := make([]*raftServerProxy, n)

	for i := 0; i < n; i++ {
		listeners[i] = bufconn.Listen(testBufConnSize)
		servers[i] = gogrpc.NewServer()
		proxies[i] = &raftServerProxy{}
		raftpb.RegisterRaftServer(servers[i], proxies[i])

		go func(srv *gogrpc.Server, lis *bufconn.Listener) {
			_ = srv.Serve(lis)
		}(servers[i], listeners[i])
	}

	conns := make([]*gogrpc.ClientConn, n)
	for i := 0; i < n; i++ {
		conns[i] = dialBufConn(t, listeners[i])
	}

	applyChs := make([]chan raft.ApplyMsg, n)
	rafts := make([]*raft.Raft, n)

	for i := 0; i < n; i++ {
		applyChs[i] = make(chan raft.ApplyMsg, 64)

		peers := make([]raft.Peer, n)
		for j := 0; j < n; j++ {
			peers[j] = grpctransport.NewGrpcPeer(conns[j])
		}

		rafts[i] = raft.Make(peers, i, persister.MakePersister(), applyChs[i])
		proxies[i].setRaft(rafts[i])
	}

	return &raftGRPCHarness{
		rafts:     rafts,
		applyChs:  applyChs,
		conns:     conns,
		servers:   servers,
		listeners: listeners,
	}
}

func startKVGRPCHarness(t *testing.T, n int) *kvGRPCHarness {
	t.Helper()

	raftLis := make([]*bufconn.Listener, n)
	raftServers := make([]*gogrpc.Server, n)
	raftProxies := make([]*raftServerProxy, n)

	for i := 0; i < n; i++ {
		raftLis[i] = bufconn.Listen(testBufConnSize)
		raftServers[i] = gogrpc.NewServer()
		raftProxies[i] = &raftServerProxy{}
		raftpb.RegisterRaftServer(raftServers[i], raftProxies[i])

		go func(srv *gogrpc.Server, lis *bufconn.Listener) {
			_ = srv.Serve(lis)
		}(raftServers[i], raftLis[i])
	}

	raftConns := make([]*gogrpc.ClientConn, n)
	for i := 0; i < n; i++ {
		raftConns[i] = dialBufConn(t, raftLis[i])
	}

	applyChs := make([]chan raft.ApplyMsg, n)
	rafts := make([]*raft.Raft, n)
	cores := make([]*kvserver.Server, n)

	for i := 0; i < n; i++ {
		applyChs[i] = make(chan raft.ApplyMsg, 128)

		peers := make([]raft.Peer, n)
		for j := 0; j < n; j++ {
			peers[j] = grpctransport.NewGrpcPeer(raftConns[j])
		}

		rafts[i] = raft.Make(peers, i, persister.MakePersister(), applyChs[i])
		raftProxies[i].setRaft(rafts[i])
		store := mvcc.NewKVStoreWithOptions(mvcc.StoreOptions{
			LeaseExpireMode: mvcc.LeaseExpireExternal,
		})
		cores[i] = kvserver.NewServer(i, rafts[i], store, applyChs[i])
	}

	kvLis := make([]*bufconn.Listener, n)
	kvServers := make([]*gogrpc.Server, n)
	kvConns := make([]*gogrpc.ClientConn, n)
	kvClients := make([]kvpb.KVClient, n)

	for i := 0; i < n; i++ {
		kvLis[i] = bufconn.Listen(testBufConnSize)
		kvServers[i] = gogrpc.NewServer()
		kvpb.RegisterKVServer(kvServers[i], kvserver.NewRPCAdapter(cores[i]))

		go func(srv *gogrpc.Server, lis *bufconn.Listener) {
			_ = srv.Serve(lis)
		}(kvServers[i], kvLis[i])
	}

	for i := 0; i < n; i++ {
		kvConns[i] = dialBufConn(t, kvLis[i])
		kvClients[i] = kvpb.NewKVClient(kvConns[i])
	}

	return &kvGRPCHarness{
		rafts:       rafts,
		kvClients:   kvClients,
		raftConns:   raftConns,
		kvConns:     kvConns,
		raftServers: raftServers,
		kvServers:   kvServers,
		raftLis:     raftLis,
		kvLis:       kvLis,
	}
}

func dialBufConn(t *testing.T, lis *bufconn.Listener) *gogrpc.ClientConn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := gogrpc.DialContext(
		ctx,
		"bufnet",
		gogrpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}

	return conn
}

func waitForLeader(t *testing.T, rafts []*raft.Raft, timeout time.Duration) int {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := -1

		for i, rf := range rafts {
			if _, ok := rf.GetState(); ok {
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

	t.Fatal("timed out waiting for leader election")
	return -1
}

func waitForApplyMsg(t *testing.T, ch <-chan raft.ApplyMsg, timeout time.Duration) raft.ApplyMsg {
	t.Helper()

	select {
	case msg := <-ch:
		return msg
	case <-time.After(timeout):
		t.Fatal("timed out waiting for apply message")
		return raft.ApplyMsg{}
	}
}

func putViaAnyNode(
	clients []kvpb.KVClient,
	key string,
	value []byte,
	clientID int64,
	seq int64,
	timeout time.Duration,
) (int64, error) {
	return putViaAnyNodeWithLease(clients, key, value, 0, clientID, seq, timeout)
}

func putWithLeaseViaAnyNode(
	clients []kvpb.KVClient,
	key string,
	value []byte,
	leaseID int64,
	clientID int64,
	seq int64,
	timeout time.Duration,
) error {
	_, err := putViaAnyNodeWithLease(clients, key, value, leaseID, clientID, seq, timeout)
	return err
}

func putViaAnyNodeWithLease(
	clients []kvpb.KVClient,
	key string,
	value []byte,
	leaseID int64,
	clientID int64,
	seq int64,
	timeout time.Duration,
) (int64, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		for _, cli := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resp, err := cli.Put(ctx, &kvpb.PutRequest{
				Key:      key,
				Value:    value,
				ClientId: clientID,
				Seq:      seq,
				LeaseId:  leaseID,
			})
			cancel()

			if err != nil {
				lastErr = err
				continue
			}

			if resp.Err == "" {
				return resp.Revision, nil
			}

			lastErr = errors.New(resp.Err)
		}

		time.Sleep(30 * time.Millisecond)
	}

	if lastErr == nil {
		lastErr = errors.New("put timed out without a concrete error")
	}
	return 0, lastErr
}

func getViaAnyNode(
	clients []kvpb.KVClient,
	key string,
	clientID int64,
	seq int64,
	timeout time.Duration,
) ([]byte, error) {
	resp, err := getResponseViaAnyNode(clients, key, clientID, seq, timeout)
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

func getResponseViaAnyNode(
	clients []kvpb.KVClient,
	key string,
	clientID int64,
	seq int64,
	timeout time.Duration,
) (*kvpb.GetResponse, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		for _, cli := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resp, err := cli.Get(ctx, &kvpb.GetRequest{
				Key:      key,
				ClientId: clientID,
				Seq:      seq,
			})
			cancel()

			if err != nil {
				lastErr = err
				continue
			}

			if resp.Err == "" {
				return resp, nil
			}

			lastErr = errors.New(resp.Err)
		}

		time.Sleep(30 * time.Millisecond)
	}

	if lastErr == nil {
		lastErr = errors.New("get timed out without a concrete error")
	}
	return nil, lastErr
}

func leaseGrantViaAnyNode(
	clients []kvpb.KVClient,
	ttl int64,
	clientID int64,
	seq int64,
	timeout time.Duration,
) (int64, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		for _, cli := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resp, err := cli.LeaseGrant(ctx, &kvpb.LeaseGrantRequest{
				Ttl:      ttl,
				ClientId: clientID,
				Seq:      seq,
			})
			cancel()

			if err != nil {
				lastErr = err
				continue
			}

			if resp.Err == "" {
				return resp.Id, nil
			}

			lastErr = errors.New(resp.Err)
		}

		time.Sleep(30 * time.Millisecond)
	}

	if lastErr == nil {
		lastErr = errors.New("lease grant timed out without a concrete error")
	}
	return 0, lastErr
}

func txnViaAnyNode(
	clients []kvpb.KVClient,
	req *kvpb.TxnRequest,
	timeout time.Duration,
) (*kvpb.TxnResponse, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		for _, cli := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resp, err := cli.Txn(ctx, req)
			cancel()

			if err != nil {
				lastErr = err
				continue
			}

			if resp.Err == "" {
				return resp, nil
			}

			lastErr = errors.New(resp.Err)
		}

		time.Sleep(30 * time.Millisecond)
	}

	if lastErr == nil {
		lastErr = errors.New("txn timed out without a concrete error")
	}
	return nil, lastErr
}
