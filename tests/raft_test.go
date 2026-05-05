package tests

import (
	"bytes"
	"context"
	"testing"
	"time"

	kv "etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/raft"
	"etcd-KV/internal/server/kvserver"
	"etcd-KV/internal/storage/mvcc"
	"etcd-KV/internal/storage/persister"
)

type singleNodeDiskKV struct {
	raft   *raft.Raft
	server *kvserver.Server
}

func TestSingleNodeRestartsFromDiskPersister(t *testing.T) {
	dir := t.TempDir()

	node := startSingleNodeDiskKV(t, dir)
	waitForSingleNodeLeader(t, node.raft, 2*time.Second)

	putCtx, putCancel := context.WithTimeout(context.Background(), 2*time.Second)
	putResp, err := node.server.Put(putCtx, &kv.PutRequest{
		Key:      "restart/key",
		Value:    []byte("persisted-value"),
		ClientID: 1001,
		Seq:      1,
	})
	putCancel()
	if err != nil {
		t.Fatalf("Put before restart returned error: %v", err)
	}
	if putResp.Revision == 0 {
		t.Fatal("Put before restart returned zero revision")
	}

	node.Close()
	time.Sleep(200 * time.Millisecond)

	restarted := startSingleNodeDiskKV(t, dir)
	defer restarted.Close()
	waitForSingleNodeLeader(t, restarted.raft, 2*time.Second)

	waitForDirectValue(t, restarted.server, "restart/key", []byte("persisted-value"), 3*time.Second)
}

func startSingleNodeDiskKV(t *testing.T, dir string) *singleNodeDiskKV {
	t.Helper()

	ps, err := persister.MakeDiskPersister(dir)
	if err != nil {
		t.Fatalf("MakeDiskPersister() error = %v", err)
	}

	applyCh := make(chan raft.ApplyMsg, 128)
	rf := raft.Make([]raft.Peer{nil}, 0, ps, applyCh)
	store := mvcc.NewKVStoreWithOptions(mvcc.StoreOptions{
		LeaseExpireMode: mvcc.LeaseExpireExternal,
	})
	server := kvserver.NewServer(0, rf, store, applyCh)

	return &singleNodeDiskKV{
		raft:   rf,
		server: server,
	}
}

func (n *singleNodeDiskKV) Close() {
	if n.raft != nil {
		n.raft.Kill()
	}
}

func waitForSingleNodeLeader(t *testing.T, rf *raft.Raft, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, isLeader := rf.GetState(); isLeader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("timed out waiting for single-node raft leader")
}

func waitForDirectValue(t *testing.T, server *kvserver.Server, key string, want []byte, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var seq int64

	for time.Now().Before(deadline) {
		seq++
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		resp, err := server.Get(ctx, &kv.GetRequest{
			Key:      key,
			ClientID: 2002,
			Seq:      seq,
		})
		cancel()

		if err == nil && resp.Found && bytes.Equal(resp.Value, want) {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for key %q to recover with value %q", key, want)
}
