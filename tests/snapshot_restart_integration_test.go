//go:build integration

package tests

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"etcd-KV/internal/storage/persister"
)

func TestIntegrationSnapshotRestartRestoresClusterAndAllowsNewWrites(t *testing.T) {
	h := startRestartableKVGRPCHarness(t, 3)
	defer h.Close()

	waitForHarnessLeader(t, h, 5*time.Second)

	const putClientID = int64(7101)
	var putSeq int64
	nextPutSeq := func() int64 {
		putSeq++
		return putSeq
	}

	written := make(map[string][]byte)
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("restart/snapshot/%02d", i)
		value := bytes.Repeat([]byte{byte('a' + i)}, 64)

		if _, err := putViaAnyNode(
			h.kvClients,
			key,
			value,
			putClientID,
			nextPutSeq(),
			5*time.Second,
		); err != nil {
			t.Fatalf("Put before snapshot failed at step %d: %v", i, err)
		}

		written[key] = append([]byte(nil), value...)
	}

	waitForSnapshotsOnAllNodes(t, h.dataDirs, 5*time.Second)

	h.stopAllNodes()
	time.Sleep(300 * time.Millisecond)
	h.restartAllNodes(t)

	waitForHarnessLeader(t, h, 5*time.Second)

	readClientID := int64(7200)
	for key, want := range written {
		readClientID++
		waitForValueViaAnyNode(t, h.kvClients, key, want, readClientID, 5*time.Second)
	}

	finalKey := "restart/snapshot/after-restart"
	finalValue := []byte("cluster-alive-after-snapshot-restart")
	if _, err := putViaAnyNode(
		h.kvClients,
		finalKey,
		finalValue,
		putClientID,
		nextPutSeq(),
		8*time.Second,
	); err != nil {
		t.Fatalf("Put after snapshot restart failed: %v", err)
	}

	waitForValueViaAnyNode(t, h.kvClients, finalKey, finalValue, 7301, 5*time.Second)
}

func waitForSnapshotsOnAllNodes(t *testing.T, dataDirs []string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allReady := true

		for i, dir := range dataDirs {
			ps, err := persister.MakeDiskPersister(dir)
			if err != nil {
				t.Fatalf("MakeDiskPersister(node=%d) error = %v", i, err)
			}

			if len(ps.ReadSnapshot()) == 0 {
				allReady = false
				break
			}
		}

		if allReady {
			return
		}

		time.Sleep(30 * time.Millisecond)
	}

	t.Fatal("timed out waiting for snapshots to persist on all nodes")
}
