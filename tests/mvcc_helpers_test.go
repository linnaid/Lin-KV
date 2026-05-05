package tests

import (
	"testing"

	"etcd-KV/internal/storage/mvcc"
)

func mustPut(t *testing.T, store *mvcc.KVStore, key string, value []byte, leaseID int64) mvcc.Revision {
	t.Helper()

	rev, err := store.Put(key, value, leaseID)
	if err != nil {
		t.Fatalf("Put(%q) returned error: %v", key, err)
	}

	return rev
}
