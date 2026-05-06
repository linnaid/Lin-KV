package mvcc

import (
	"bytes"
	"testing"
	"time"
)

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	t.Fatal("condition was not satisfied before timeout")
}

func historyLen(t *testing.T, store *KVStore, key string) int {
	t.Helper()

	store.mu.RLock()
	defer store.mu.RUnlock()

	revisions, ok := store.backend.GetRevisions(key)
	if !ok {
		return 0
	}

	return len(revisions)
}

func mustPut(t *testing.T, store *KVStore, key string, value []byte, leaseID int64) Revision {
	t.Helper()

	rev, err := store.Put(key, value, leaseID)
	if err != nil {
		t.Fatalf("Put(%q) returned error: %v", key, err)
	}

	return rev
}

func TestLeaseRevokeDeletesAttachedKey(t *testing.T) {
	store := NewKVStore()

	leaseID := store.leaseMgr.LeaseGrant(10)
	mustPut(t, store, "lease/key", []byte("value"), leaseID)

	got, _, ok := store.Get("lease/key", 0)
	if !ok {
		t.Fatal("expected leased key to exist before revoke")
	}
	if !bytes.Equal(got, []byte("value")) {
		t.Fatalf("value before revoke = %q, want %q", got, []byte("value"))
	}

	if err := store.leaseMgr.LeaseRevoke(leaseID); err != nil {
		t.Fatalf("LeaseRevoke returned error: %v", err)
	}

	if _, _, ok := store.Get("lease/key", 0); ok {
		t.Fatal("expected leased key to be removed after revoke")
	}
}

func TestLeaseExpirationDeletesAttachedKey(t *testing.T) {
	store := NewKVStore()

	leaseID := store.leaseMgr.LeaseGrant(1)
	mustPut(t, store, "lease/ttl", []byte("value"), leaseID)

	waitUntil(t, 2500*time.Millisecond, func() bool {
		_, _, ok := store.Get("lease/ttl", 0)
		return !ok
	})
}

func TestPutWithMissingLeaseDoesNotPersistKey(t *testing.T) {
	store := NewKVStore()

	if _, err := store.Put("lease/missing", []byte("value"), 12345); err == nil {
		t.Fatal("expected Put with missing lease to fail")
	}

	if _, _, ok := store.Get("lease/missing", 0); ok {
		t.Fatal("expected key to remain absent after failed Put")
	}
}

func TestExternalLeaseModeDoesNotStartLocalExpirationLoop(t *testing.T) {
	store := NewKVStoreWithOptions(StoreOptions{
		LeaseExpireMode: LeaseExpireExternal,
	})

	leaseID := store.leaseMgr.LeaseGrant(1)
	mustPut(t, store, "lease/external", []byte("value"), leaseID)

	time.Sleep(1500 * time.Millisecond)

	got, _, ok := store.Get("lease/external", 0)
	if !ok {
		t.Fatal("expected key to survive without external lease reaper")
	}
	if !bytes.Equal(got, []byte("value")) {
		t.Fatalf("value after waiting = %q, want %q", got, []byte("value"))
	}
}
