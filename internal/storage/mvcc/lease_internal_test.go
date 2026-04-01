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

func TestLeaseRevokeDeletesAttachedKey(t *testing.T) {
	store := NewKVStore()

	leaseID := store.leaseMgr.LeaseGrant(10)
	store.Put("lease/key", []byte("value"), leaseID)

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
	store.Put("lease/ttl", []byte("value"), leaseID)

	waitUntil(t, 2500*time.Millisecond, func() bool {
		_, _, ok := store.Get("lease/ttl", 0)
		return !ok
	})
}
