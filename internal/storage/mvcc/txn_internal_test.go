package mvcc

import (
	"testing"
	"time"
)

func TestDeleteDetachesKeyFromLease(t *testing.T) {
	store := NewKVStore()

	leaseID := store.LeaseGrant(10)
	key := "lease/delete"

	mustPut(t, store, key, []byte("value"), leaseID)

	before := historyLen(t, store, key)
	store.Delete(key)

	store.mu.RLock()
	_, keyLeaseExists := store.keyLease[key]
	_, leaseKeyExists := store.leaseMgr.leases[leaseID].Keys[key]
	store.mu.RUnlock()

	if keyLeaseExists {
		t.Fatalf("expected keyLease entry for %q to be removed", key)
	}
	if leaseKeyExists {
		t.Fatalf("expected lease %d to stop tracking %q after delete", leaseID, key)
	}

	if err := store.LeaseRevoke(leaseID); err != nil {
		t.Fatalf("LeaseRevoke returned error: %v", err)
	}

	got := historyLen(t, store, key)
	if got != before+1 {
		t.Fatalf("delete history len = %d, want %d", got, before+1)
	}
}

func TestPutWithoutLeaseDetachesOldLease(t *testing.T) {
	store := NewKVStore()

	leaseID := store.LeaseGrant(10)
	key := "lease/overwrite"

	mustPut(t, store, key, []byte("v1"), leaseID)
	mustPut(t, store, key, []byte("v2"), 0)

	store.mu.RLock()
	_, keyLeaseExists := store.keyLease[key]
	_, leaseKeyExists := store.leaseMgr.leases[leaseID].Keys[key]
	store.mu.RUnlock()

	if keyLeaseExists {
		t.Fatalf("expected overwrite without lease to detach %q from keyLease", key)
	}
	if leaseKeyExists {
		t.Fatalf("expected overwrite without lease to detach %q from lease %d", key, leaseID)
	}
}

func TestPutRebindsKeyToNewLease(t *testing.T) {
	store := NewKVStore()

	lease1 := store.LeaseGrant(10)
	lease2 := store.LeaseGrant(10)
	key := "lease/rebind"

	mustPut(t, store, key, []byte("v1"), lease1)
	mustPut(t, store, key, []byte("v2"), lease2)

	store.mu.RLock()
	gotLeaseID := store.keyLease[key]
	_, oldLeaseHasKey := store.leaseMgr.leases[lease1].Keys[key]
	_, newLeaseHasKey := store.leaseMgr.leases[lease2].Keys[key]
	store.mu.RUnlock()

	if gotLeaseID != lease2 {
		t.Fatalf("keyLease[%q] = %d, want %d", key, gotLeaseID, lease2)
	}
	if oldLeaseHasKey {
		t.Fatalf("expected old lease %d to stop tracking %q", lease1, key)
	}
	if !newLeaseHasKey {
		t.Fatalf("expected new lease %d to track %q", lease2, key)
	}
}

func TestTxnPutWithMissingLeaseDoesNotPersistKey(t *testing.T) {
	store := NewKVStore()

	_, _, err := store.Txn(Txn{
		ThenOps: []Operation{{
			Type:    OpPut,
			Key:     "txn/missing-lease",
			Value:   []byte("value"),
			LeaseID: 12345,
		}},
	})
	if err == nil {
		t.Fatal("expected txn put with missing lease to fail")
	}

	if _, _, ok := store.Get("txn/missing-lease", 0); ok {
		t.Fatal("expected failed txn put to leave key absent")
	}
}

func TestTxnDeleteDetachesKeyFromLease(t *testing.T) {
	store := NewKVStore()

	leaseID := store.LeaseGrant(10)
	key := "txn/delete"

	mustPut(t, store, key, []byte("value"), leaseID)

	if _, _, err := store.Txn(Txn{
		ThenOps: []Operation{{
			Type: OpDelete,
			Key:  key,
		}},
	}); err != nil {
		t.Fatalf("Txn delete returned error: %v", err)
	}

	store.mu.RLock()
	_, keyLeaseExists := store.keyLease[key]
	_, leaseKeyExists := store.leaseMgr.leases[leaseID].Keys[key]
	store.mu.RUnlock()

	if keyLeaseExists {
		t.Fatalf("expected txn delete to remove keyLease entry for %q", key)
	}
	if leaseKeyExists {
		t.Fatalf("expected txn delete to remove %q from lease %d", key, leaseID)
	}
}

func TestTxnPutWithLeaseExpires(t *testing.T) {
	store := NewKVStore()

	leaseID := store.LeaseGrant(1)

	succeeded, _, err := store.Txn(Txn{
		ThenOps: []Operation{{
			Type:    OpPut,
			Key:     "txn/ttl",
			Value:   []byte("value"),
			LeaseID: leaseID,
		}},
	})
	if err != nil {
		t.Fatalf("Txn put with lease returned error: %v", err)
	}
	if !succeeded {
		t.Fatal("expected txn put with lease to succeed")
	}

	waitUntil(t, 2500*time.Millisecond, func() bool {
		_, _, ok := store.Get("txn/ttl", 0)
		return !ok
	})
}
