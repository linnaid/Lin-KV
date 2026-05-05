package tests

import (
	"bytes"
	"testing"

	"etcd-KV/internal/storage/mvcc"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	store := mvcc.NewKVStore()

	rev1 := mustPut(t, store, "alpha", []byte("one"), 0)
	mustPut(t, store, "beta", []byte("two"), 0)
	rev3 := mustPut(t, store, "alpha", []byte("three"), 0)

	snapshot := store.Snapshot()

	restored := mvcc.NewKVStore()
	restored.Restore(snapshot)

	gotAlpha, rev, ok := restored.Get("alpha", 0)
	if !ok {
		t.Fatal("expected alpha to exist after restore")
	}
	if !bytes.Equal(gotAlpha, []byte("three")) {
		t.Fatalf("alpha after restore = %q, want %q", gotAlpha, []byte("three"))
	}
	if rev != rev3.Main {
		t.Fatalf("restore revision = %d, want %d", rev, rev3.Main)
	}

	historical, _, ok := restored.Get("alpha", rev1.Main)
	if !ok {
		t.Fatal("expected historical alpha to exist after restore")
	}
	if !bytes.Equal(historical, []byte("one")) {
		t.Fatalf("historical alpha = %q, want %q", historical, []byte("one"))
	}

	gotBeta, _, ok := restored.Get("beta", 0)
	if !ok {
		t.Fatal("expected beta to exist after restore")
	}
	if !bytes.Equal(gotBeta, []byte("two")) {
		t.Fatalf("beta after restore = %q, want %q", gotBeta, []byte("two"))
	}
}
