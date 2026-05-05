package tests

import (
	"bytes"
	"testing"

	"etcd-KV/internal/storage/mvcc"
)

func TestMVCCPutGetDeletePreservesHistory(t *testing.T) {
	store := mvcc.NewKVStore()

	rev1 := mustPut(t, store, "alpha", []byte("v1"), 0)
	rev2 := mustPut(t, store, "alpha", []byte("v2"), 0)

	got, rev, ok := store.Get("alpha", 0)
	if !ok {
		t.Fatal("expected latest value to exist")
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("latest value = %q, want %q", got, []byte("v2"))
	}
	if rev != rev2.Main {
		t.Fatalf("latest revision = %d, want %d", rev, rev2.Main)
	}

	history, histRev, ok := store.Get("alpha", rev1.Main)
	if !ok {
		t.Fatal("expected historical value to exist")
	}
	if !bytes.Equal(history, []byte("v1")) {
		t.Fatalf("historical value = %q, want %q", history, []byte("v1"))
	}
	if histRev != rev1.Main {
		t.Fatalf("historical revision = %d, want %d", histRev, rev1.Main)
	}

	delRev := store.Delete("alpha")
	if delRev.Main != rev2.Main+1 {
		t.Fatalf("delete revision = %d, want %d", delRev.Main, rev2.Main+1)
	}

	if _, _, ok := store.Get("alpha", 0); ok {
		t.Fatal("expected deleted key to be absent at latest revision")
	}

	oldValue, _, ok := store.Get("alpha", rev2.Main)
	if !ok {
		t.Fatal("expected old revision to remain readable after delete")
	}
	if !bytes.Equal(oldValue, []byte("v2")) {
		t.Fatalf("value before delete = %q, want %q", oldValue, []byte("v2"))
	}
}
