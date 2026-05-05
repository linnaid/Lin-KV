package tests

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"etcd-KV/internal/storage/mvcc"
)

func waitForWatchEvent(t *testing.T, ch <-chan mvcc.Event) mvcc.Event {
	t.Helper()

	select {
	case ev := <-ch:
		return ev
	case <-time.After(800 * time.Millisecond):
		t.Fatal("timed out waiting for watch event")
		return mvcc.Event{}
	}
}

func TestWatchReceivesBacklogAndLiveEvents(t *testing.T) {
	store := mvcc.NewKVStore()
	rev1 := mustPut(t, store, "watch/key", []byte("v1"), 0)

	ch, id, err := store.Watch("watch/key", rev1.Main, false)
	if err != nil {
		t.Fatalf("Watch returned error: %v", err)
	}
	defer store.CancelWatcher(id)

	backlog := waitForWatchEvent(t, ch)
	if backlog.Type != mvcc.EventPut {
		t.Fatalf("backlog type = %v, want %v", backlog.Type, mvcc.EventPut)
	}
	if backlog.Key != "watch/key" {
		t.Fatalf("backlog key = %q, want %q", backlog.Key, "watch/key")
	}
	if backlog.Rev.Main != rev1.Main {
		t.Fatalf("backlog revision = %d, want %d", backlog.Rev.Main, rev1.Main)
	}
	if !bytes.Equal(backlog.Value, []byte("v1")) {
		t.Fatalf("backlog value = %q, want %q", backlog.Value, []byte("v1"))
	}

	rev2 := mustPut(t, store, "watch/key", []byte("v2"), 0)
	live := waitForWatchEvent(t, ch)
	if live.Rev.Main != rev2.Main {
		t.Fatalf("live revision = %d, want %d", live.Rev.Main, rev2.Main)
	}
	if !bytes.Equal(live.Value, []byte("v2")) {
		t.Fatalf("live value = %q, want %q", live.Value, []byte("v2"))
	}
}

func TestPrefixWatchFiltersByPrefix(t *testing.T) {
	store := mvcc.NewKVStore()
	rev1 := mustPut(t, store, "svc/a", []byte("a1"), 0)
	mustPut(t, store, "other/x", []byte("x1"), 0)

	ch, id, err := store.Watch("svc/", rev1.Main, true)
	if err != nil {
		t.Fatalf("Watch returned error: %v", err)
	}
	defer store.CancelWatcher(id)

	first := waitForWatchEvent(t, ch)
	if first.Key != "svc/a" {
		t.Fatalf("first prefix event key = %q, want %q", first.Key, "svc/a")
	}

	mustPut(t, store, "other/y", []byte("y1"), 0)
	rev4 := mustPut(t, store, "svc/b", []byte("b1"), 0)

	next := waitForWatchEvent(t, ch)
	if next.Key != "svc/b" {
		t.Fatalf("second prefix event key = %q, want %q", next.Key, "svc/b")
	}
	if next.Rev.Main != rev4.Main {
		t.Fatalf("second prefix event revision = %d, want %d", next.Rev.Main, rev4.Main)
	}
}

func TestWatchRejectsCompactedRevision(t *testing.T) {
	store := mvcc.NewKVStore()
	rev1 := mustPut(t, store, "watch/key", []byte("v1"), 0)
	mustPut(t, store, "watch/key", []byte("v2"), 0)

	if err := store.Compact(rev1.Main); err != nil {
		t.Fatalf("Compact returned error: %v", err)
	}

	_, _, err := store.Watch("watch/key", rev1.Main, false)
	if !errors.Is(err, mvcc.ErrCompacted) {
		t.Fatalf("Watch error = %v, want %v", err, mvcc.ErrCompacted)
	}
}
