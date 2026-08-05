//go:build !cgo || !lindb

package mvcc

import "fmt"

type LinDBBackend struct{}

func OpenLinDBBackend(path string) (*LinDBBackend, error) {
	return nil, fmt.Errorf("lin-db backend requires CGO_ENABLED=1 and -tags lindb")
}

func (backend *LinDBBackend) Close() error { return nil }

func (backend *LinDBBackend) CurrentRev() int64 { return 0 }

func (backend *LinDBBackend) SetCurrentRev(rev int64) {}

func (backend *LinDBBackend) CompactRev() int64 { return 0 }

func (backend *LinDBBackend) SetCompactRev(rev int64) {}

func (backend *LinDBBackend) GetRevisions(key string) ([]ValueRevision, bool) { return nil, false }

func (backend *LinDBBackend) SetRevisions(key string, revisions []ValueRevision) {}

func (backend *LinDBBackend) RangeKeys(startKey, endKey string) []string { return nil }

func (backend *LinDBBackend) Events() []Event { return nil }

func (backend *LinDBBackend) SetEvents(events []Event) {}

func (backend *LinDBBackend) AppendEvent(event Event) {}

func (backend *LinDBBackend) SnapshotState() BackendState { return BackendState{} }

func (backend *LinDBBackend) ReplaceState(state BackendState) {}
