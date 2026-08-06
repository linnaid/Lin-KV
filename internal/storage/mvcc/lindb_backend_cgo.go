//go:build cgo && lindb

package mvcc

/*
#cgo CFLAGS: -I/home/linnaid/lin-DB/Lin-DB/include
#cgo LDFLAGS: -L/home/linnaid/lin-DB/Lin-DB/build -Wl,-rpath,/home/linnaid/lin-DB/Lin-DB/build -llindb_core -lstdc++ -lsnappy -lzstd -pthread
#include <stdlib.h>
#include <lindb/c.h>
*/
import "C"

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"unsafe"

	mvccpb "etcd-KV/internal/pb/mvcc"

	"google.golang.org/protobuf/proto"
)

const (
	metaAppliedIndex = "meta/applied_index"
	metaAppliedTerm = "meta/applied_term"
	metaCurrentRev = "meta/current_rev"
	metaCompactRev = "meta/compact_rev"
	dataPrefix     = "data/"
	eventPrefix    = "event/"
)

var _ Backend = (*LinDBBackend)(nil)

type LinDBBackend struct {
	mu           sync.Mutex
	db           *C.lindb_t
	readOptions  *C.lindb_readoptions_t
	writeOptions *C.lindb_writeoptions_t
}

func OpenLinDBBackend(path string) (*LinDBBackend, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("create lindb directory %q: %w", path, err)
	}

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	options := C.lindb_options_create()
	defer C.lindb_options_destroy(options)

	C.lindb_options_set_create_if_missing(options, 1)
	C.lindb_options_set_compression(options, C.lindb_no_compression)

	var errorPointer *C.char
	database := C.lindb_open(options, cpath, &errorPointer)
	if err := lindbError(errorPointer); err != nil {
		return nil, err
	}
	if database == nil {
		return nil, errors.New("open Lin-DB returned nil")
	}

	return &LinDBBackend{
		db:           database,
		readOptions:  C.lindb_readoptions_create(),
		writeOptions: C.lindb_writeoptions_create(),
	}, nil
}

func (backend *LinDBBackend) Close() error {
	if backend == nil {
		return nil
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	if backend.readOptions != nil {
		C.lindb_readoptions_destroy(backend.readOptions)
		backend.readOptions = nil
	}
	if backend.writeOptions != nil {
		C.lindb_writeoptions_destroy(backend.writeOptions)
		backend.writeOptions = nil
	}
	if backend.db != nil {
		C.lindb_close(backend.db)
		backend.db = nil
	}

	return nil
}

func (backend *LinDBBackend) CurrentRev() int64 {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	return backend.getInt64Locked(metaCurrentRev)
}

func (backend *LinDBBackend) SetCurrentRev(rev int64) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	backend.putRawLocked(metaCurrentRev, encodeInt64(rev))
}

func (backend *LinDBBackend) CompactRev() int64 {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	return backend.getInt64Locked(metaCompactRev)
}

func (backend *LinDBBackend) SetCompactRev(rev int64) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	backend.putRawLocked(metaCompactRev, encodeInt64(rev))
}

func (backend *LinDBBackend) GetRevisions(key string) ([]ValueRevision, bool) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	return backend.getRevisionsLocked(key)
}

func (backend *LinDBBackend) SetRevisions(key string, revisions []ValueRevision) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	storageKey := dataStorageKey(key)
	if len(revisions) == 0 {
		backend.deleteRawLocked(storageKey)
		return
	}

	encoded, err := encodeKeyEntry(key, revisions)
	if err != nil {
		panic(err)
	}
	backend.putRawLocked(storageKey, encoded)
}

func (backend *LinDBBackend) RangeKeys(startKey, endKey string) []string {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	return backend.rangeKeysLocked(startKey, endKey)
}

func (backend *LinDBBackend) Events() []Event {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	return backend.eventsLocked()
}

func (backend *LinDBBackend) SetEvents(events []Event) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	batch := C.lindb_writebatch_create()
	defer C.lindb_writebatch_destroy(batch)

	backend.deletePrefixLocked(batch, eventPrefix)
	backend.batchPutEventsLocked(batch, events)
	backend.writeBatchLocked(batch)
}

func (backend *LinDBBackend) AppendEvent(event Event) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	encoded, err := encodeEvent(event)
	if err != nil {
		panic(err)
	}
	backend.putRawLocked(eventStorageKey(event), encoded)
}

func (backend *LinDBBackend) SnapshotState() BackendState {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	data := make(map[string][]ValueRevision)
	for _, key := range backend.rangeKeysLocked("", "") {
		revisions, ok := backend.getRevisionsLocked(key)
		if ok {
			data[key] = revisions
		}
	}

	return BackendState{
		Data:       data,
		Events:     backend.eventsLocked(),
		CurrentRev: backend.getInt64Locked(metaCurrentRev),
		CompactRev: backend.getInt64Locked(metaCompactRev),
	}
}

func (backend *LinDBBackend) ReplaceState(state BackendState) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	batch := C.lindb_writebatch_create()
	defer C.lindb_writebatch_destroy(batch)

	backend.deletePrefixLocked(batch, dataPrefix)
	backend.deletePrefixLocked(batch, eventPrefix)

	keys := make([]string, 0, len(state.Data))
	for key := range state.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		revisions := state.Data[key]
		if len(revisions) == 0 {
			continue
		}

		encoded, err := encodeKeyEntry(key, revisions)
		if err != nil {
			panic(err)
		}
		batchPut(batch, dataStorageKey(key), encoded)
	}

	backend.batchPutEventsLocked(batch, state.Events)
	batchPut(batch, metaCurrentRev, encodeInt64(state.CurrentRev))
	batchPut(batch, metaCompactRev, encodeInt64(state.CompactRev))
	backend.writeBatchLocked(batch)
}

func (backend *LinDBBackend) getRevisionsLocked(key string) ([]ValueRevision, bool) {
	encoded, ok := backend.getRawLocked(dataStorageKey(key))
	if !ok {
		return nil, false
	}

	revisions, err := decodeKeyEntry(encoded)
	if err != nil {
		panic(err)
	}
	return revisions, true
}

func (backend *LinDBBackend) rangeKeysLocked(startKey, endKey string) []string {
	backend.ensureOpenLocked()

	seekKey := dataPrefix
	if startKey != "" {
		seekKey = dataStorageKey(startKey)
	}

	limitKey := ""
	if endKey != "" {
		limitKey = dataStorageKey(endKey)
	}

	iterator := C.lindb_create_iterator(backend.db, backend.readOptions)
	defer C.lindb_iter_destroy(iterator)

	seekPointer, seekLength := cBytes([]byte(seekKey))
	defer freeCPointer(seekPointer)
	C.lindb_iter_seek(iterator, (*C.char)(seekPointer), seekLength)

	keys := make([]string, 0)
	for C.lindb_iter_valid(iterator) != 0 {
		storageKey := iteratorKey(iterator)
		if !strings.HasPrefix(storageKey, dataPrefix) {
			break
		}
		if limitKey != "" && storageKey >= limitKey {
			break
		}

		decodedKey, err := hex.DecodeString(strings.TrimPrefix(storageKey, dataPrefix))
		if err != nil {
			panic(fmt.Errorf("decode lindb data key %q: %w", storageKey, err))
		}
		keys = append(keys, string(decodedKey))

		C.lindb_iter_next(iterator)
	}
	backend.checkIteratorLocked(iterator)

	return keys
}

func (backend *LinDBBackend) eventsLocked() []Event {
	backend.ensureOpenLocked()

	iterator := C.lindb_create_iterator(backend.db, backend.readOptions)
	defer C.lindb_iter_destroy(iterator)

	seekPointer, seekLength := cBytes([]byte(eventPrefix))
	defer freeCPointer(seekPointer)
	C.lindb_iter_seek(iterator, (*C.char)(seekPointer), seekLength)

	events := make([]Event, 0)
	for C.lindb_iter_valid(iterator) != 0 {
		storageKey := iteratorKey(iterator)
		if !strings.HasPrefix(storageKey, eventPrefix) {
			break
		}

		event, err := decodeEvent(iteratorValue(iterator))
		if err != nil {
			panic(err)
		}
		events = append(events, event)

		C.lindb_iter_next(iterator)
	}
	backend.checkIteratorLocked(iterator)

	sortEvents(events)
	return events
}

func (backend *LinDBBackend) getInt64Locked(key string) int64 {
	encoded, ok := backend.getRawLocked(key)
	if !ok {
		return 0
	}
	if len(encoded) != 8 {
		panic(fmt.Errorf("lindb meta key %q has %d bytes, want 8", key, len(encoded)))
	}
	return int64(binary.BigEndian.Uint64(encoded))
}

func (backend *LinDBBackend) getRawLocked(key string) ([]byte, bool) {
	backend.ensureOpenLocked()

	keyPointer, keyLength := cBytes([]byte(key))
	defer freeCPointer(keyPointer)

	var valueLength C.size_t
	var errorPointer *C.char
	valuePointer := C.lindb_get(
		backend.db,
		backend.readOptions,
		(*C.char)(keyPointer),
		keyLength,
		&valueLength,
		&errorPointer,
	)
	if err := lindbError(errorPointer); err != nil {
		panic(err)
	}
	if valuePointer == nil {
		return nil, false
	}
	defer C.lindb_free(unsafe.Pointer(valuePointer))

	return C.GoBytes(unsafe.Pointer(valuePointer), C.int(valueLength)), true
}

func (backend *LinDBBackend) putRawLocked(key string, value []byte) {
	backend.ensureOpenLocked()

	keyPointer, keyLength := cBytes([]byte(key))
	defer freeCPointer(keyPointer)
	valuePointer, valueLength := cBytes(value)
	defer freeCPointer(valuePointer)

	var errorPointer *C.char
	C.lindb_put(
		backend.db,
		backend.writeOptions,
		(*C.char)(keyPointer),
		keyLength,
		(*C.char)(valuePointer),
		valueLength,
		&errorPointer,
	)
	if err := lindbError(errorPointer); err != nil {
		panic(err)
	}
}

func (backend *LinDBBackend) deleteRawLocked(key string) {
	backend.ensureOpenLocked()

	keyPointer, keyLength := cBytes([]byte(key))
	defer freeCPointer(keyPointer)

	var errorPointer *C.char
	C.lindb_delete(backend.db, backend.writeOptions, (*C.char)(keyPointer), keyLength, &errorPointer)
	if err := lindbError(errorPointer); err != nil {
		panic(err)
	}
}

func (backend *LinDBBackend) deletePrefixLocked(batch *C.lindb_writebatch_t, prefix string) {
	backend.ensureOpenLocked()

	iterator := C.lindb_create_iterator(backend.db, backend.readOptions)
	defer C.lindb_iter_destroy(iterator)

	seekPointer, seekLength := cBytes([]byte(prefix))
	defer freeCPointer(seekPointer)
	C.lindb_iter_seek(iterator, (*C.char)(seekPointer), seekLength)

	for C.lindb_iter_valid(iterator) != 0 {
		storageKey := iteratorKey(iterator)
		if !strings.HasPrefix(storageKey, prefix) {
			break
		}

		batchDelete(batch, storageKey)
		C.lindb_iter_next(iterator)
	}
	backend.checkIteratorLocked(iterator)
}

func (backend *LinDBBackend) batchPutEventsLocked(batch *C.lindb_writebatch_t, events []Event) {
	clonedEvents := cloneEvents(events)
	sortEvents(clonedEvents)

	for _, event := range clonedEvents {
		encoded, err := encodeEvent(event)
		if err != nil {
			panic(err)
		}
		batchPut(batch, eventStorageKey(event), encoded)
	}
}

func (backend *LinDBBackend) writeBatchLocked(batch *C.lindb_writebatch_t) {
	backend.ensureOpenLocked()

	var errorPointer *C.char
	C.lindb_write(backend.db, backend.writeOptions, batch, &errorPointer)
	if err := lindbError(errorPointer); err != nil {
		panic(err)
	}
}

func (backend *LinDBBackend) checkIteratorLocked(iterator *C.lindb_iterator_t) {
	var errorPointer *C.char
	C.lindb_iter_get_error(iterator, &errorPointer)
	if err := lindbError(errorPointer); err != nil {
		panic(err)
	}
}

func (backend *LinDBBackend) ensureOpenLocked() {
	if backend == nil || backend.db == nil {
		panic("lindb backend is closed")
	}
}

func dataStorageKey(key string) string {
	return dataPrefix + hex.EncodeToString([]byte(key))
}

func eventStorageKey(event Event) string {
	return fmt.Sprintf("%s%020d/%010d", eventPrefix, event.Rev.Main, event.Rev.Sub)
}

func encodeInt64(value int64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, uint64(value))
	return encoded
}

func encodeKeyEntry(key string, revisions []ValueRevision) ([]byte, error) {
	clonedRevisions := cloneValueRevisions(revisions)
	sort.Slice(clonedRevisions, func(leftIndex, rightIndex int) bool {
		leftRev := clonedRevisions[leftIndex].Rev
		rightRev := clonedRevisions[rightIndex].Rev
		if leftRev.Main != rightRev.Main {
			return leftRev.Main < rightRev.Main
		}
		return leftRev.Sub < rightRev.Sub
	})

	entry := &mvccpb.KeyEntry{
		Key:       key,
		Revisions: make([]*mvccpb.ValueRevision, 0, len(clonedRevisions)),
	}
	for _, revision := range clonedRevisions {
		entry.Revisions = append(entry.Revisions, &mvccpb.ValueRevision{
			Rev: &mvccpb.Revision{
				Main: revision.Rev.Main,
				Sub:  revision.Rev.Sub,
			},
			Value:   append([]byte(nil), revision.Value...),
			Deleted: revision.Deleted,
		})
	}

	return proto.Marshal(entry)
}

func decodeKeyEntry(encoded []byte) ([]ValueRevision, error) {
	var entry mvccpb.KeyEntry
	if err := proto.Unmarshal(encoded, &entry); err != nil {
		return nil, fmt.Errorf("decode lindb key entry: %w", err)
	}

	revisions := make([]ValueRevision, 0, len(entry.Revisions))
	for _, revision := range entry.Revisions {
		if revision == nil || revision.Rev == nil {
			continue
		}
		revisions = append(revisions, ValueRevision{
			Rev: Revision{
				Main: revision.Rev.Main,
				Sub:  revision.Rev.Sub,
			},
			Value:   append([]byte(nil), revision.Value...),
			Deleted: revision.Deleted,
		})
	}

	return revisions, nil
}

func encodeEvent(event Event) ([]byte, error) {
	return proto.Marshal(&mvccpb.Event{
		Type: mvccpb.EventType(event.Type),
		Key:  event.Key,
		Rev: &mvccpb.Revision{
			Main: event.Rev.Main,
			Sub:  event.Rev.Sub,
		},
		Value: append([]byte(nil), event.Value...),
	})
}

func decodeEvent(encoded []byte) (Event, error) {
	var event mvccpb.Event
	if err := proto.Unmarshal(encoded, &event); err != nil {
		return Event{}, fmt.Errorf("decode lindb event: %w", err)
	}
	if event.Rev == nil {
		return Event{}, errors.New("decode lindb event: nil revision")
	}

	return Event{
		Type:  EventType(event.Type),
		Key:   event.Key,
		Value: append([]byte(nil), event.Value...),
		Rev: Revision{
			Main: event.Rev.Main,
			Sub:  event.Rev.Sub,
		},
	}, nil
}

func sortEvents(events []Event) {
	sort.Slice(events, func(leftIndex, rightIndex int) bool {
		leftRev := events[leftIndex].Rev
		rightRev := events[rightIndex].Rev
		if leftRev.Main != rightRev.Main {
			return leftRev.Main < rightRev.Main
		}
		return leftRev.Sub < rightRev.Sub
	})
}

func batchPut(batch *C.lindb_writebatch_t, key string, value []byte) {
	keyPointer, keyLength := cBytes([]byte(key))
	defer freeCPointer(keyPointer)
	valuePointer, valueLength := cBytes(value)
	defer freeCPointer(valuePointer)

	C.lindb_writebatch_put(batch, (*C.char)(keyPointer), keyLength, (*C.char)(valuePointer), valueLength)
}

func batchDelete(batch *C.lindb_writebatch_t, key string) {
	keyPointer, keyLength := cBytes([]byte(key))
	defer freeCPointer(keyPointer)

	C.lindb_writebatch_delete(batch, (*C.char)(keyPointer), keyLength)
}

func iteratorKey(iterator *C.lindb_iterator_t) string {
	var keyLength C.size_t
	keyPointer := C.lindb_iter_key(iterator, &keyLength)
	return C.GoStringN(keyPointer, C.int(keyLength))
}

func iteratorValue(iterator *C.lindb_iterator_t) []byte {
	var valueLength C.size_t
	valuePointer := C.lindb_iter_value(iterator, &valueLength)
	return C.GoBytes(unsafe.Pointer(valuePointer), C.int(valueLength))
}

func cBytes(value []byte) (unsafe.Pointer, C.size_t) {
	if len(value) == 0 {
		return nil, 0
	}
	return C.CBytes(value), C.size_t(len(value))
}

func freeCPointer(pointer unsafe.Pointer) {
	if pointer != nil {
		C.free(pointer)
	}
}

func lindbError(errorPointer *C.char) error {
	if errorPointer == nil {
		return nil
	}
	defer C.lindb_free(unsafe.Pointer(errorPointer))

	return errors.New(C.GoString(errorPointer))
}
