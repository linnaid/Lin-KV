package persister

import (
	"encoding/binary"
	"testing"
)

func TestDiskPersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()

	ps, err := MakeDiskPersister(dir)
	if err != nil {
		t.Fatalf("MakeDiskPersister() error = %v", err)
	}

	raftState := []byte("raft-state")
	snapshot := []byte("snapshot-data")

	ps.Save(raftState, snapshot)

	reloaded, err := MakeDiskPersister(dir)
	if err != nil {
		t.Fatalf("MakeDiskPersister(reload) error = %v", err)
	}

	if got := string(reloaded.ReadRaftState()); got != string(raftState) {
		t.Fatalf("ReadRaftState() = %q, want %q", got, raftState)
	}

	if got := string(reloaded.ReadSnapshot()); got != string(snapshot) {
		t.Fatalf("ReadSnapshot() = %q, want %q", got, snapshot)
	}
}

func TestDecodePersistFileRejectsInvalidMagic(t *testing.T) {
	data := make([]byte, diskHeaderSize)
	copy(data[:4], []byte("BAD!"))
	binary.BigEndian.PutUint64(data[4:12], 0)
	binary.BigEndian.PutUint64(data[12:20], 0)

	_, _, err := decodePersistFile(data)
	if err == nil {
		t.Fatal("decodePersistFile() accepted invalid magic")
	}
}

func TestDecodePersistFileRejectsTooShortInputWithoutPanicking(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("decodePersistFile() panicked on short input: %v", r)
		}
	}()

	_, _, err := decodePersistFile([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("decodePersistFile() accepted too-short input")
	}
}

func TestDecodePersistFileRejectsOverflowingLengthsWithoutPanicking(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("decodePersistFile() panicked on overflowing lengths: %v", r)
		}
	}()

	data := make([]byte, diskHeaderSize)
	copy(data[:4], []byte(diskMagic))
	binary.BigEndian.PutUint64(data[4:12], uint64(1)<<63)
	binary.BigEndian.PutUint64(data[12:20], uint64(1)<<63)

	_, _, err := decodePersistFile(data)
	if err == nil {
		t.Fatal("decodePersistFile() accepted overflowing lengths")
	}
}
