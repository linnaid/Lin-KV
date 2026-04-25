// 把 raft 和 snapshot 分别持久化到 wal/ 与 snapshot/ 目录
package persister

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	snapshotstore "etcd-KV/internal/storage/snapshot"
	"etcd-KV/internal/storage/wal"
)

const (
	diskMagic      = "RPS1"
	diskFileName   = "raft.persist"
	diskHeaderSize = 20 // 4字节 magic + 8字节 raft 长度 + 8字节 snapshot 长度
)

// 磁盘持久化器
type DiskPersister struct {
	mu         sync.Mutex
	dir        string
	legacypath string // 指向旧版 raft.persist 文件路径

	walStore      *wal.WAL
	snapshotStore *snapshotstore.Store

	raftSlot     int
	snapshotSlot int

	raftstate []byte
	snapshot  []byte
}

func MakeDiskPersister(dir string) (*DiskPersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	walStore, err := wal.OpenWAL(filepath.Join(dir, "wal"))
	if err != nil {
		return nil, err
	}

	snapshotStore, err := snapshotstore.OpenStore(filepath.Join(dir, "snapshot"))
	if err != nil {
		return nil, err
	}

	ps := &DiskPersister{
		dir:           dir,
		legacypath:    filepath.Join(dir, diskFileName),
		walStore:      walStore,
		snapshotStore: snapshotStore,
		raftSlot:      0,
		snapshotSlot:  noSnapshotSlot,
	}

	if err := ps.loadFromDisk(); err != nil {
		return nil, err
	}

	return ps, nil
}

// 读取旧状态
func (ps *DiskPersister) loadFromDisk() error {
	m, ok, err := loadManifest(ps.dir)
	if err != nil {
		return err
	}

	if ok {
		return ps.loadSlots(m)
	}

	// manifest 不存在，尝试迁移旧版raft.persist
	return ps.migrateLegacyFile()
}

func (ps *DiskPersister) loadSlots(m slotManifest) error {
	raftstate, err := ps.walStore.LoadSlot(m.RaftSlot)
	if err != nil {
		return err
	}

	var snapshot []byte
	if m.SnapshotSlot != noSnapshotSlot {
		snapshot, err = ps.snapshotStore.LoadSlot(m.SnapshotSlot)
		if err != nil {
			return err
		}
	}

	ps.raftSlot = m.RaftSlot
	ps.snapshotSlot = m.SnapshotSlot
	ps.snapshot = clone(snapshot)
	ps.raftstate = clone(raftstate)

	return nil
}

// 把旧版单文件格式搬到 wal/ + snapshot/
func (ps *DiskPersister) migrateLegacyFile() error {
	data, err := os.ReadFile(ps.legacypath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			ps.snapshotSlot = noSnapshotSlot
			ps.raftSlot = 0
			ps.raftstate = nil
			ps.snapshot = nil
			return nil
		}
		return err
	}

	raftstate, snapshot, err := decodePersistFile(data)
	if err != nil {
		return err
	}

	raftSlot := 0
	snapshotSlot := noSnapshotSlot
	if len(snapshot) > 0 {
		snapshotSlot = 0
		if err := ps.snapshotStore.SaveSlot(snapshotSlot, snapshot); err != nil {
			return err
		}
	}
	if err := ps.walStore.SaveSlot(raftSlot, raftstate); err != nil {
		return err
	}

	if err := saveManifest(ps.dir, slotManifest{
		RaftSlot:     raftSlot,
		SnapshotSlot: snapshotSlot,
	}); err != nil {
		return err
	}

	ps.raftSlot = raftSlot
	ps.raftstate = clone(raftstate)
	ps.snapshotSlot = snapshotSlot
	ps.snapshot = clone(snapshot)

	return nil
}

// 双槽位切换
func nextSlot(current int) int {
	if current == 0 {
		return 1
	}
	return 0
}

// raft 层主链路里真正调用的持久化入口
func (ps *DiskPersister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	newRaft := clone(raftstate)
	newSnap := clone(snapshot)

	newRaftSlot := nextSlot(ps.raftSlot)
	newSnapshotSlot := ps.snapshotSlot

	if len(newSnap) == 0 {
		newSnapshotSlot = noSnapshotSlot
	}

	if len(newSnap) > 0 && (ps.snapshotSlot == noSnapshotSlot || !bytes.Equal(newSnap, ps.snapshot)) {
		newSnapshotSlot = nextSlot(ps.snapshotSlot)

		if err := ps.snapshotStore.SaveSlot(newSnapshotSlot, newSnap); err != nil {
			panic(err)
		}
	}

	if err := ps.walStore.SaveSlot(newRaftSlot, newRaft); err != nil {
		panic(err)
	}

	if err := saveManifest(ps.dir, slotManifest{
		RaftSlot:     newRaftSlot,
		SnapshotSlot: newSnapshotSlot,
	}); err != nil {
		panic(err)
	}

	ps.raftSlot = newRaftSlot
	ps.raftstate = newRaft
	ps.snapshotSlot = newSnapshotSlot
	ps.snapshot = newSnap
}

func decodePersistFile(data []byte) ([]byte, []byte, error) {
	if len(data) == 0 {
		return nil, nil, nil
	}

	if len(data) < diskHeaderSize {
		return nil, nil, fmt.Errorf("insufficient data length: got %d bytes", len(data))
	}
	if string(data[0:4]) != diskMagic {
		return nil, nil, fmt.Errorf("invalid magic header: %q", string(data[0:4]))
	}

	raftLen := binary.BigEndian.Uint64(data[4:12])
	snapLen := binary.BigEndian.Uint64(data[12:20])
	payloadLen := uint64(len(data) - diskHeaderSize)

	if raftLen > payloadLen {
		return nil, nil, fmt.Errorf("persist file size mismatch: raftLen=%d payload=%d", raftLen, payloadLen)
	}
	if snapLen != payloadLen-raftLen {
		return nil, nil, fmt.Errorf("persist file size mismatch: snapLen=%d remaining=%d", snapLen, payloadLen-raftLen)
	}

	raftBegin := diskHeaderSize
	raftEnd := raftBegin + int(raftLen)

	snapBegin := raftEnd
	snapEnd := snapBegin + int(snapLen)

	raftstate := clone(data[raftBegin:raftEnd])
	snapshot := clone(data[snapBegin:snapEnd])

	return raftstate, snapshot, nil
}

func (ps *DiskPersister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return clone(ps.raftstate)
}

func (ps *DiskPersister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return len(ps.raftstate)
}

func (ps *DiskPersister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return clone(ps.snapshot)
}

func (ps *DiskPersister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return len(ps.snapshot)
}
