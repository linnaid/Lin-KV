// 把 raft 和 snapshot 分别持久化到 wal/ 与 snapshot/ 目录
package persister

import (
	"bytes"
	"encoding/binary"
	"errors"
	"etcd-KV/internal/storage/snapshot"
	"etcd-KV/internal/storage/wal"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	diskMagic      = "RPS1"
	diskFileName   = "raft.persist"
	diskHeaderSize = 20 // 4字节maigc + 8字节raft长度 + 8字节snapshot长度
)

type DiskPersister struct {
	mu        sync.Mutex
	dir       string
	path      string

	walStore *wal.WAL
	snapshotStore *snapshot.Store

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

	snapshotStore, err := snapshot.OpenStore(filepath.Join(dir, "snapshot"))
	if err != nil {
		return nil, err
	}

	ps := &DiskPersister{
		dir:  dir,
		path: filepath.Join(dir, diskFileName),
		walStore: walStore,
		snapshotStore: snapshotStore,
	}

	if err := ps.loadFromDisk(); err != nil {
		return nil, err
	}

	return ps, nil
}

// 读取旧状态
func (ps *DiskPersister) loadFromDisk() error {
	raftstate, err := ps.walStore.Load()
	if err != nil {
		return err
	}

	snapshot, err := ps.snapshotStore.Load()
	if err != nil {
		return err
	}

	if len(raftstate) == 0 && len(snapshot) == 0 {
		if err := ps.migrateLegacyFile(); err != nil {
			return err
		}

		snapshot, err = ps.snapshotStore.Load()
		if err != nil {
			return err
		}

		raftstate, err = ps.walStore.Load()
		if err != nil {
			return err
		}
	}

	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)

	return nil
}

// 把旧版单文件格式搬到 wal/ + snapshot/
func (ps *DiskPersister) migrateLegacyFile() error {
	data, err := os.ReadFile(ps.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	raftstate, snapshot, err := decodePersistFile(data)
	if err != nil {
		return err
	}

	if err := ps.snapshotStore.Save(snapshot); err != nil {
		return err
	}
	if err := ps.walStore.Save(raftstate); err != nil {
		return err
	}

	return nil
}

// raft 层主链路里真正调用的持久化入口
func (ps *DiskPersister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	newRaft := clone(raftstate)
	newSnap := clone(snapshot)

	if !bytes.Equal(newSnap, ps.snapshot) {
		if err := ps.snapshotStore.Save(newSnap); err != nil {
			panic(err)
		}
	}
	
	// data := encodePersistFile(newRaft, newSnap)

	// if err := writeFileAtomic(ps.dir, ps.path, data); err != nil {
	// 	panic(err)
	// }

	if err := ps.walStore.Save(newRaft); err != nil {
		panic(err)
	}

	ps.raftstate = newRaft
	ps.snapshot = newSnap
}

// func encodePersistFile(raftstate []byte, snapshot []byte) []byte {
// 	raftCopy := clone(raftstate)
// 	snapCopy := clone(snapshot)

// 	total := diskHeaderSize + len(raftCopy) + len(snapCopy)
// 	buf := make([]byte, total)

// 	copy(buf[0:4], []byte(diskMagic))
// 	binary.BigEndian.PutUint64(buf[4:12], uint64(len(raftCopy)))
// 	binary.BigEndian.PutUint64(buf[12:20], uint64(len(snapCopy)))

// 	copy(buf[20:20+len(raftCopy)], raftCopy)
// 	copy(buf[20+len(raftCopy):], snapCopy)

// 	return buf
// }

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

// func writeFileAtomic(dir string, path string, data []byte) error {
// 	tmp, err := os.CreateTemp(dir, ".raft.persist-*.tmp")

// 	if err != nil {
// 		return err
// 	}

// 	tmpName := tmp.Name()

// 	defer func() { _ = os.Remove(tmpName) }()

// 	if _, err := tmp.Write(data); err != nil {
// 		_ = tmp.Close()
// 		return err
// 	}

// 	// 强制把文件内容刷到磁盘，而不是留在页缓存里
// 	if err := tmp.Sync(); err != nil {
// 		_ = tmp.Close()
// 		return err
// 	}

// 	if err := tmp.Close(); err != nil {
// 		return err
// 	}

// 	if err := os.Rename(tmpName, path); err != nil {
// 		return err
// 	}

// 	return syncDir(dir)
// }

// // 把目录项刷盘
// func syncDir(dir string) error {
// 	f, err := os.Open(dir)
// 	if err != nil {
// 		return err
// 	}

// 	defer f.Close()

// 	return f.Sync()
// }


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
