// 负责 snapshot 文件的编码、解码、原子保存和加载
package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)


type Store struct {
	mu sync.Mutex
	dir string
	// path string
}

func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return &Store{
		dir: dir,
		// path: filepath.Join(dir, snapshotFileName),
	}, nil
}

func clone(data []byte) []byte {
	if data == nil {
		return nil
	}
	return append([]byte(nil), data...)
}

func encodeFile(payload []byte) []byte {
	body := clone(payload)
	buf := make([]byte, snapshotHeaderSize+len(body))

	copy(buf[0:4], []byte(snapshotMagic))
	binary.BigEndian.PutUint64(buf[4:12], uint64(len(body)))
	copy(buf[snapshotHeaderSize:], body)

	return buf
}

func decodeFile(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	if len(data) < snapshotHeaderSize {
		return nil, fmt.Errorf("snapshot file too short: got %d bytes", len(data))
	}

	if string(data[0:4]) != snapshotMagic {
		return nil, fmt.Errorf("invalid snapshot magic: %q", string(data[0:4]))
	}

	payloadLen := binary.BigEndian.Uint64(data[4:12])
	actualLen := uint64(len(data) - snapshotHeaderSize)

	if payloadLen != actualLen {
		return nil, fmt.Errorf("snapshot payload size mismatch: header=%d, actual=%d", payloadLen, actualLen)
	}

	return clone(data[snapshotHeaderSize:]), nil
}

func (s *Store) SaveSlot(slot int, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.dir, slotFileName(slot))
	data := encodeFile(snapshot)
	tmp, err := os.CreateTemp(s.dir, ".snapshot-*.tmp")
	if err != nil {
		return err
	}

	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, path); err != nil {
		return err
	}

	return syncDir(s.dir)
}

func (s *Store) LoadSlot(slot int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.dir, slotFileName(slot))
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	return decodeFile(data)
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}