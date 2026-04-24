// 对外提供按槽位保存和加载 raft state 的接口
package wal

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// TODO:
// upgrade to append-only WAL with snapshot compaction

type WAL struct {
	mu sync.Mutex
	// file *os.File
	dir string
	// path string
}

type Record struct {
	Type int // 1=HardState 2=Entry
	Data []byte
}

func OpenWAL (dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return &WAL{
		dir: dir,
		// path: segmentPath(dir),
	}, nil
}

func (w *WAL) SaveSlot(slot int, state []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	path := filepath.Join(w.dir, slotFileName(slot))

	return writeSegmentAtomic(w.dir, path, clone(state))
}

func (w *WAL) LoadSlot(slot int) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	path := filepath.Join(w.dir, slotFileName(slot))
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return  nil, err
	}

	return decodeSegment(data)
}

func (w *WAL) Close() error {
	return nil
}

// func (w *WAL) Append(rec *Record) error {
// 	w.mu.Lock()
// 	defer w.mu.Unlock()

// 	totalLen := uint32(4 + len(rec.Data))
// 	if err := binary.Write(w.file, binary.BigEndian, totalLen); err != nil {
// 		return err
// 	}

// 	if err := binary.Write(w.file, binary.BigEndian, uint32(rec.Type)); err != nil {
// 		return  err
// 	}
	
// 	if _, err := w.file.Write(rec.Data); err != nil {
// 		return err
// 	}
	
// 	// file.sync() 把程序缓冲区里的文件内容强制、立刻真正刷到磁盘里
// 	// 返回值(error) 成功 nil，失败 error
// 	return  w.file.Sync()
// }


// func Replay(path string) ([]*Record, error) {
// 	f, err := os.Open(path)
// 	if err != nil {
// 		return  nil, err
// 	}

// 	defer f.Close()

// 	var entries []*Record

// 	for {
// 		var totalLen uint32

// 		err := binary.Read(f, binary.BigEndian, &totalLen); 
// 		if err == io.EOF || err == io.ErrUnexpectedEOF {
// 			break
// 		}
// 		if err != nil {
// 			return nil, err
// 		}

// 		var t uint32
// 		if err := binary.Read(f, binary.BigEndian, &t); err != nil {
// 			if err == io.EOF || err == io.ErrUnexpectedEOF {
// 				break
// 			}
// 			return nil, err
// 		}

// 		dataLen := totalLen - 4
// 		data := make([]byte, dataLen)

// 		if _, err := io.ReadFull(f, data); err != nil {
// 			if err == io.EOF || err == io.ErrUnexpectedEOF {
// 				break
// 			}
// 			return nil, err
// 		}

// 		entries = append(entries, &Record{
// 			Type: int(t),
// 			Data: data,
// 		})
// 	}

// 	return entries, nil
// }

// func (w *WAL) Close() error {
// 	w.mu.Lock()
// 	defer w.mu.Unlock()
// 	return w.file.Close()
// }