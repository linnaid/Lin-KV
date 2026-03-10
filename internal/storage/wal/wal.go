package wal

import (
	"encoding/binary"
	"io"
	"os"
	"sync"

)

type WAL struct {
	mu sync.Mutex
	file *os.File
}

type Record struct {
	Type int // 1=HardState 2=Entry
	Data []byte
}

func OpenWAL (path string) (*WAL, error) {
	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return  nil, err
	}

	return &WAL{
		file: f,
	}, nil
}

func (w *WAL) Append(rec *Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	totalLen := uint32(4 + len(rec.Data))
	if err := binary.Write(w.file, binary.BigEndian, totalLen); err != nil {
		return err
	}

	if err := binary.Write(w.file, binary.BigEndian, uint32(rec.Type)); err != nil {
		return  err
	}
	
	if _, err := w.file.Write(rec.Data); err != nil {
		return err
	}
	
	// file.sync() 把程序缓冲区里的文件内容强制、立刻真正刷到磁盘里
	// 返回值(error) 成功 nil，失败 error
	return  w.file.Sync()
}


func Replay(path string) ([]*Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return  nil, err
	}

	defer f.Close()

	var entries []*Record

	for {
		var totalLen uint32

		err := binary.Read(f, binary.BigEndian, &totalLen); 
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}

		var t uint32
		if err := binary.Read(f, binary.BigEndian, &t); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		dataLen := totalLen - 4
		data := make([]byte, dataLen)

		if _, err := io.ReadFull(f, data); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		entries = append(entries, &Record{
			Type: int(t),
			Data: data,
		})
	}

	return entries, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}