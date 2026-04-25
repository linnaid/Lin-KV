// 负责单个 WAL 文件的编码、解码和原子写
package wal

import (
	"encoding/binary"
	"fmt"
	"os"
)

// clone 用来防止调用方和持久化层共享底层切片
func clone(data []byte) []byte {
	if data == nil {
		return nil
	}
	return append([]byte(nil), data...)
}

// encodeSegment 把 payload 编码成带 header 的文件内容
func encodeSegment(payload []byte) []byte {
	body := clone(payload)
	buf := make([]byte, walHeaderSize+len(body))
	copy(buf[0:4], []byte(walMagic))
	binary.BigEndian.PutUint64(buf[4:12], uint64(len(body)))
	copy(buf[walHeaderSize:], body)

	return buf
}

// decodeSegment 从磁盘文件内容中恢复出原始 payload
func decodeSegment(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	if len(data) < walHeaderSize {
		return nil, fmt.Errorf("wal file too short: got %d bytes", len(data))
	}

	if string(data[0:4]) != walMagic {
		return nil, fmt.Errorf("invalid wal magic: %q", string(data[0:4]))
	}
	payloadLen := binary.BigEndian.Uint64(data[4:12])
	actualLen := uint64(len(data) - walHeaderSize)

	if payloadLen != actualLen {
		return nil, fmt.Errorf("wal payload size mismatch: header=%d actual=%d", payloadLen, actualLen)
	}

	return clone(data[walHeaderSize:]), nil
}

// 用原子 rename 方式更新 WAL 文件
func writeSegmentAtomic(dir string, path string, payload []byte) error {
	data := encodeSegment(payload)
	tmp, err := os.CreateTemp(dir, walTempPattern)
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

	return syncDir(dir)
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
