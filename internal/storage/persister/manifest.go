// 负责记录当前生效的 wal 槽位和 snapshot 槽位
package persister

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)


const manifestFileName = "current.json"
const noSnapshotSlot = -1

type slotManifest struct {
	RaftSlot int `json:"raft_slot"`
	SnapshotSlot int `json:"snapshot_slot"`
}

func manifestPath(dir string) string {
	return filepath.Join(dir, manifestFileName)
}

// 尝试加载当前 manifest
func loadManifest(dir string) (slotManifest, bool, error) {
	data, err := os.ReadFile(manifestPath(dir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return slotManifest{}, false, nil
		}
		return slotManifest{}, false, err
	}

	var m slotManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return slotManifest{}, false, err
	}
	if err := validateManifest(m); err != nil {
		return slotManifest{}, false, err
	}

	return m, true, nil
}

// 把新的 manifest 原子写到磁盘
func saveManifest(dir string, m slotManifest) error {
	if err := validateManifest(m); err != nil {
		return err
	}

	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	return writeFileAtomic(manifestPath(dir), data)
}

// 校验 manifest 中的槽位值是否合法
func validateManifest(m slotManifest) error {
	if m.RaftSlot < 0 || m.RaftSlot >= 2 {
		return fmt.Errorf("invalid raft slot %d", m.SnapshotSlot)
	}

	if m.SnapshotSlot != noSnapshotSlot && (m.SnapshotSlot < 0 || m.SnapshotSlot >= 2) {
		return fmt.Errorf("invalid snapshot slot %d", m.SnapshotSlot)
	}

	return nil
}

// 用原子 rename 方式更新 manifest 文件
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".mainfest-*.tmp")
	if err != nil {
		return err
	}

	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return  err
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	return syncDir(dir)
}

// 保证 rename 后目录元数据也真正持久化
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()

	return f.Sync()
}