// 负责保存最新的 raft state 磁盘副本
package wal

import "fmt"

// const currentSegmentName = "0000000000000000.wal"
// const walTempPattern = ".0000000000000000.wal-*.tmp"
// const walMagic = "WAL1"
// const walHeaderSize = 12

const walSlotCount = 2

// slotFileName 根据槽位编号返回对应 WAL 文件名
func slotFileName(slot int) string {
	return fmt.Sprintf("slot-%d.wal", slot)
}