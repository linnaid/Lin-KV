// 负责保存最新的 raft state 磁盘副本
package wal

import "fmt"

const walSlotCount = 2              // WAL 采用双槽位保存 raft state，manifest 只指向已提交槽位。
const walTempPattern = ".wal-*.tmp" // 临时文件统一放在 WAL 目录下，rename 前不会污染正式槽位文件名。
const walMagic = "WAL1"             // WAL 文件头 magic 用来拒绝误读其他格式文件。
const walHeaderSize = 12            // WAL 文件头为 4 字节 magic 加 8 字节 payload 长度。

// slotFileName 根据槽位编号返回对应 WAL 文件名
func slotFileName(slot int) string {
	return fmt.Sprintf("slot-%d.wal", slot)
}
