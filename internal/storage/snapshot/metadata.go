// 负责保存 kvserver 传给 raft 的快照字节流
package snapshot

import "fmt"

// const snapshotFileName = "snapshot.bin"
// const snapshotTempPattern = ".snapshot-*.tmp"
// const snapshotMagic = "SNP1"
// const snapshotHeaderSize = 12

const snapshotSlotCount = 2
const snapshotMagic = "SNP1"
const snapshotHeaderSize = 12

func slotFileName(slot int) string {
	return fmt.Sprintf("slot-%d.snap", slot)
}