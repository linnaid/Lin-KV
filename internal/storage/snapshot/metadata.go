// 负责保存 kvserver 传给 raft 的快照字节流
package snapshot

const snapshotFileName = "snapshot.bin"
const snapshotTempPattern = ".snapshot-*.tmp"
const snapshotMagic = "SNP1"
const snapshotHeaderSize = 12