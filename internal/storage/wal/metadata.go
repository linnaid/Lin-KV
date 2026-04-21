// 负责保存最新的 raft state 磁盘副本
package wal

const currentSegmentName = "0000000000000000.wal"
const walTempPattern = ".0000000000000000.wal-*.tmp"
const walMagic = "WAL1"
const walHeaderSize = 12
