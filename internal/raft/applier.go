package raft

import (
	// "etcd-KV/Tools"
	"time"
)

func (rf *Raft) applier() {
	for !rf.killed() {
		// Tools.Info("Applier")
		rf.mu.Lock()
		// 快照日志：如果 lastApplied < lastIncludeIndex，需要发送快照
		// Tools.Debug("Snapshot", rf.lastApplied, rf.lastIncludeIndex)
		if rf.lastApplied < rf.lastIncludeIndex {
			snapshotIndex := rf.lastIncludeIndex
			snapshotTerm := rf.lastIncludeTerm
			snapshotData := make([]byte, len(rf.snapshot))
			copy(snapshotData, rf.snapshot)

			// 更新 lastApplied 为快照索引，这样测试框架的 ingestSnap 会正确更新
			rf.lastApplied = snapshotIndex
			rf.mu.Unlock()
			// Tools.Info("Apply Snapshot")
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				SnapshotIndex: snapshotIndex,
				SnapshotTerm:  snapshotTerm,
				Snapshot:      snapshotData,
			}
			continue
		}

		// 普通日志：只应用 commitIndex 之后且不在快照中的日志
		msgs := make([]ApplyMsg, 0)
		// Tools.Debug("Entries", rf.lastApplied, rf.commitIndex)
		for rf.lastApplied < rf.commitIndex {
			nextIdx := rf.lastApplied + 1
			// 如果 nextIdx 在快照范围内，说明快照还没有被发送
			// 这种情况不应该发生，因为快照应该在前面被发送
			if nextIdx <= rf.lastIncludeIndex {
				rf.lastApplied = rf.lastIncludeIndex
				continue
			}
			sliceId := nextIdx - rf.lastIncludeIndex
			if sliceId <= 0 || sliceId >= len(rf.log) {
				// No more entries to apply
				// rf.lastApplied = rf.commitIndex
				break
			}
			rf.lastApplied = nextIdx
			entry := rf.log[sliceId]
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			msgs = append(msgs, msg)
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			// Tools.Info("Apply Entries")
			rf.applyCh <- msg
		}
		// 防止空转
		time.Sleep(10 * time.Microsecond)
	}
}
