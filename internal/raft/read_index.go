// 线性读取
package raft

import "time"

func (rf *Raft) ReadIndex() (int, bool) {
	rf.mu.Lock()

	if rf.state != Leader {
		return 0, false
	}

	index := rf.commitIndex
	term := rf.currentTerm
	rf.mu.Unlock()

	time.Sleep(rf.heartbeatInterval)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm != term {
		return 0, false
	}

	return index, true
}

func (rf *Raft) LastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.lastApplied
}