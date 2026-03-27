// 线性读取
package raft

import "time"

func (rf *Raft) ReadIndex() (int, bool) {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return 0, false
	}

	if rf.pendingReadCh != nil {
		rf.mu.Unlock()
		return 0, false
	}

	index := rf.commitIndex
	term := rf.currentTerm

	// 初始化一次
	rf.pendingReadCh = make(chan struct{}, 1)
	rf.pendingReadCount = 1 // leader 自己算一个

	rf.mu.Unlock()

	select {
	case rf.readTriggerCh<- struct{}{}:
	default:
	}

	// time.Sleep(rf.heartbeatInterval)

	select {
	case <-rf.pendingReadCh:
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Leader || rf.currentTerm != term {
			return  0, false
		}

		rf.pendingReadCh = nil
		rf.pendingReadCount = 0
		
		return index, true
		
	case <-time.After(100 * time.Millisecond):
		rf.mu.Lock()
		rf.pendingReadCh = nil
		rf.pendingReadCount = 0
		rf.mu.Unlock()

		return 0, false
	}

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// if rf.state != Leader || rf.currentTerm != term {
	// 	return 0, false
	// }

	// return index, true
}

func (rf *Raft) LastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.lastApplied
}