// 工具，不属于任何一个模块，但被使用在许多模块内
package raft

import "sync/atomic"

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) RoleCh() <-chan RoleEvent {
	return rf.roleCh
}

func (rf *Raft) GetState() (int, bool) {

	var id int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// term = rf.currentTerm
	id = rf.me
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return id, isleader
}