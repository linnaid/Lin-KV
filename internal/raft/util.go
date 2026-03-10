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