// Raft 的“心跳与选举驱动器”
package raft

import (
	// "etcd-KV/Tools"
	"math/rand"
	"time"
)


func (rf *Raft) ticker() {
	// Tools.Info("Start Election")
	// fmt.Printf("已开始ticker函数...\n")
	for !rf.killed() {
		// Tools.Info("Start Loop")
		// fmt.Printf("已开始循环...\n")
		rf.mu.Lock()
		// is_leader := false
		state := rf.state
		timer := rf.electionTimer
		rf.mu.Unlock()

		switch state {
		case Follower, Candidate:
			// 检测选举是否超时
			if timer == nil {
				rf.mu.Lock()
				rf.resetElectionTimer()
				rf.mu.Unlock()
				time.Sleep(10 * time.Microsecond)
				continue
			}
			select {
			case <-timer.C:
				// Tools.Info("1")
				rf.mu.Lock()
				// Tools.Info("Into Election")
				rf.resetElectionTimer()
				rf.mu.Unlock()
				go rf.startElection()
			default:
				// Tools.Info("2")
				time.Sleep(10 * time.Microsecond)
			}
		}
	}
	// Tools.Info("3")
}

func (rf *Raft) resetElectionTimer() {
	t_out := time.Duration(150+rand.Intn(150)) * time.Millisecond
	if rf.electionTimer != nil {
		if !rf.electionTimer.Stop() {
			select {
			case <-rf.electionTimer.C:
			default:
			}
		}
	} else {
		rf.electionTimer = time.NewTimer(t_out)
		return
	}
	rf.electionTimer.Reset(t_out)
}
