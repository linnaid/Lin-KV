// 选举
package raft

import (
	"etcd-KV/Tools"
	"sync/atomic"
)


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Tools.Debug(">>> RequestVote RECEIVED on server", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// fmt.Println("term1:", reply.Term)
	// fmt.Println("state:", rf.state)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.state = Follower
		event := RoleEvent{
			IsLeader: false,
			Term: int64(rf.currentTerm),
		}
		select {
		case rf.roleCh <- event:
		default:
		}
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = -1
		rf.persist()
		rf.resetElectionTimer()
		// fmt.Println("term2:", reply.Term)
	}

	upToDate := false
	// fmt.Println(rf.lastIncludeIndex)
	if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
		(args.LastLogIndex >= rf.lastIncludeIndex+len(rf.log)-1 && args.LastLogTerm == rf.log[len(rf.log)-1].Term) {
		upToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
		rf.resetElectionTimer()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	// fmt.Printf("已开始选举...\n")
	// Tools.Info("Election Start")
	rf.mu.Lock()
	// slog.Info("state0:",slog.Int("state",rf.state))
	rf.state = Candidate
	// slog.Info("state:",slog.Int("state",rf.state))
	rf.currentTerm++
	// slog.Info("term:",slog.Int("term",rf.currentTerm))
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()
	lastLogIndex := rf.lastIncludeIndex + len(rf.log) - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()
	// 补充字段
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	var votes int32 = 1
	if len(rf.peers) == 1 {
		rf.mu.Lock()
		rf.state = Leader
		// Tools.Info("Leader was done")
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for j := range rf.peers {
			rf.nextIndex[j] = lastLogIndex + 1
			rf.matchIndex[j] = rf.lastIncludeIndex
		}
		go rf.logUpdate()
		rf.mu.Unlock()
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			// Tools.Info("It is me")
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			// Tools.Info("send RequestVote")
			ok := rf.sendRequestVote(server, args, reply)
			// Tools.Info("ok", ok)
			if ok {
				// 处理
				// fmt.Printf("rpc调用成功...")
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Candidate || rf.currentTerm != args.Term {
					return
				}
				if reply.Term > term {
					rf.resetElectionTimer()
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}

				if reply.VoteGranted && reply.Term == term {
					newnode := atomic.AddInt32(&votes, 1)
					// fmt.Println("votes:",newnode)
					// Tools.Info("Yes Leader")
					if int(newnode) > len(rf.peers)/2 {
						if reply.Term == term && rf.state == Candidate {
							rf.state = Leader
							Tools.Info("Leader Success")
							event := RoleEvent{ IsLeader: true, Term: int64(rf.currentTerm)}
							select {
							case rf.roleCh <- event:
							default:
							}
							// go rf.sendHeartbeats()	//////////////////////////////
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))

							for j := range rf.peers {
								rf.nextIndex[j] = lastLogIndex + 1
								rf.matchIndex[j] = rf.lastIncludeIndex
							}
							// rf.mu.Unlock()
							go rf.logUpdate()
							// rf.mu.Lock()
						}
					}
				}
			}
		}(i)
	}
}
