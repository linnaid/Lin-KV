// 日志复制RPC
package raft

import "etcd-KV/Tools"

// import "etcd-KV/Tools"


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Tools.Info("AppendEntry")
	// fmt.Printf("已开始AppendEntries函数...\n")
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		// rf.mu.Unlock()
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// reply.Term = rf.currentTerm
		rf.votedFor = -1
		rf.state = Follower
		event := RoleEvent{ IsLeader: false, Term: int64(rf.currentTerm)}
			select {
			case rf.roleCh <- event:
			default:
			}
		rf.persist()
		rf.resetElectionTimer()
	} else {
		// rf.votedFor = -1
		if rf.state != Follower {
			rf.state = Follower
			event := RoleEvent{ IsLeader: false, Term: int64(rf.currentTerm)}
			select {
			case rf.roleCh <- event:
			default:
			}
		}
		
		// rf.persist()
		rf.resetElectionTimer()
	}

	lastIndex := len(rf.log) + rf.lastIncludeIndex
	// if args.PrevLogTerm >= rf.lastIncludeTerm {
		if args.PrevLogIndex < rf.lastIncludeIndex {
			reply.Success = false
			reply.ConflictIndex = rf.lastIncludeIndex + 1
			return
		}
		sliceIdx := args.PrevLogIndex - rf.lastIncludeIndex
		if sliceIdx >= len(rf.log) {
			reply.Success = false
			reply.ConflictTerm = -1
			reply.ConflictIndex = lastIndex
			// rf.mu.Unlock()
			return
		} else if sliceIdx < 0 {
			reply.Success = false
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.lastIncludeIndex + 1
			return 
		}
		if rf.log[sliceIdx].Term != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictTerm = rf.log[sliceIdx].Term
			conflictIdx := sliceIdx
			for conflictIdx > 0 && rf.log[conflictIdx-1].Term == reply.ConflictTerm {
				conflictIdx--
			}
			reply.ConflictIndex = rf.lastIncludeIndex + conflictIdx
			// raft.commitIndex 不允许回退
			// if rf.commitIndex >= lastIndex+1 {
			// 	rf.commitIndex = lastIndex
			// }
			// rf.mu.Unlock()
			return
		}
	// }

	reply.Success = true

	if len(args.Entries) > 0 {
		idx := args.PrevLogIndex + 1 - rf.lastIncludeIndex
		rf.log = append(rf.log[:idx], args.Entries...)
		// rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewIdx := rf.lastIncludeIndex + len(rf.log) - 1
		if args.LeaderCommit < lastNewIdx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIdx
		}
	}

	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// fmt.Printf("sendAppendEntries函数进入...\n")
	// ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	err := rf.peers[server].AppendEntries(args, reply)
	if err != nil {
		Tools.Debug("sendAppendEntries error", err)
		return false
	}
	return true
}
