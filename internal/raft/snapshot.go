// 快照存储与加载
package raft


func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// fmt.Printf("我要调用这个函数了\n")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludeIndex {
		return
	}

	first_index := rf.lastIncludeIndex
	abs_index := index - first_index
	new_term := rf.log[abs_index].Term
	new_log := make([]LogEntry, 0)
	new_log = append(new_log, LogEntry{
		Term: new_term,
		Index: index,
	})

	for _, log := range rf.log {
		if log.Index > index {
			new_log = append(new_log, log)
		}
	}

	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = new_term
	rf.log = new_log
	rf.snapshot = snapshot

	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	if rf.state == Leader {
		for i := range rf.peers {
			if rf.nextIndex[i] <= rf.lastIncludeIndex {
				rf.nextIndex[i] = rf.lastIncludeIndex + 1
			}
			if rf.matchIndex[i] < rf.lastIncludeIndex {
				rf.matchIndex[i] = rf.lastIncludeIndex
			}
		}
	}
	rf.persist()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	// fmt.Printf("sendAppendEntries函数进入...\n")
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("已进入函数InstallSnapshot...")

	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// reply.Term = args.Term
		rf.state = Follower
		event := RoleEvent{ IsLeader: false, Term: int64(rf.currentTerm)}
		select {
		case rf.roleCh <- event:
		default:
		}
		rf.votedFor = -1
		rf.persist()
	} else if args.Term < rf.currentTerm {
		// fmt.Printf("被返回...")
		return
	}
	rf.state = Follower
	rf.resetElectionTimer()

	if args.LastIncludeIndex <= rf.lastIncludeIndex {
		// fmt.Printf("被返回...")
		// fmt.Println(args.LastIncludeIndex, rf.lastIncludeIndex)
		return
	}

	new_log := make([]LogEntry, 0)
	new_log = append(new_log, LogEntry{
		Term: args.LastIncludeTerm,
		Index: args.LastIncludeIndex,
	})
	for _, log := range rf.log {
		if log.Index > args.LastIncludeIndex {
			new_log = append(new_log, log)
		}
	}
	rf.log = new_log

	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.snapshot = args.Data

	// Update commitIndex
	if rf.commitIndex < rf.lastIncludeIndex {
		rf.commitIndex = rf.lastIncludeIndex
	}
	if rf.lastApplied < rf.lastIncludeIndex {
		rf.lastApplied = rf.lastIncludeIndex
	}

	rf.persist()

	rf.mu.Unlock()

	go func() { 
		rf.applyCh<-ApplyMsg{
			SnapshotValid: true,
			Snapshot: args.Data,
			SnapshotIndex: args.LastIncludeIndex,
			SnapshotTerm: args.LastIncludeTerm,
		}
	}()
	rf.mu.Lock()
}
