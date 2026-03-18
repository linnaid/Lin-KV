// Leader专属逻辑
package raft

import (
	"etcd-KV/Tools"
	"time"
)

func (rf *Raft) logUpdate() {
	// Tools.Info("logUpdate start")
	// slog.Info("logUpdate start lalala", slog.Int("new_index", new_index))
	for !rf.killed(){
		rf.mu.Lock()
		new_index := len(rf.log) + rf.lastIncludeIndex - 1
		// slog.Info("logUpdate start lalala", slog.Int("new_index", new_index))
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			event := RoleEvent{ IsLeader: false, Term: int64(rf.currentTerm)}
			select {
			case rf.roleCh <- event:
			default:
			}
			return
		}
		rf.mu.Unlock()

		if len(rf.peers) <= 1 {
			rf.mu.Lock()
			rf.updateCommiIndex()
			rf.mu.Unlock()
		}
		for i := range rf.peers {
			if i == rf.me {
				rf.mu.Lock()
				rf.nextIndex[i] = new_index + 1
				rf.matchIndex[i] = new_index
				rf.mu.Unlock()
				continue
			}

			go func(server int) {
				// for !rf.killed() {
					rf.mu.Lock()
					if rf.state != Leader {
						event := RoleEvent{ IsLeader: false, Term: int64(rf.currentTerm)}
						select {
						case rf.roleCh <- event:
						default:
						}
						rf.mu.Unlock()
						return
					}
					rf.updateCommiIndex()
					Index := rf.nextIndex[server]
					// slog.Warn("No access go")
					// slog.Int("Index", Index)
					// slog.Int("rf.lastIncludeIndex", rf.lastIncludeIndex)
					// return

					// slog.Info("AppendEntries",slog.Int("Index", Index), slog.Int("rf.lastIncludeIndex", rf.lastIncludeIndex))
					if Index <= rf.lastIncludeIndex {
						// slog.Warn("No access go")
						args := InstallSnapshotArgs{
							Term:             rf.currentTerm,
							LeaderID:         rf.me,
							LastIncludeIndex: rf.lastIncludeIndex,
							LastIncludeTerm:  rf.lastIncludeTerm,
							Data:             rf.snapshot,
						}
						reply := InstallSnapshotReply{}

						currentTerm := rf.currentTerm
						rf.mu.Unlock()
						Tools.Info("Send InstallSnapshot")
						ok := rf.sendInstallSnapshot(server, &args, &reply)
						rf.mu.Lock()
						// defer rf.mu.Unlock()
						if !ok || rf.state != Leader || rf.currentTerm != currentTerm {
								rf.mu.Unlock()
								return
							}
						if ok {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = Follower
								rf.votedFor = -1
								rf.persist()
								event := RoleEvent{ IsLeader: false, Term: int64(rf.currentTerm)}
								select {
								case rf.roleCh <- event:
								default:
								}
								rf.mu.Unlock()
								return
							}
							Index = rf.lastIncludeIndex
							rf.nextIndex[server] = Index + 1
							rf.matchIndex[server] = Index
						}
						rf.mu.Unlock()
						return
					} 
					// slog.Warn("No access go")
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						LeaderCommit: rf.commitIndex,
					}

					Index = rf.nextIndex[server]
					args.PrevLogIndex = Index - 1
					sliceIdx := args.PrevLogIndex - rf.lastIncludeIndex
					if sliceIdx < 0 || sliceIdx >= len(rf.log) {
						rf.nextIndex[server] = rf.lastIncludeIndex + 1
						rf.mu.Unlock()
						return
					}
					args.PrevLogTerm = rf.log[sliceIdx].Term

					real_Index := Index - rf.lastIncludeIndex
					lastLogIndex := rf.lastIncludeIndex + len(rf.log) - 1
					if Index > lastLogIndex {
						args.Entries = nil
					} else {
						args.Entries = rf.log[real_Index:]
					}
					// entries := rf.log[real_Index:]
					// args.Entries = entries

					reply := AppendEntriesReply{}
					if rf.state != Leader || args.Term != rf.currentTerm {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					// Tools.Info("Send Appendentries")
					ok := rf.sendAppendEntries(server, &args, &reply)
					rf.mu.Lock()
					// defer rf.mu.Unlock()
					if !ok || rf.state != Leader || args.Term != rf.currentTerm {
						rf.mu.Unlock()
						return
					}
					if ok {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.persist()
							event := RoleEvent{ IsLeader: false, Term: int64(rf.currentTerm)}
							select {
							case rf.roleCh <- event:
							default:
							}
							rf.mu.Unlock()
							return
						}
						if reply.Success {
							// if len(entries) > 0 {
								rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
								rf.nextIndex[server] = rf.matchIndex[server] + 1
							// } else {
							// 	if rf.matchIndex[server] < args.PrevLogIndex {
							// 		rf.matchIndex[server] = args.PrevLogIndex
							// 		rf.nextIndex[server] = rf.matchIndex[server] + 1
							// 	}
							// }
							///////////////////////////////////////////commite
						} else {
							if reply.ConflictTerm == -1 {
								rf.nextIndex[server] = reply.ConflictIndex
								// return
							} else {
								// lastIndexOfTerm := -1
								// for i := len(rf.log) - 1; i >= 0; i-- {
								// 	if rf.log[i].Term == reply.ConflictTerm {
								// 		lastIndexOfTerm = i + rf.lastIncludeIndex
								// 		break
								// 	}
								// }
								// if lastIndexOfTerm != -1 {
								// 	rf.nextIndex[server] = lastIndexOfTerm + 1
								// } else {
								// 	rf.nextIndex[server] = reply.ConflictIndex
								// }
							}
							if reply.ConflictIndex != -1 {
								rf.nextIndex[server] = reply.ConflictIndex
							}
							// time.Sleep(10 * time.Millisecond)
						}

						if rf.matchIndex[server] > rf.nextIndex[server] {
							rf.matchIndex[server] = rf.nextIndex[server] - 1
						}
						// slog.Warn("No access go")
					} 
					rf.mu.Unlock()
				// }
			}(i)
		}
		time.Sleep(rf.heartbeatInterval)
	}
	
}

func (rf *Raft) updateCommiIndex() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// Tools.Info("updateCommite Index")

	lastIndex := rf.lastIncludeIndex + len(rf.log) - 1
	// Tools.Info("lastIndex", lastIndex, rf.commitIndex)
	for N := lastIndex; N > rf.commitIndex; N-- {
		// Tools.Info("N=", N)
		if N <= rf.lastIncludeIndex {
			break
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			sliceIdx := N - rf.lastIncludeIndex
			if sliceIdx > 0 && sliceIdx < len(rf.log) && rf.log[sliceIdx].Term == rf.currentTerm {
				rf.commitIndex = N
				break
			}
		}
	}
}
