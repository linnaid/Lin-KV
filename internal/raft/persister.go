package raft

import (
	// "etcd-KV/Tools"
	"etcd-KV/internal/pb/raftpb"

	"google.golang.org/protobuf/proto"
)

// 日志转换
func convertLogToPB(log []LogEntry) []*raftpb.LogEntry {
	logs := make([]*raftpb.LogEntry, 0, len(log))
	for _, one_log := range log {
		logs = append(logs, &raftpb.LogEntry{
			Term:    int64(one_log.Term),
			Index:   int64(one_log.Index),
			Command: one_log.Command,
		})
	}
	return logs
}

func (rf *Raft) persist() {

	e := &raftpb.RaftState{
		Hardstate: &raftpb.HardState{
			Term:     int64(rf.currentTerm),
			VotedFor: int64(rf.votedFor),
		},
		Snapshot: &raftpb.Snapshot{
			LastIncludeIndex: int64(rf.lastIncludeIndex),
			LastIncludeTerm:  int64(rf.lastIncludeTerm),
		},
		Log: convertLogToPB(rf.log),
	}

	// e := .NewEncoder(w)

	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.log)
	// e.Encode(rf.lastIncludeIndex)
	// e.Encode(rf.lastIncludeTerm)

	data, err := proto.Marshal(e)
	if err != nil {
		// Tools.("persist marshal failed.", err.())
		return
	}
	// data := w.Bytes()
	rf.persister.Save(data, rf.snapshot)
}

// 日志恢复
func restoreLogFromPB(pblogs []*raftpb.LogEntry) []LogEntry {
	res := make([]LogEntry, 0, len(pblogs))
	for _, log := range pblogs {
		res = append(res, LogEntry{
			Term:    int(log.Term),
			Index:   int(log.Index),
			Command: log.Command,
		})
	}
	return res
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 || data == nil { // bootstrap without any state?
		return
	}

	e := &raftpb.RaftState{}
	if err := proto.Unmarshal(data, e); err != nil {
		// Tools.Warn("readPersist failed.", err.Error())
		return
	}

	// r := bytes.NewBuffer(data)
	// d := .NewDecoder(r)

	// var currentTerm int
	// var votedFor int
	// var log []LogEntry
	// var lastIncludeIndex int
	// var lastIncludeTerm int

	// if d.Decode(&currentTerm) != nil ||
	// 	d.Decode(&votedFor) != nil ||
	// 	d.Decode(&log) != nil {
	// 	slog.Warn("server readPersist failed","ID",rf.me,)
	// 	return
	// }

	rf.currentTerm = int(e.Hardstate.Term)
	rf.votedFor = int(e.Hardstate.VotedFor)
	if len(e.Log) > 0 {
		rf.log = restoreLogFromPB(e.Log)
	} else {
		rf.log = []LogEntry{{
			Term:  rf.lastIncludeTerm,
			Index: rf.lastIncludeIndex,
		}}
	}

	// if d.Decode(&lastIncludeIndex) == nil ||
	// 	d.Decode(&lastIncludeTerm) == nil {
	// 		rf.lastIncludeIndex = lastIncludeIndex
	// 		rf.lastIncludeTerm = lastIncludeTerm
	// 	}

	rf.lastIncludeIndex = int(e.Snapshot.LastIncludeIndex)
	rf.lastIncludeTerm = int(e.Snapshot.LastIncludeTerm)

	rf.commitIndex = rf.lastIncludeIndex
	rf.lastApplied = rf.lastIncludeIndex

	rf.snapshot = rf.persister.ReadSnapshot()
	// rf.readSnapshot()
}

func (rf *Raft) readSnapshot() {
	if len(rf.snapshot) < 1 || rf.snapshot == nil {
		return
	}

	new_log := make([]LogEntry, 0)
	new_log = append(new_log, LogEntry{
		Term:  rf.lastIncludeTerm,
		Index: rf.lastIncludeIndex,
	})
	for _, log := range rf.log {
		if log.Index > rf.lastIncludeIndex {
			new_log = append(new_log, log)
		}
	}
	rf.log = new_log
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}
