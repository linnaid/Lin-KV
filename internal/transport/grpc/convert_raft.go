package grpc

import (
	"etcd-KV/internal/pb/raftpb"
	"etcd-KV/internal/raft"
)

func toPBLogEntries(entries []raft.LogEntry) []*raftpb.LogEntry {
	if len(entries) == 0 {
		return nil
	}

	out := make([]*raftpb.LogEntry, 0, len(entries))
	for _, e := range entries {
		cmd := append([]byte(nil), e.Command...)
		out = append(out, &raftpb.LogEntry{
			Term:    int64(e.Term),
			Command: cmd,
			Index:   int64(e.Index),
		})
	}
	return out
}

func fromPBLogEntries(entries []*raftpb.LogEntry) []raft.LogEntry {
	if len(entries) == 0 {
		return nil
	}

	out := make([]raft.LogEntry, 0, len(entries))
	for _, e := range entries {
		cmd := append([]byte(nil), e.Command...)
		out = append(out, raft.LogEntry{
			Term:    int(e.Term),
			Command: cmd,
			Index:   int(e.Index),
		})
	}
	return out
}


func toPBAppendEntriesArgs(args *raft.AppendEntriesArgs) *raftpb.AppendEntriesRequest {
	if args == nil {
		return nil
	}
	return &raftpb.AppendEntriesRequest{
		Term:         int64(args.Term),
		LeaderId:     int64(args.LeaderId),
		PrevLogIndex: int64(args.PrevLogIndex),
		PrevLogTerm:  int64(args.PrevLogTerm),
		Entries:      toPBLogEntries(args.Entries),
		LeaderCommit: int64(args.LeaderCommit),
	}
}

func fromPBAppendEntriesArgs(req *raftpb.AppendEntriesRequest) *raft.AppendEntriesArgs {
	if req == nil {
		return nil
	}
	return &raft.AppendEntriesArgs{
		Term:         int(req.Term),
		LeaderId:     int(req.LeaderId),
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      fromPBLogEntries(req.Entries),
		LeaderCommit: int(req.LeaderCommit),
	}
}


func toPBAppendEntriesReply(reply *raft.AppendEntriesReply) *raftpb.AppendEntriesReply {
	if reply == nil {
		return nil
	}

	return &raftpb.AppendEntriesReply{
		Term:          int64(reply.Term),
		Success:       reply.Success,
		ConflictTerm:  int64(reply.ConflictTerm),
		ConflictIndex: int64(reply.ConflictIndex),
	}
}

func fromPBAppendEntriesReply(resp *raftpb.AppendEntriesReply) *raft.AppendEntriesReply {
	if resp == nil {
		return nil
	}

	return &raft.AppendEntriesReply{
		Term:          int(resp.Term),
		Success:       resp.Success,
		ConflictTerm:  int(resp.ConflictTerm),
		ConflictIndex: int(resp.ConflictIndex),
	}
}


func toPBRequestVoteArgs(args *raft.RequestVoteArgs) *raftpb.RequestVoteRequest {
	if args == nil {
		return nil
	}

	return &raftpb.RequestVoteRequest{
		Term:         int64(args.Term),
		CandidateId:  int64(args.CandidateId),
		LastLogIndex: int64(args.LastLogIndex),
		LastLogTerm:  int64(args.LastLogTerm),
	}
}

func fromPBRequestVoteArgs(req *raftpb.RequestVoteRequest) *raft.RequestVoteArgs {
	if req == nil {
		return nil
	}

	return &raft.RequestVoteArgs{
		Term:         int(req.Term),
		CandidateId:  int(req.CandidateId),
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	}
}


func toPBRequestVoteReply(reply *raft.RequestVoteReply) *raftpb.RequestVoteReply {
	if reply == nil {
		return nil
	}

	return &raftpb.RequestVoteReply{
		Term:        int64(reply.Term),
		VoteGranted: reply.VoteGranted,
	}
}

func fromPBRequestVoteReply(resp *raftpb.RequestVoteReply) *raft.RequestVoteReply {
	if resp == nil {
		return nil
	}

	return &raft.RequestVoteReply{
		Term:        int(resp.Term),
		VoteGranted: resp.VoteGranted,
	}
}


func toPBInstallSnapshotArgs(args *raft.InstallSnapshotArgs) *raftpb.InstallSnapshotRequest {
	if args == nil {
		return nil
	}

	return &raftpb.InstallSnapshotRequest{
		Term:             int64(args.Term),
		LeaderId:         int64(args.LeaderID),
		LastIncludeIndex: int64(args.LastIncludeIndex),
		LastIncludeTerm:  int64(args.LastIncludeTerm),
		Data:             append([]byte(nil), args.Data...),
	}
}

func fromPBInstallSnapshotArgs(req *raftpb.InstallSnapshotRequest) *raft.InstallSnapshotArgs {
	if req == nil {
		return nil
	}

	return &raft.InstallSnapshotArgs{
		Term:             int(req.Term),
		LeaderID:         int(req.LeaderId),
		LastIncludeIndex: int(req.LastIncludeIndex),
		LastIncludeTerm:  int(req.LastIncludeTerm),
		Data:             append([]byte(nil), req.Data...),
	}
}


func toPBInstallSnapshotReply(reply *raft.InstallSnapshotReply) *raftpb.InstallSnapshotReply {
	if reply == nil {
		return nil
	}

	return &raftpb.InstallSnapshotReply{
		Term: int64(reply.Term),
	}
}

func fromPBInstallSnapshotReply(resp *raftpb.InstallSnapshotReply) *raft.InstallSnapshotReply {
	if resp == nil {
		return nil
	}

	return &raft.InstallSnapshotReply{
		Term: int(resp.Term),
	}
}
