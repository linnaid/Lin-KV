package grpc

import (
	"etcd-KV/internal/pb/raftpb"
	"etcd-KV/internal/raft"
)

func toPBLogEntries(entries []raft.LogEntry) []*raftpb.LogEntry

func fromPBLogEntries(entries []*raftpb.LogEntry) []raft.LogEntry


func toPBAppendEntriesArgs(args *raft.AppendEntriesArgs) *raftpb.AppendEntriesRequest

func fromPBAppendEntriesArgs(req *raftpb.AppendEntriesRequest) *raft.AppendEntriesArgs


func toPBAppendEntriesReply(reply *raft.AppendEntriesReply) *raftpb.AppendEntriesReply

func fromPBAppendEntriesReply(resp *raftpb.AppendEntriesReply) *raft.AppendEntriesReply


func toPBRequestVoteArgs(args *raft.RequestVoteArgs) *raftpb.RequestVoteRequest

func fromPBRequestVoteArgs(req *raftpb.RequestVoteRequest) *raft.RequestVoteArgs


func toPBRequestVoteReply(reply *raft.RequestVoteReply) *raftpb.RequestVoteReply

func fromPBRequestVoteReply(resp *raftpb.RequestVoteReply) *raft.RequestVoteReply



func toPBInstallSnapshotArgs(args *raft.InstallSnapshotArgs) *raftpb.InstallSnapshotRequest

func fromPBInstallSnapshotArgs(req *raftpb.InstallSnapshotRequest) *raft.InstallSnapshotArgs


func toPBRInstallSnapshotReply(reply *raft.InstallSnapshotReply) *raftpb.InstallSnapshotReply

func fromPBInstallSnapshotReply(resp *raftpb.InstallSnapshotReply) *raft.InstallSnapshotReply