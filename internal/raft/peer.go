package raft

type Peer interface {
	RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error 
	AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error 
	InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error
}