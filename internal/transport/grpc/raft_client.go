package grpc

import (
	"context"
	"etcd-KV/internal/pb/raftpb"
	"etcd-KV/internal/raft"

	"google.golang.org/grpc"
)

type GrpcPeer struct {
	cli raftpb.RaftClient
}


func NewGrpcPeer(conn *grpc.ClientConn) raft.Peer {
	return &GrpcPeer{cli: raftpb.NewRaftClient(conn)}
}

func (p *GrpcPeer) AppendEntries(args *raft.AppendEntriesArgs,
	reply *raft.AppendEntriesReply) error {
		resp, err := p.cli.AppendEntries(context.Background(), toPBAppendEntriesArgs(args))
		if err != nil {
			return err
		}
		*reply = *fromPBAppendEntriesReply(resp)
		return nil
}

func (p *GrpcPeer) InstallSnapshot(args *raft.InstallSnapshotArgs,
	reply *raft.InstallSnapshotReply) error {
		resp, err := p.cli.InstallSnapshot(context.Background(), toPBInstallSnapshotArgs(args))
		if err != nil {
			return err
		}
		*reply = *fromPBInstallSnapshotReply(resp)
		return nil
}

func (p *GrpcPeer) RequestVote(args *raft.RequestVoteArgs, 
	reply *raft.RequestVoteReply) error {
		resp, err := p.cli.RequestVote(context.Background(), toPBRequestVoteArgs(args))
		if err != nil {
			return err
		}
		*reply = *fromPBRequestVoteReply(resp)
		return nil
}