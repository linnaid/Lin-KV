package grpc

import (
	"context"
	"etcd-KV/internal/pb/raftpb"
	"etcd-KV/internal/raft"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RaftServer struct {
	raftpb.UnimplementedRaftServer

	mu sync.RWMutex
	rf *raft.Raft
}

func NewRaftServer(rf *raft.Raft) *RaftServer {
	return &RaftServer{
		rf: rf,
	}
}

func (s *RaftServer) SetRaft(rf *raft.Raft) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rf = rf
}

func (s *RaftServer) GetRaft() (*raft.Raft, error) {
	s.mu.Lock()
	rf := s.rf
	s.mu.Unlock()

	if rf == nil {
		return nil, status.Error(codes.Unavailable, "raft server not ready")
	}
	return rf, nil
}

func (s *RaftServer) AppendEntries(ctx context.Context, 
	req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesReply, error) {
		rf, err := s.GetRaft()
		if err != nil {
			return nil, err
		}

		args := fromPBAppendEntriesArgs(req)
		reply := &raft.AppendEntriesReply{}
		rf.AppendEntries(args, reply)

		return toPBAppendEntriesReply(reply), nil
	}


func (s *RaftServer) RequestVote(ctx context.Context, 
	req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteReply, error) {
		rf, err := s.GetRaft()
		if err != nil {
			return nil, err
		}

		args := fromPBRequestVoteArgs(req)
		reply := &raft.RequestVoteReply{}
		rf.RequestVote(args, reply)

		return toPBRequestVoteReply(reply), nil
	}


func (s *RaftServer) InstallSnapshot(ctx context.Context, 
	req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotReply, error) {
		rf, err := s.GetRaft()
		if err != nil {
			return nil, err
		}

		args := fromPBInstallSnapshotArgs(req)
		reply := &raft.InstallSnapshotReply{}
		rf.InstallSnapshot(args, reply)

		return toPBInstallSnapshotReply(reply), nil
	}