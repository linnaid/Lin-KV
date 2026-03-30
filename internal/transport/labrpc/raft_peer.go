package labrpc

import (
	"etcd-KV/internal/labrpc"
	"etcd-KV/internal/raft"
	"fmt"
)

// 实现:
// - RequestVote -> "Raft.RequestVote"
// - AppendEntries -> "Raft.AppendEntries"
// - InstallSnapshot -> "Raft.InstallSnapshot"

type LabrpcPeer struct {
	end *labrpc.ClientEnd
}

func(p *LabrpcPeer) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	if ok := p.end.Call("Raft.RequestVote", args, reply); !ok {
		return fmt.Errorf("rpc call failed: Raft.RequestVote")
	}
	return nil
}

func (p *LabrpcPeer) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	if ok := p.end.Call("Raft.AppendEntries", args, reply); !ok {
		return fmt.Errorf("rpc call failed: Raft.AppendEntries")
	}
	return nil
}

func (p *LabrpcPeer) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	if ok := p.end.Call("Raft.InstallSnapshot", args, reply); !ok {
		return fmt.Errorf("rpc call failed: Raft.InstallSnapshot")
	}
	return  nil
}