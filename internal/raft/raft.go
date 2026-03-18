// Raft 主逻辑，状态管理
package raft

import (
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	"etcd-KV/internal/labrpc"
	"etcd-KV/internal/storage"
	"etcd-KV/internal/storage/persister"
)

type RoleEvent struct {
	IsLeader bool
	Term int64
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister persister.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm       int           // 当前任期号
	votedFor          int           // 当前任期内候选者id
	state             State         // 当前身份
	electionTimer     *time.Timer   // 控制选举超时
	heartbeatInterval time.Duration // 定期发送心跳的间隔

	// 日志
	log         []LogEntry
	commitIndex int                   // 已知被提交的最大日志记录索引值
	lastApplied int                   // 被执行的最大日志索引号
	nextIndex   []int                 // 每一个服务器下一个日志索引号(初使化为领导者的最后一条日志索引号+1)
	matchIndex  []int                 // 每一个服务器已经复制到该服务器的最大索引号(初始化为0，单调递增)
	applyCh     chan ApplyMsg // 管道，发送可执行日志消息

	// 快照
	lastIncludeIndex int    // 快照中最后一条日志的索引
	lastIncludeTerm  int    // 快照中最后一条日志的任期
	snapshot         []byte // 储存来自上层 kvserver 的快照

	// 状态变化
	roleCh chan RoleEvent
	// 保存
	storage storage.LogStorage
}

func (rf *Raft) Start(command []byte) (int, int, bool) {
	// Tools.Info("Raft Start()", len(rf.peers))

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	term := rf.currentTerm
	isLeader := true
	if rf.state != Leader {
		isLeader = false
		// Tools.Warn("Not Leader")
		return -1, term, isLeader
	} 
	lastIndex := rf.lastIncludeIndex + len(rf.log) - 1
	// Tools.Info("lem(log)", len(rf.log))
	new_index := lastIndex + 1 
	entry := LogEntry{
		Command: command,
		Term:    term,
		Index: new_index,
	}
	rf.log = append(rf.log, entry)
	// rf.wal.Append(entry)
	if len(rf.peers) <= 1 {
		rf.commitIndex = new_index

		// go func(entry LogEntry) {
		// 	rf.applyCh<-ApplyMsg{
		// 		CommandValid: true,
		// 		Command: entry.Command,
		// 		CommandIndex: entry.Index,
		// 	}
		// }(entry)
	}
	// Tools.Info("len(rf.log)", len(rf.log))
	rf.persist()
	
	// Tools.Info("Finished Start")

	return new_index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

// 初始化
func Make(peers []*labrpc.ClientEnd, me int,
	persister persister.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1)
	// rf.log[0].Command = nil
	rf.log[0].Term = 0
	rf.log[0].Index = 0

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	/////////////////////////////////
	rf.state = Follower
	if len(peers) <= 1 {
		rf.state = Leader
		go rf.logUpdate()
	}

	/////////////////////////////////
	rf.heartbeatInterval = 100 * time.Millisecond
	rf.roleCh = make(chan RoleEvent, 8)
	rf.readPersist(persister.ReadRaftState())
	// rf.readSnapshot()

	// rf.persist()

	go rf.applier()

	rf.resetElectionTimer()

	go rf.ticker()
	// go rf.startElection()

	return rf
}
