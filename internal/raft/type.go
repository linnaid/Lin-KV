// 将课程的包转为自己的包
package raft

type ApplyMsg struct {
	CommandValid bool // 是否为有效命令
	Command 	 []byte // 命令内容
	CommandIndex int // 索引

	SnapshotValid bool // 是否为有效快照
	Snapshot      []byte // 快照内容
	SnapshotTerm  int  // 快照任期
	SnapshotIndex int  // 快照日志索引
}

type LogEntry struct {
	Term    int
	Command []byte
	Index   int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type InstallSnapshotArgs struct {
	Term             int
	LeaderID         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte // 快照内容
}

type InstallSnapshotReply struct {
	Term int
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int // leader任期号
	CandidateId int // 候选者id
	// 下面是日志
	LastLogIndex int // 最新一条日志索引
	LastLogTerm  int // 最新日志任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool // 是否投票给当前任期者
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// 下面是日志
	PrevLogIndex int        // 之前处理的日志索引
	PrevLogTerm  int        // 日志任期号
	Entries      []LogEntry // 需要追加的日志条目，为空则是心跳
	LeaderCommit int        // Leader的已提交索引
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type OpType string

const(
	OpPut     OpType = "PUT"
	OpDelete  OpType = "DELETE"
)

type Op struct {
	Type 	  OpType
	Key 	  string
	Value     string
	LeaseID   int64
	ClientID  int64
	ReqID	  int64
}