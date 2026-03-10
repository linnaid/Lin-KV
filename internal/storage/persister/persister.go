// 持久化器
package persister

type Persister interface {
	Save(raftstate []byte, snapshot []byte)
	ReadSnapshot() []byte
	SnapshotSize() int
	ReadRaftState() []byte
	RaftStateSize() int
}