// 结构体类型
package kv

type PutRequest struct {
	Key 		string
	Value 		[]byte

	LeaseID 	int64
	ClientID 	int64
	Seq 		int64
}

type PutResponse struct {
	Revision   int64

	Err 	   string
}

type GetRequest struct {
	Key 		string
	Revision 	int64
	
	ClientID 	int64
	Seq 		int64
}

type GetResponse struct {
	Value 		[]byte
	Revision 	int64
	Found 		bool

	Err 		string
}

type DeleteRequest struct {
	Key 		string

	LeaseID 	int64
	ClientID 	int64
	Seq 		int64
}

type DeleteResponse struct {
	Revision 	int64
	Deleted 	bool

	Err 		string
}

type CompareOp int
const (
	CompareEqual CompareOp = iota
	CompareLess
	CompareGreater
)
type Compare struct {
	Key 	string
	Op 		CompareOp
	Revision int64
}

type TxnRequest struct {
	Compares 	[]*Compare
	ThenOps 	[]*Op
	ElseOps 	[]*Op
	ClientID 	int64
	Seq 		int64
}

type OpResult struct {
	Type 	OpType
	Key 	string
	Value 	[]byte
	Revision int64
}

type TxnResponse struct {
	Succeeded    bool
	Results 	 []*OpResult
	Err 		 string
}

type TxnResult struct {
	Results 	[]*OpResult
}

func (r *TxnResult) Get(i int) []byte {
	return r.Results[i].Value
}

type OpType int
const (
	OpGet OpType = iota
	OpPut
	OpDelete
)

type Op struct {
	Type 	OpType
	Key 	string
	Value 	[]byte
}

type Event struct {
	Type 	OpType
	Key 	string
	Value 	[]byte
	Rev 	int64
}


type WatchRequest struct {
	Key 		string
	Prefix		bool
	Revision 	int64
	ClientID 	int64
	Seq 		int64
}

type WatchResponse struct {
	Events 		[]*Event
	Revision 	int64
	Err 		string
}

type LeaseGrantRequest struct {
	ttl int64
}
type LeaseGrantResponse struct {
	id int64
	ttl int64
	err string
}

type LeaseRevokeRequest struct {
	id int64
}
type LeaseRevokeResponse struct {
	err string
}

type LeaseKeepAliveRequest struct {
	id int64
}
type LeaseKeepAliveResponse struct {
	id int64
	ttl int64
	err string
}
