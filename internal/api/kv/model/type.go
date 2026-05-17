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
	CompareGreater
	CompareLess
)
type Compare struct {
	Key 	string
	Op 		CompareOp
	Revision int64
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
	LeaseID int64
}

type KeyValue struct {
	Key 	string
	Value 	[]byte
	Revision int64
}

type TxnRequest struct {
	Compares 	[]*Compare
	ThenOps 	[]*Op
	ElseOps 	[]*Op
	ClientID 	int64
	Seq 		int64
}

type TxnResponse struct {
	Succeeded    bool
	Results 	 []*KeyValue
	Err 		 string
}

type TxnResult struct {
	Succeeded   bool
	Results 	[]*KeyValue
}

func (r *TxnResult) Get(i int) []byte {
	if i < 0 || i >= len(r.Results) || r.Results[i] == nil {
		return nil
	}
	return r.Results[i].Value
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
	TTL int64
	ClientID int64
	Seq int64
}
type LeaseGrantResponse struct {
	ID int64
	TTL int64
	Err string
}

type LeaseRevokeRequest struct {
	ID int64
	ClientID int64
	Seq int64
}
type LeaseRevokeResponse struct {
	Err string
}

type LeaseKeepAliveRequest struct {
	ID int64
	ClientID int64
	Seq int64
}
type LeaseKeepAliveResponse struct {
	ID int64
	TTL int64
	Err string
}

type RangeRequest struct {
	Key string
	Prefix bool
	Revision int64
	ClientID int64
	Seq int64
}

type RangeResponse struct {
	KVs  []*KeyValue
	Revision int64
	Err  string
}