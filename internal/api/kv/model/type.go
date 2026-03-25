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

type TxnRequest struct {
	Ops 		[]*Op
	ClientID 	int64
	Seq 		int64
}

type OpResult struct {
	Type 	OpType
	Key 	string
	Value 	[]byte
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
