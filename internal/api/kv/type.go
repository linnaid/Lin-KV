// 结构体类型
package kv

type PutRequest struct {
	Key []byte
	Value []byte
	LeaseID int64
	ClientID int64
	Seq int64
}

type GetRequest struct {
	Key []byte
	ClientID int64
	Seq int64
}

type DeleteRequest struct {
	Key []byte
	LeaseID int64
	ClientID int64
	Seq int64
}

type PutResponse struct {
	Revision int64

	Err string
}

type GetResponse struct {
	Value []byte
	Revision int64
	Found bool

	Err string
}

type DeleteResponse struct {
	Revision int64
	Deleted bool

	Err string
}