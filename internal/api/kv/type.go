// 结构体类型
package kv

import "etcd-KV/internal/storage/mvcc"

type PutRequest struct {
	Key string
	Value []byte

	LeaseID int64
	ClientID int64
	Seq int64
}

type PutResponse struct {
	Revision int64

	Err string
}

type GetRequest struct {
	Key string
	Revision int64
	
	ClientID int64
	Seq int64
}

type GetResponse struct {
	Value []byte
	Revision int64
	Found bool

	Err string
}

type DeleteRequest struct {
	Key string

	LeaseID int64
	ClientID int64
	Seq int64
}

type DeleteResponse struct {
	Revision int64
	Deleted bool

	Err string
}

type TxnRequest struct {
	Txn mvcc.Txn

}

type TxnResponse struct {
	Succeeded bool
	kvs []mvcc.KeyValue

}