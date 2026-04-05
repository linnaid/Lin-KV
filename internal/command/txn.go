package command

type TxnCompareOp uint8

const (
	TxnCompareEqual TxnCompareOp = iota
	TxnCompareGreater
	TxnCompareLess
)

type TxnCompare struct {
	Key 	string
	Op 		TxnCompareOp
	Revision int64
}

type TxnOpType uint8

const (
	TxnOpGet TxnOpType = iota
	TxnOpPut
	TxnOpDelete
)

type TxnOp struct {
	Type 	TxnOpType
	Key 	string
	Value 	[]byte
	LeaseID int64
}

type TxnCommand struct {
	Compares []*TxnCompare
	ThenOps  []*TxnOp
	ElseOps  []*TxnOp
	ClientID int64
	Seq      int64
}