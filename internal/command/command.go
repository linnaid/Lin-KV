// 实现raft与其他模块解耦
package command

type Kind uint8

const (
	KindKV Kind = iota
	KindTxn
	KindLeaseGrant
	KindLeaseRevoke
	KindLeaseKeepAlive
)

type Command struct {
	Kind  		Kind
	KV 			*KVCommand
	Txn 		*TxnCommand

	LeaseGrant  *LeaseGrantCommand
	LeaseRevoke *LeaseRevokeCommand
	LeaseKeepAlive *LeaseKeepAliveCommand
}

func (c *Command) ClientID() int64 {
	switch c.Kind {
	case KindKV:
		if c.KV != nil {
			return c.KV.ClientID
		}
	case KindTxn:
		if c.Txn != nil {
			return c.Txn.ClientID
		}
	case KindLeaseGrant:
		if c.LeaseGrant != nil {
			return c.LeaseGrant.ClientID
		}
	case KindLeaseRevoke:
		if c.LeaseRevoke != nil {
			return c.LeaseRevoke.ClientID
		}
	case KindLeaseKeepAlive:
		if c.LeaseKeepAlive != nil {
			return c.LeaseKeepAlive.ClientID
		}
	}
	return 0
}

func (c *Command) Seq() int64 {
	switch c.Kind {
	case KindKV:
		if c.KV != nil {
			return c.KV.Seq
		}
	case KindTxn:
		if c.Txn != nil {
			return c.Txn.Seq
		}
	case KindLeaseGrant:
		if c.LeaseGrant != nil {
			return c.LeaseGrant.Seq
		}
	case KindLeaseRevoke:
		if c.LeaseRevoke != nil {
			return c.LeaseRevoke.Seq
		}
	case KindLeaseKeepAlive:
		if c.LeaseKeepAlive != nil {
			return c.LeaseKeepAlive.Seq
		}
	}
	return 0
}