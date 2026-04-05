package command

type LeaseGrantCommand struct {
	TTL 	int64
	ClientID int64
	Seq 	int64
}

type LeaseRevokeCommand struct {
	ID 		int64
	ClientID int64
	Seq 	int64
}

type LeaseKeepAliveCommand struct {
	ID 		int64
	ClientID int64
	Seq 	int64
}