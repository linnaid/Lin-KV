// 利用KVserver把raft和KV粘起来
package kvserver

import (
	"context"
	"etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/command"
	"etcd-KV/internal/raft"
	"etcd-KV/internal/storage/mvcc"
	"time"
)

func NewServer(
	id int, 
	raft *raft.Raft, 
	store *mvcc.KVStore, 
	applyCh chan raft.ApplyMsg) *Server {

	s := &Server{
		id: id,
		raft: raft,
		store: store,
		applyCh: applyCh,
		waitCh: make(map[int64]*waitEntry),
		LastResult: map[int64]Result{},

		clientLastSeq: make(map[int64]int64),
		clientLastResult: make(map[int64]Result),

		maxraftstate: 100,

		watchers: make(map[string][]*watcher),
	}

	if snap := raft.SnapshotBytes(); len(snap) > 0 {
		s.restoreSnapshot(snap)
	}

	// 开始循环接收
	go s.applyLoop()
	go s.leaseReaperLoop()

	go func() {
		for event := range raft.RoleCh() {
			if !event.IsLeader {
				s.clearWaitCh(ErrNotLeader)
			}
		}
	}()

	return s
}

func (s *Server) submit(ctx context.Context, 
	env *command.Command) (Result, error) {
		var zero Result

		data, err := command.Encode(env)
		if err != nil {
			return zero, err
		}

		indexRaft, _, isLeader := s.raft.Start(data)
		if !isLeader {
			return zero, ErrNotLeader
		}
		index := change(indexRaft)

		ch := &waitEntry{
			Notify: make(chan struct{}),
			ClientID: env.ClientID(),
			Seq: env.Seq(),
			Kind: env.Kind,
		}

		s.mu.Lock()
		if r, ok := s.LastResult[index]; ok {
			if match(ch, env) {
				delete(s.LastResult, index)
				s.mu.Unlock()
				return r, r.Err
			}
		}
		s.waitCh[index] = ch
		s.mu.Unlock()

		select {
		case <-ch.Notify:
			return ch.Result, ch.Result.Err
		case <-ctx.Done():
			s.mu.Lock()
			delete(s.waitCh, index)
			s.mu.Unlock()
			return zero, ctx.Err()
		case <-time.After(2 * time.Second):
			s.mu.Lock()
			delete(s.waitCh, index)
			s.mu.Unlock()
			return zero, ErrTimeout
		}
		
	}

func (s *Server)  Put(
	ctx context.Context, 
	req *kv.PutRequest, 
	) (*kv.PutResponse, error) {

		// 构造 Command
		env := &command.Command{
			Kind: command.KindKV,
			KV: &command.KVCommand{
				Type: command.CmdPut,
				Key: string(req.Key),
				Value: append([]byte(nil), req.Value...),
				LeaseID: req.LeaseID,
				ClientID: req.ClientID,
				Seq: req.Seq,
			},
		}

		result, err := s.submit(ctx, env)
		resp := &kv.PutResponse{}
		if result.Rev != nil {
			resp.Revision = result.Rev.Main
		}
		if err != nil {
			resp.Err = err.Error()
		}
		return resp, err
	}

func (s *Server) Get(ctx context.Context, 
	req *kv.GetRequest,
	) (*kv.GetResponse, error) {

		if _, ok := s.raft.GetState(); !ok {
			return nil, ErrNotLeader
		}

		reply := &kv.GetResponse{}
		index, ok := s.raft.ReadIndex()
		if !ok {
			reply.Err = ErrNotLeader.Error()
			return nil, ErrNotLeader
		}

		for {
			applied := s.raft.LastApplied()
			if applied >= index {
				break
			}

			select {
			case <-ctx.Done():
				reply.Err = ctx.Err().Error()
				return reply, ctx.Err()
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}

		s.mu.Lock()

		// Exactly-Once dedup
		lastSeq := s.clientLastSeq[req.ClientID]

		if req.Seq < lastSeq {
			s.mu.Unlock()
			return reply, nil
		}

		if req.Seq == lastSeq {
			result := s.clientLastResult[req.ClientID]

			valCopy := make([]byte, len(result.Value))
			copy(valCopy, result.Value)

			s.mu.Unlock()
			return &kv.GetResponse{
				Value: valCopy,
				Revision: result.Rev.Main,
				Found: result.Found,
			}, nil
		}

		value, rev, ok := s.store.Get(string(req.Key), req.Revision)

		var valCopy []byte
		if ok {
			valCopy = make([]byte, len(value))
			copy(valCopy, value)
		}

		revCopy := rev

		s.clientLastSeq[req.ClientID] = req.Seq
		s.clientLastResult[req.ClientID] = Result{
			Rev: &mvcc.Revision{
				Main: revCopy,
			},
			Value: valCopy,
			Found: ok,
		}
		
		s.mu.Unlock()

		if !ok {
			value = nil
		}

		return &kv.GetResponse{
			Value: value,
			Revision: rev,
			Found: ok,
		}, nil
	}

func (s *Server) Delete(ctx context.Context, 
	req *kv.DeleteRequest, 
	) (*kv.DeleteResponse, error) {
		// 构造 Command
		env := &command.Command{
			Kind: command.KindKV,
			KV: &command.KVCommand{
				Type: command.CmdDelete,
				Key: string(req.Key),
				LeaseID: req.LeaseID,
				ClientID: req.ClientID,
				Seq: req.Seq,
			},
		}

		result, err := s.submit(ctx, env)
		resp := &kv.DeleteResponse{}
		if result.Rev != nil {
			resp.Revision = result.Rev.Main
			resp.Deleted = true
		}
		if err != nil {
			resp.Err = err.Error()
		}
		return resp, err
	}

func (s *Server) Txn(ctx context.Context, 
	req *kv.TxnRequest) (*kv.TxnResponse, error) {
		env := &command.Command{
			Kind: command.KindTxn,
			Txn: toTxnCommand(req),
		}

		result, err := s.submit(ctx, env)
		resp := &kv.TxnResponse{
			Succeeded: result.TxnSucceeded,
			Results: cloneKeyValues(result.TxnResults),
		}

		if err != nil {
			resp.Err = err.Error()
		}

		return resp, err
	}

func (s *Server) LeaseGrant(ctx context.Context, 
	req *kv.LeaseGrantRequest) (*kv.LeaseGrantResponse, error) {
		env := &command.Command{
			Kind: command.KindLeaseGrant,
			LeaseGrant: &command.LeaseGrantCommand{
				TTL: req.TTL,
				ClientID: req.ClientID,
				Seq: req.Seq,
			},
		}

		result, err := s.submit(ctx, env)
		resp := &kv.LeaseGrantResponse{
			ID: result.LeaseID,
			TTL: result.LeaseTTL,
		}
		if err != nil {
			resp.Err = err.Error()
		}

		return resp, err
	}

func (s *Server) LeaseRevoke(ctx context.Context, 
	req *kv.LeaseRevokeRequest) (*kv.LeaseRevokeResponse, error) {
		env := &command.Command{
			Kind: command.KindLeaseRevoke,
			LeaseRevoke: &command.LeaseRevokeCommand{
				ID: req.ID,
				ClientID: req.ClientID,
				Seq: req.Seq,
			},
		}

		_, err := s.submit(ctx, env)
		resp := &kv.LeaseRevokeResponse{}
		if err != nil {
			resp.Err = err.Error()
		}

		return resp, err
	}

func (s *Server) LeaseKeepAlive(ctx context.Context, 
	req *kv.LeaseKeepAliveRequest) (*kv.LeaseKeepAliveResponse, error) {
		env := &command.Command{
			Kind: command.KindLeaseKeepAlive,
			LeaseKeepAlive: &command.LeaseKeepAliveCommand{
				ID: req.ID,
				ClientID: req.ClientID,
				Seq: req.Seq,
			},
		}

		result, err := s.submit(ctx, env)
		resp := &kv.LeaseKeepAliveResponse{
			ID: result.LeaseID,
			TTL: result.LeaseTTL,
		}
		if err != nil {
			resp.Err = err.Error()
		}

		return resp, err
	}

func (s *Server) leaseReaperLoop() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if _, isLeader := s.raft.GetState(); !isLeader {
			continue
		}

		expired := s.store.ExpiredLeaseIDs(time.Now(), 1)
		for _, leaseID := range expired {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := s.submit(ctx, &command.Command{
				Kind: command.KindLeaseRevoke,
				LeaseRevoke: &command.LeaseRevokeCommand{
					ID: leaseID,
					ClientID: internalLeaseRevokeClientID(leaseID),
					Seq: 1,
				},
			})

			cancel()

			if err == ErrNotLeader {
				break
			}
		}
	}
}