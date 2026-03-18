// 利用KVserver把raft和KV粘起来
package kvserver

import (
	"context"
	"etcd-KV/Tools"
	"etcd-KV/internal/api/kv"
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

		leaseMgr := mvcc.NewLeaseManager(store)

		s := &Server{
			id: id,
			raft: raft,
			store: store,
			leaseMgr: leaseMgr,
			applyCh: applyCh,
			waitCh: make(map[int64]*waitEntry),

			clientLastSeq: make(map[int64]int64),
			clientLastValue: make(map[int64][]byte),

			maxraftstate: 100,
		}

	// 开始循环接收
	go s.applyLoop()

	go func() {
		for event := range raft.RoleCh() {
			if !event.IsLeader {
				s.clearWaitCh(ErrNotLeader)
			}
		}
	}()

	return s
}

func (s *Server)  Put(
	ctx context.Context, 
	req *kv.PutRequest, 
	) (*kv.PutResponse, error) {
		res := &kv.PutResponse{}
		// s.mu.Lock()
		// if req.Seq <= s.clientLastSeq[req.ClientID] {
		// 	s.mu.Unlock()
		// 	return res, nil
		// }
		// s.mu.Unlock()

		// 构造 Command
		cmd := command.KVCommand{
			Type: command.CmdPut,
			Key: string(req.Key),
			Value: req.Value,
			ClientID: req.ClientID,
			Seq: req.Seq,
		}

		// 注册wait channel
		ch := &waitEntry{
			Notify: make(chan struct{}),
			ClientID: req.ClientID,
			Seq: req.Seq,
		}

		data, err := command.Encode(&cmd)
		if err != nil {
			res.Err = err.Error()
			return res, err
		}

		// 提交到Raft
		index_raft, _, isLeader := s.raft.Start(data)
		if !isLeader {
			Tools.Debug("123")
			res.Err = ErrNotLeader.Error()
			return res, ErrNotLeader
		}

		index := change(index_raft)

		s.mu.Lock()
		s.waitCh[index] = ch
		s.mu.Unlock()

		// s.mu.Lock()        
		// if _, exists := s.waitCh[index]; !exists {
		// 	s.waitCh[index] = ch
		// }
		// s.mu.Unlock()

		// 等待 apply
		select {
		case <-ch.Notify:
			if ch.Err != nil {
				res.Err = ch.Err.Error()
				return res, ch.Err
			}
			if ch.Rev != nil {
				res.Revision = ch.Rev.Main
			}
			return res,nil
		case <-ctx.Done():
			s.mu.Lock()
			delete(s.waitCh, index)
			s.mu.Unlock()
			res.Err = ctx.Err().Error()
			return res, ctx.Err()
		case <-time.After(2 * time.Second):
			s.mu.Lock()
			delete(s.waitCh, index)
			s.mu.Unlock()
			res.Err = ErrTimeout.Error()
			return res, ErrTimeout
		}
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
			return reply, ErrNotLeader
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
		value, rev, ok := s.store.Get(string(req.Key), req.Revision)
		s.mu.Unlock()

		if !ok {
			value = nil
		}

		return &kv.GetResponse{
			Value: value,
			Revision: rev,
		}, nil
	}

func (s *Server) Delete(ctx context.Context, 
	req *kv.DeleteRequest, 
	) (*kv.DeleteResponse, error) {
		// s.mu.Lock()
		// if req.Seq <= s.clientLastSeq[req.ClientID] {
		// 	s.mu.Unlock()
		// 	return &kv.DeleteResponse{}, nil
		// }
		// s.mu.Unlock()

		cmd := &command.KVCommand{
			Type: command.CmdDelete,
			Key: string(req.Key),
			ClientID: req.ClientID,
			Seq: req.Seq,
		}

		ch := &waitEntry{
			Notify: make(chan struct{}),
			ClientID: req.ClientID,
			Seq: req.Seq,
		}

		data, err := command.Encode(cmd)
		if err != nil {
			return nil, err
		}

		index_raft, _, ok := s.raft.Start(data)
		if !ok {
			return nil, ErrNotLeader
		}

		index := change(index_raft)

		s.mu.Lock()
		s.waitCh[index] = ch
		s.mu.Unlock()

		// s.mu.Lock()        
		// if _, exists := s.waitCh[index]; !exists {
		// 	s.waitCh[index] = ch
		// }
		// s.mu.Unlock()

		select {
		case<-ch.Notify:
			if ch.Err != nil {
				return nil, ch.Err
			}
			res := &kv.DeleteResponse{}
			if ch.Rev != nil {
				res.Revision = ch.Rev.Main
				res.Deleted = true
			}
			return res, nil
		case<-ctx.Done():
			s.mu.Lock()
			delete(s.waitCh, index)
			s.mu.Unlock()
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
			s.mu.Lock()
			delete(s.waitCh, index)
			s.mu.Unlock()
			return nil, ErrTimeout
		}
	}

