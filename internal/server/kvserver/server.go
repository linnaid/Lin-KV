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

		leaseMgr := mvcc.NewLeaseManager(store)

		s := &Server{
			id: id,
			raft: raft,
			store: store,
			leaseMgr: leaseMgr,
			applyCh: applyCh,
			waitCh: make(map[int64]*waitEntry),

			clientLastSeq: make(map[int64]int64),
			// clientLastValue: make(map[int64][]byte),
			clientLastResult: make(map[int64]Result),

			maxraftstate: 100,

			watchers: make(map[string][]*watcher),
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
			OpType: command.CmdPut,
			Key: req.Key,
		}

		data, err := command.Encode(&cmd)
		if err != nil {
			res.Err = err.Error()
			return res, err
		}

		// 提交到Raft
		index_raft, _, isLeader := s.raft.Start(data)
		if !isLeader {
			// Tools.Debug("123")
			res.Err = ErrNotLeader.Error()
			return res, ErrNotLeader
		}

		index := change(index_raft)

		s.mu.Lock()
		
		if r, ok := s.LastResult[index]; ok {

			if match(ch, &r.Cmd) {
				delete(s.LastResult, index)
				s.mu.Unlock()

				if r.Err != nil {
					return nil, r.Err
				}
				if r.Rev != nil {
					res.Revision = r.Rev.Main
				}
				return res, nil
			}
			// 不匹配直接忽略
		}

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
			if ch.Result.Err != nil {
				res.Err = ch.Result.Err.Error()
				return res, ch.Result.Err
			}
			if ch.Result.Rev != nil {
				res.Revision = ch.Result.Rev.Main
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
		}
		
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
		res := &kv.DeleteResponse{}
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
			OpType: command.CmdDelete,
			Key: req.Key,
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
		
		if r, ok := s.LastResult[index]; ok {

			if match(ch, &r.Cmd) {
				delete(s.LastResult, index)
				s.mu.Unlock()

				if r.Err != nil {
					return nil, r.Err
				}
				if r.Rev != nil {
					res.Revision = r.Rev.Main
				}
				return res, nil
			}
			// 不匹配直接忽略
		}

		s.waitCh[index] = ch
		s.mu.Unlock()

		// s.mu.Lock()        
		// if _, exists := s.waitCh[index]; !exists {
		// 	s.waitCh[index] = ch
		// }
		// s.mu.Unlock()

		select {
		case<-ch.Notify:
			if ch.Result.Err != nil {
				return nil, ch.Result.Err
			}
			// res := &kv.DeleteResponse{}
			if ch.Result.Rev != nil {
				res.Revision = ch.Result.Rev.Main
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

