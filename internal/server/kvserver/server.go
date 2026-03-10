// 利用KVserver把raft和KV粘起来
package kvserver

import (
	"context"
	"errors"
	"etcd-KV/Tools"
	"etcd-KV/internal/api/kv"
	"etcd-KV/internal/command"
	"etcd-KV/internal/raft"
	"etcd-KV/internal/storage/mvcc"
	"sync"
	"time"
)

type waitEntry struct {
	Notify chan struct{}
	ClientID int64
	Seq int64

	Value []byte
	Err error
}

type Server struct {
	id int
	raft *raft.Raft
	kv *mvcc.KVStore

	applyCh chan raft.ApplyMsg  // 全局状态机推进流

	mu sync.Mutex
	waitCh map[int]*waitEntry // 某一个 Raft log Index 的完成通知

	// 客户端去重
	clientLastSeq map[int64]int64  // 最后一次执行请求的Seq
	clientLastValue map[int64][]byte  // 最后一次执行请求的返回值
}

var ErrNotLeader = errors.New("Is not Leader.")
var ErrTimeout = errors.New("Is TimeOut.")

func NewServer(
	id int, 
	raft *raft.Raft, 
	kv *mvcc.KVStore, 
	applyCh chan raft.ApplyMsg) *Server {

		s := &Server{
			id: id,
			raft: raft,
			kv: kv,
			applyCh: applyCh,
			waitCh: make(map[int]*waitEntry),

			clientLastSeq: make(map[int64]int64),
			clientLastValue: make(map[int64][]byte),
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
		s.mu.Lock()
		if req.Seq <= s.clientLastSeq[req.ClientID] {
			s.mu.Unlock()
			return res, nil
		}
		s.mu.Unlock()

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
		}

		data, err := command.Encode(&cmd)
		if err != nil {
			res.Err = err.Error()
			return res, err
		}

		// 提交到Raft
		index, _, isLeader := s.raft.Start(data)
		if !isLeader {
			Tools.Debug("123")
			res.Err = ErrNotLeader.Error()
			return res, ErrNotLeader
		}

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
			return res,nil
		case <-ctx.Done():
			s.mu.Lock()
			delete(s.waitCh, index)
			s.mu.Unlock()
			res.Err = ctx.Err().Error()
			return res, ctx.Err()
		case <-time.After(500 * time.Millisecond):
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
		value, ok := s.kv.Get(string(req.Key))
		s.mu.Unlock()

		if !ok {
			value = nil
		}

		return &kv.GetResponse{
			Value: value,
		}, nil
	}

func (s *Server) Delete(ctx context.Context, 
	req *kv.DeleteRequest, 
	) (*kv.DeleteResponse, error) {
		s.mu.Lock()
		if req.Seq <= s.clientLastSeq[req.ClientID] {
			s.mu.Unlock()
			return &kv.DeleteResponse{}, nil
		}
		s.mu.Unlock()

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

		index, _, ok := s.raft.Start(data)
		if !ok {
			return nil, ErrNotLeader
		}

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
			return &kv.DeleteResponse{}, nil
		case<-ctx.Done():
			s.mu.Lock()
			delete(s.waitCh, index)
			s.mu.Unlock()
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			s.mu.Lock()
			delete(s.waitCh, index)
			s.mu.Unlock()
			return nil, ErrTimeout
		}
	}

