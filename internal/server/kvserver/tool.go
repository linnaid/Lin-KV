package kvserver

import (
	kv "etcd-KV/internal/api/kv/model"
	"etcd-KV/internal/command"
	"etcd-KV/internal/storage/mvcc"
)

func change(n int) int64 {
	return int64(n)
}

func convert(ch <-chan mvcc.Event) chan *kv.Event {
	out := make(chan *kv.Event)

	go func() {
		for ev := range ch {

			var t kv.OpType
			switch ev.Type {
			case mvcc.EventDelete:
				t = kv.OpDelete
			case mvcc.EventPut:
				t = kv.OpPut
			default:
				t = kv.OpGet
			}

			e := &kv.Event{
				Key: ev.Key,
				Value: ev.Value,
				Type: t,
				Rev: ev.Rev.Main,
			}

			out <- e
		}
		close(out)
	}()

	return out
}

func match(ch *waitEntry, cmd *command.KVCommand) bool {
	return ch.ClientID == cmd.ClientID &&
		   ch.Seq == cmd.Seq && 
		   ch.OpType == cmd.Type && 
		   ch.Key == cmd.Key
}