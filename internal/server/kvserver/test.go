package kvserver

import (
	"context"
	"etcd-KV/internal/api/kv"
	"fmt"
	"log"
	"sync"
	"time"
)

func StressTest(s *Server) {
	const (
		clientNum = 50
		reqPerClient = 200
	)

	var wg sync.WaitGroup

	for i := 0; i < clientNum; i++ {
		wg.Add(1)

		go func(cid int64) {
			defer wg.Done()

			for seq := 1; seq <= reqPerClient; seq++ {

				key := fmt.Sprintf("k-%d", cid)
				val := fmt.Sprintf("v-%d-%d", cid, seq)

				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

				_, err := s.Put(ctx, &kv.PutRequest{
					Key: key,
					Value: []byte(val),
					ClientID: cid,
					Seq: int64(seq),
				})

				cancel()

				if err != nil {
					log.Printf("[ERROR] Put failed cid=%d seq=%d err=%v\n", cid, seq, err)
					return
				}

				// 读回来验证
				ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Second)

				resp, err := s.Get(ctx2, &kv.GetRequest{
					Key: key,
				})

				cancel2()

				if err != nil {
					log.Printf("[ERROR] Get failed cid=%d seq=%d err=%v\n", cid, seq, err)
					return
				}

				if string(resp.Value) != val {
					log.Fatalf("[FATAL] WRONG VALUE cid=%d seq=%d got=%s want=%s",
						cid, seq, resp.Value, val)
				}
			}
		}(int64(i))
	}

	wg.Wait()

	log.Println("✅ Stress test finished")
}