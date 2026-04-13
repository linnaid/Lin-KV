package mvcc

import (
	"etcd-KV/Tools"
	"etcd-KV/internal/pb/mvcc"
	"fmt"
	"sort"
	"time"
)

// 4
// LeaseGrant      创建租约
// LeaseRevoke     删除租约
// AttachKey       key 绑定租约
// KeepAlive       延长 TTL
// ExpirationLoop  自动过期

func NewLeaseManager(kv *KVStore) *LeaseManager {
	return &LeaseManager{
		kv:          kv,
		leases:      make(map[int64]*Lease),
		nextLeaseID: 0,
	}
}

// 创建租约
func (lm *LeaseManager) LeaseGrant(ttl int64) int64 {
	lm.kv.mu.Lock()
	defer lm.kv.mu.Unlock()

	lm.nextLeaseID++
	leaseID := lm.nextLeaseID
	lease := &Lease{
		ID:       leaseID,
		TTL:      ttl,
		ExpireAt: time.Now().Add(time.Duration(ttl) * time.Second),
		Keys:     make(map[string]struct{}),
	}

	lm.leases[leaseID] = lease

	return leaseID
}

// key 绑定租约
func (lm *LeaseManager) AttachKey(key string, leaseID int64) error {
	lm.kv.mu.Lock()
	defer lm.kv.mu.Unlock()

	lease, err := lm.lookupLeaseLocked(leaseID)
	if err != nil {
		return err
	}

	lm.bindKeyToLeaseLocked(key, leaseID, lease)

	return nil
}

func (lm *LeaseManager) bindKeyToLeaseLocked(key string, leaseID int64, lease *Lease) {
	if oldLeaseID, exists := lm.kv.keyLease[key]; exists && oldLeaseID != leaseID {
		if oldLease, ok := lm.leases[oldLeaseID]; ok {
			delete(oldLease.Keys, key)
		}
	}

	lease.Keys[key] = struct{}{}
	lm.kv.keyLease[key] = leaseID
}

func (lm *LeaseManager) detachKeyLocked(key string) {
	leaseID, exists := lm.kv.keyLease[key]
	if !exists {
		return
	}

	delete(lm.kv.keyLease, key)
	if lease, ok := lm.leases[leaseID]; ok {
		delete(lease.Keys, key)
	}
}

// 在加锁状态下，查找一个 lease，并验证它仍可被绑定。
func (lm *LeaseManager) lookupLeaseLocked(leaseID int64) (*Lease, error) {
	lease, ok := lm.leases[leaseID]
	if !ok {
		return nil, fmt.Errorf("Lease %d not found", leaseID)
	}

	if time.Now().After(lease.ExpireAt) {
		return nil, fmt.Errorf("Lease %d already expired", leaseID)
	}

	return lease, nil
}

// 延长 TTL
func (lm *LeaseManager) LeaseKeepAlive(leaseID int64) (int64, error) {
	lm.kv.mu.Lock()
	defer lm.kv.mu.Unlock()

	lease, ok := lm.leases[leaseID]
	if !ok {
		return 0, fmt.Errorf("Lease %d not found", leaseID)
	}

	now := time.Now()

	// 已经过期了(不可复活)
	if now.After(lease.ExpireAt) {
		return 0, fmt.Errorf("Lease %d already expired", leaseID)
	}

	lease.ExpireAt = now.Add(time.Duration(lease.TTL) * time.Second)

	reaming := int64(lease.ExpireAt.Sub(now).Seconds())
	return reaming, nil
}

// 自动过期
func (lm *LeaseManager) ExpiredLeaseIDs(now time.Time, limit int) []int64 {
	lm.kv.mu.RLock()
	defer lm.kv.mu.RUnlock()

	ids := make([]int64, 0)
	for id, lease := range lm.leases {
		if now.After(lease.ExpireAt) {
			ids = append(ids, id)
			if limit > 0 && len(ids) >= limit {
				break
			}
		}
	}

	return ids
}

func (s *KVStore) ExpiredLeaseIDs(now time.Time, limit int) []int64 {
	return s.leaseMgr.ExpiredLeaseIDs(now, limit)
}

func (lm *LeaseManager) expirationLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for now := range ticker.C {
		expired := lm.ExpiredLeaseIDs(now, 64)
		for _, leaseID := range expired {
			_ = lm.LeaseRevoke(leaseID)
		}
	}
}

// 删除租约
func (lm *LeaseManager) LeaseRevoke(leaseID int64) error {
	lm.kv.mu.Lock()

	lease, ok := lm.leases[leaseID]
	if !ok {
		lm.kv.mu.Unlock()
		return fmt.Errorf("Lease %d not found", leaseID)
	}

	// var keys []string
	keys := make([]string, 0, len(lease.Keys))
	for key := range lease.Keys {
		keys = append(keys, key)
	}

	delete(lm.leases, leaseID)

	events := make([]Event, 0, len(keys))
	for _, key := range keys {
		_, ev := lm.kv.deleteLocked(key)
		events = append(events, ev)
	}

	lm.kv.mu.Unlock()

	// for _, key := range keys {

	// 	lm.kv.mu.Lock()
	// 	delete(lm.kv.keyLease, key)
	// 	lm.kv.mu.Unlock()

	// 	lm.kv.Delete(key)
	// }
	for _, ev := range events {
		lm.kv.eventCh <- ev
	}

	return nil
}

func (lm *LeaseManager) snapshot() ([]*mvcc.Lease, int64) {

	leases := make([]*mvcc.Lease, 0, len(lm.leases))
	for _, l := range lm.leases {
		keys := make([]string, 0, len(l.Keys))
		for k := range l.Keys {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		leases = append(leases, &mvcc.Lease{
			Id:               l.ID,
			Ttl:              l.TTL,
			ExpireAtUnixNano: l.ExpireAt.UnixNano(),
			Keys:             keys,
		})
	}

	nextLeaseID := lm.nextLeaseID

	return leases, nextLeaseID
}

func (lm *LeaseManager) restore(snapLeases []*mvcc.Lease) map[int64]*Lease {
	leases := make(map[int64]*Lease, len(snapLeases))
	for _, l := range snapLeases {
		if l.Id <= 0 {
			Tools.Error("invalid lease id", l.Id)
			continue
		}

		keys := make(map[string]struct{}, len(l.Keys))
		for _, k := range l.Keys {
			keys[k] = struct{}{}
		}

		expireAt := time.Unix(0, l.ExpireAtUnixNano)

		leases[l.Id] = &Lease{
			ID:       l.Id,
			TTL:      l.Ttl,
			ExpireAt: expireAt,
			Keys:     keys,
		}
	}

	return leases
}
