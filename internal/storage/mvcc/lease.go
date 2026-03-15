package mvcc

import (
	"fmt"
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
		kv: kv,
		leases: make(map[int64]*Lease),
		nextLeaseID: 0,
	}
}

// 创建租约
func (lm *LeaseManager) LeaseGrant(ttl int64) int64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.nextLeaseID++
	leaseID := lm.nextLeaseID
	lease := &Lease{
		ID: leaseID,
		TTL: ttl,
		ExpireAt: time.Now().Add(time.Duration(ttl) * time.Second),
		Keys: make(map[string]struct{}),
	}

	lm.leases[leaseID] = lease

	return leaseID
}

// key 绑定租约
func (lm *LeaseManager) AttachKey(key string, leaseID int64) error {
	lm.mu.Lock()
	lease, ok := lm.leases[leaseID]
	lm.mu.Unlock()

	if !ok {
		return fmt.Errorf("Lease %d not found", leaseID)
	}

	if time.Now().After(lease.ExpireAt) {
		return fmt.Errorf("Lease %d already expired", leaseID)
	}

	lease.Keys[key] = struct{}{}

	if oldLease, exists := lm.kv.keyLease[key]; exists && oldLease != leaseID {
		return fmt.Errorf("Key already attached to lease %d", oldLease)
	}

	lm.kv.mu.Lock()
	lm.kv.keyLease[key] = leaseID
	lm.kv.mu.Unlock()

	return nil
}

// 延长 TTL
func (lm *LeaseManager) KeepAlive(leaseID int64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lease, ok := lm.leases[leaseID]
	if !ok {
		return fmt.Errorf("Lease %d not found", leaseID)
	}

	now := time.Now()

	// if now.After(lease.ExpireAt) {
	// 	return fmt.Errorf("Lease %d already expired", leaseID)
	// }

	lease.ExpireAt = now.Add(time.Duration(lease.TTL) * time.Second)

	return nil
}

// 自动过期
func (lm *LeaseManager) expirationLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()

		lm.mu.Lock()

		var keys []string

		for id, lease := range lm.leases {
			
			if now.After(lease.ExpireAt) {
				for key := range lease.Keys {
					keys = append(keys, key)
				}

				delete(lm.leases, id)
			}
		}

		lm.mu.Unlock()

		for _, key := range keys {

			lm.kv.mu.Lock()
			delete(lm.kv.keyLease, key)
			lm.kv.mu.Unlock()

			lm.kv.Delete(key)
		}
	}
}

// 删除租约
func (lm *LeaseManager) LeaseRevoke(leaseID int64) error {
	lm.mu.Lock()

	lease, ok := lm.leases[leaseID]
	if !ok {
		return fmt.Errorf("Lease %d not found", leaseID)
	}

	var keys []string
	for key := range lease.Keys {
		keys = append(keys, key)
	}

	delete(lm.leases, leaseID)
	lm.mu.Unlock()

	for _, key := range keys {

		lm.kv.mu.Lock()
		delete(lm.kv.keyLease, key)
		lm.kv.mu.Unlock()
		
		lm.kv.Delete(key)
	}

	return nil
}

// 5 LeaseRevoke
// 6 expirationLoop
// 7 Put 集成 lease