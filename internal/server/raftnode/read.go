// 等待commit
package raftnode

// pendingRequest 待处理请求
type pendingRequest struct {
	index int
	ch chan struct{}
}