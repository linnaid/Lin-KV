//go:build integration

package tests // tests 包负责承载跨模块、跨进程的集成测试。

import ( // 引入真实进程集成测试需要的标准库与项目内依赖。
	"bytes"         // 比较 Get 返回值和期望值是否一致。
	"context"       // 为 go build、gRPC 调用和等待可写流程设置超时。
	"encoding/json" // 把临时节点配置编码成 JSON 文件。
	"fmt"           // 生成测试二进制、配置文件和日志文件名。
	"net"           // 动态申请真实空闲 TCP 端口。
	"os"            // 创建日志文件、配置文件并读取失败日志。
	"os/exec"       // 启动 go build 和真实 kv-server 子进程。
	"path/filepath" // 组合仓库根目录、临时目录和配置路径。
	"runtime"       // 定位当前测试文件，从而反推仓库根目录。
	"sync"          // 保护进程退出结果和 Close 只执行一次。
	"syscall"       // 向真实 kv-server 进程发送 SIGTERM 做优雅退出。
	"testing"       // 提供测试入口、断言和自动清理能力。
	"time"          // 控制构建、启动、RPC 和清理阶段的超时。

	"etcd-KV/internal/client"         // 复用项目内 gRPC 客户端封装，真实走多地址重试逻辑。
	"etcd-KV/internal/server/cluster" // 复用线上节点配置结构，避免测试配置和生产配置漂移。

	gogrpc "google.golang.org/grpc"               // 创建真实 gRPC 连接。
	"google.golang.org/grpc/credentials/insecure" // 测试环境下使用明文本地连接即可。
) // import 结束。

// 文件作用：
// 1. 构建真实 kv-server 二进制。
// 2. 动态分配 3 组真实 peer/client 端口并生成临时 JSON 配置。
// 3. 启动 3 个真实 kv-server 进程并通过真实 gRPC 连接访问它们。
// 4. 等待集群进入可写状态后，验证最基础的一次 Put / Get 流程。
// 5. 在退出时统一关闭连接、停止所有进程并依赖 t.TempDir 清理临时目录。

const ( // 把测试用到的关键超时常量集中管理，便于后续扩展或调参。
	processClusterNodeCount       = 3                // 当前第一版真实进程测试固定验证 3 节点集群。
	processClusterBuildTimeout    = 2 * time.Minute  // 构建真实 kv-server 二进制的最大等待时间。
	processClusterStartupTimeout  = 20 * time.Second // 等待子进程监听端口并让集群进入可写状态的最大时间。
	processClusterRPCDeadline     = 5 * time.Second  // 单次 Put / Get RPC 在测试里的最大等待时间。
	processClusterShutdownTimeout = 5 * time.Second  // 退出阶段等待子进程优雅结束的最大时间。
) // const 结束。

// processClusterHarness 表示一套已经启动完成的真实 3 节点测试集群。
type processClusterHarness struct {
	t           *testing.T           // 保存 testing.T，便于在清理阶段输出失败日志。
	rootDir     string               // 根临时目录，里面包含二进制、配置、日志和数据目录。
	binaryPath  string               // go build 产出的真实 kv-server 二进制路径。
	configs     []string             // 3 份临时节点 JSON 配置路径。
	clientAddrs []string             // 3 个真实 client 监听地址，用于创建 gRPC 连接。
	conns       []*gogrpc.ClientConn // 连接到 3 个真实 client addr 的 gRPC 连接。
	client      *client.Client       // 复用项目内多地址客户端，真实验证 leader 重试和路由逻辑。
	processes   []*processHandle     // 3 个真实 kv-server 子进程的运行句柄。
	closeOnce   sync.Once            // 确保 Close 只执行一次，避免重复 kill 进程。
} // processClusterHarness 结束。

// processHandle 表示一个已经启动的 kv-server 真实子进程。
type processHandle struct {
	nodeID  int           // 对应配置里的节点 ID，方便失败时快速定位日志。
	cmd     *exec.Cmd     // 启动该节点的子进程命令对象。
	logPath string        // 该节点 stdout/stderr 合并日志路径。
	done    chan struct{} // 子进程退出时关闭，便于等待退出完成。

	mu      sync.Mutex // 保护 waitErr 的并发读写。
	waitErr error      // 记录 cmd.Wait 的最终结果，便于判断是否异常退出。
} // processHandle 结束。

// reservedAddress 表示一个暂时被当前测试保留住的真实 TCP 地址。
type reservedAddress struct {
	addr     string       // 真实监听地址，格式类似 127.0.0.1:12345。
	listener net.Listener // 保留该地址的 listener，关闭后子进程才能真正绑定。
} // reservedAddress 结束。

// nodeRuntimeAddrs 表示单个节点启动时需要的一组真实地址。
type nodeRuntimeAddrs struct {
	peerAddr   string // 节点间 Raft gRPC 通信用的真实端口。
	clientAddr string // 客户端访问 KV gRPC 服务用的真实端口。
} // nodeRuntimeAddrs 结束。

// TestIntegrationProcessClusterStartsThreeNodesAndHandlesBasicReadWrite 验证真实进程集群的最小可用链路。
func TestIntegrationProcessClusterStartsThreeNodesAndHandlesBasicReadWrite(t *testing.T) {
	h := startProcessClusterHarness(t) // 构建二进制、生成配置、启动真实进程并连上 3 个 client addr。

	waitForProcessClusterWritable(t, h.client, processClusterStartupTimeout) // 先等集群真正选主并具备写入能力。

	key := "process-cluster/basic-key"       // 业务验证用 key，和就绪探针 key 分开，避免互相干扰。
	value := []byte("process-cluster-value") // 业务验证用 value，后续用 Get 读回并严格比对。

	putCtx, putCancel := context.WithTimeout(context.Background(), processClusterRPCDeadline) // 为真实 Put 设置超时，避免异常时无限等待。
	err := h.client.Put(putCtx, key, value)                                                   // 通过真实多地址 gRPC client 向集群写入一条数据。
	putCancel()                                                                               // Put 返回后立刻释放上下文资源。
	if err != nil {                                                                           // 只要 Put 失败，就说明真实进程链路仍未稳定工作。
		t.Fatalf("Put(%q) over real process cluster failed: %v", key, err) // 输出清晰错误，方便和节点日志一起定位。
	} // Put 失败处理结束。

	getCtx, getCancel := context.WithTimeout(context.Background(), processClusterRPCDeadline) // 为真实 Get 设置独立超时，避免和 Put 共享取消信号。
	got, err := h.client.Get(getCtx, key)                                                     // 通过同一套真实 gRPC 连接把刚写入的数据读回来。
	getCancel()                                                                               // Get 返回后释放上下文资源。
	if err != nil {                                                                           // Get 失败说明 leader 读路径或 ReadIndex 路径存在问题。
		t.Fatalf("Get(%q) over real process cluster failed: %v", key, err) // 直接终止，后续由 cleanup 输出日志。
	} // Get 失败处理结束。

	if !bytes.Equal(got, value) { // 严格比较读回值和写入值，确保真实部署形态下基础读写闭环成立。
		t.Fatalf("Get(%q) returned %q, want %q", key, got, value) // 输出实际值和期望值，方便排查序列化或复制问题。
	} // 返回值校验结束。
} // TestIntegrationProcessClusterStartsThreeNodesAndHandlesBasicReadWrite 结束。

// startProcessClusterHarness 负责准备一套可复用的真实 3 节点测试集群。
func startProcessClusterHarness(t *testing.T) *processClusterHarness {
	t.Helper() // 把失败位置归因到调用者，而不是辅助函数内部。

	h := &processClusterHarness{t: t} // 先创建空 harness，便于后续阶段逐步填充资源。
	shouldClose := true               // 默认认为创建过程可能中途失败，因此需要兜底清理。
	defer func() {                    // 如果中途 Fatal 或 panic，这里仍会关闭已启动的部分资源。
		if shouldClose {
			h.Close()
		}
	}() // defer 注册结束。

	h.rootDir = t.TempDir() // 用 testing 自动管理临时目录，测试结束后会统一删除。

	repoRoot := repoRootFromCurrentTestFile(t)                                             // 通过当前测试文件位置反推出仓库根目录。
	h.binaryPath = buildProcessClusterBinary(t, repoRoot, filepath.Join(h.rootDir, "bin")) // 在临时目录里构建真实 kv-server 二进制。

	nodeAddrs := reserveNodeRuntimeAddrs(t, processClusterNodeCount)               // 为 3 个节点各申请一组真实 peer/client 空闲端口。
	h.configs, h.clientAddrs = writeProcessClusterConfigs(t, h.rootDir, nodeAddrs) // 生成 3 份临时 JSON 配置，并记录 3 个 client addr。

	h.processes = startProcessClusterProcesses( // 用真实二进制和真实配置启动 3 个 kv-server 子进程。
		t,
		repoRoot,
		h.binaryPath,
		h.configs,
		filepath.Join(h.rootDir, "logs"),
	)

	h.conns, h.client = dialProcessClusterClient(t, h.clientAddrs) // 显式连上 3 个真实 client addr，并封装成多地址 client。

	t.Cleanup(h.Close)  // 把清理动作注册给 testing，确保断言失败时也会执行。
	shouldClose = false // 走到这里说明 harness 创建成功，后续交给 t.Cleanup 统一回收。

	return h // 把可用的真实进程集群返回给测试主流程。
} // startProcessClusterHarness 结束。

// Close 负责关闭连接、停止进程并在失败时输出每个节点的真实日志。
func (h *processClusterHarness) Close() {
	h.closeOnce.Do(func() { // 保证 cleanup 只执行一次，避免重复发送信号。
		for _, conn := range h.conns { // 先关闭所有 gRPC 连接，释放测试进程侧资源。
			if conn != nil {
				_ = conn.Close()
			}
		}

		for _, proc := range h.processes { // 再逐个停止真实 kv-server 子进程。
			stopProcessHandle(proc)
		}

		if h.t != nil && h.t.Failed() { // 只有测试失败时才打印节点日志，避免成功路径噪音过多。
			h.dumpProcessLogs()
		}
	}) // sync.Once 执行结束。
} // Close 结束。

// dumpProcessLogs 负责在失败时把每个节点的合并日志输出到 go test 日志里。
func (h *processClusterHarness) dumpProcessLogs() {
	for _, proc := range h.processes { // 顺序输出每个节点日志，方便定位哪台先出问题。
		if proc == nil || proc.logPath == "" {
			continue
		}

		data, err := os.ReadFile(proc.logPath) // 读取该节点启动到退出期间的完整 stdout/stderr。
		if err != nil {
			h.t.Logf("node %d log read failed: %v", proc.nodeID, err)
			continue
		}

		h.t.Logf("node %d log (%s):\n%s", proc.nodeID, proc.logPath, string(data)) // 直接把日志挂到测试输出里，方便一次看全。
	}
} // dumpProcessLogs 结束。

// buildProcessClusterBinary 负责在真实测试开始前构建 kv-server 二进制。
func buildProcessClusterBinary(t *testing.T, repoRoot string, outputDir string) string {
	t.Helper() // 把 build 失败归因到调用者。

	if err := os.MkdirAll(outputDir, 0o755); err != nil { // 先保证放置二进制的目录存在。
		t.Fatalf("MkdirAll(%q) failed: %v", outputDir, err)
	}

	binaryPath := filepath.Join(outputDir, "kv-server")                                  // 把构建产物放在当前测试自己的临时目录里。
	ctx, cancel := context.WithTimeout(context.Background(), processClusterBuildTimeout) // 防止 go build 异常卡住整条测试。
	defer cancel()                                                                       // build 结束后释放上下文资源。

	cmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/kv-server") // 真实构建生产入口二进制，而不是调用测试专用入口。
	cmd.Dir = repoRoot                                                                  // 在仓库根目录执行，保证 ./cmd/kv-server 路径稳定可解析。

	output, err := cmd.CombinedOutput() // 收集标准输出和标准错误，构建失败时一并打印出来。
	if err != nil {
		t.Fatalf("go build kv-server failed: %v\n%s", err, string(output))
	}

	return binaryPath // 返回后续启动真实子进程要用到的二进制路径。
} // buildProcessClusterBinary 结束。

// reserveNodeRuntimeAddrs 负责先保留住 3 组真实空闲端口，再把地址交给配置生成阶段。
func reserveNodeRuntimeAddrs(t *testing.T, nodeCount int) []nodeRuntimeAddrs {
	t.Helper() // 把端口分配失败归因到调用者。

	type reservedPair struct { // 局部结构体只服务于当前函数，避免把保留态暴露给外部。
		peer   reservedAddress // 当前节点的 peer 监听地址保留句柄。
		client reservedAddress // 当前节点的 client 监听地址保留句柄。
	} // reservedPair 结束。

	pairs := make([]reservedPair, nodeCount)     // 先保存所有 listener，直到全部端口准备好再统一释放。
	addrs := make([]nodeRuntimeAddrs, nodeCount) // 生成最终要写入 JSON 配置的地址切片。

	for i := 0; i < nodeCount; i++ { // 逐个节点申请真实 peer/client 端口。
		pairs[i].peer = reserveTCPAddress(t)   // 先给该节点保留一个 peer 端口。
		pairs[i].client = reserveTCPAddress(t) // 再给该节点保留一个 client 端口。
		addrs[i] = nodeRuntimeAddrs{           // 把保留下来的地址写入最终配置视图。
			peerAddr:   pairs[i].peer.addr,
			clientAddr: pairs[i].client.addr,
		}
	}

	for i := range pairs { // 全部地址拿齐以后统一释放 listener，让真实进程尽快绑定这些端口。
		if pairs[i].peer.listener != nil {
			_ = pairs[i].peer.listener.Close()
		}
		if pairs[i].client.listener != nil {
			_ = pairs[i].client.listener.Close()
		}
	}

	return addrs // 返回 3 组真实地址，供后续 JSON 配置和 gRPC 拨号使用。
} // reserveNodeRuntimeAddrs 结束。

// reserveTCPAddress 负责向操作系统申请一个当前空闲的本地 TCP 地址。
func reserveTCPAddress(t *testing.T) reservedAddress {
	t.Helper() // 把端口申请失败归因到调用者。

	lis, err := net.Listen("tcp", "127.0.0.1:0") // 用 127.0.0.1:0 让内核自动分配一个当前空闲端口。
	if err != nil {
		t.Fatalf("reserve tcp address failed: %v", err)
	}

	return reservedAddress{ // 返回地址和 listener，供调用方在真正启动进程前继续保留。
		addr:     lis.Addr().String(),
		listener: lis,
	}
} // reserveTCPAddress 结束。

// writeProcessClusterConfigs 负责根据动态端口生成 3 份真实节点 JSON 配置。
func writeProcessClusterConfigs(t *testing.T, rootDir string, addrs []nodeRuntimeAddrs) ([]string, []string) {
	t.Helper() // 把配置生成失败归因到调用者。

	configDir := filepath.Join(rootDir, "configs") // 把临时 JSON 配置集中放在同一个目录里，便于排查。
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) failed: %v", configDir, err)
	}

	peers := make([]cluster.PeerConfig, len(addrs)) // 先构建完整 peer 列表，保证 3 个节点配置一致。
	clientAddrs := make([]string, len(addrs))       // 单独记录 client addr，后面创建 gRPC 连接时直接复用。

	for i, addr := range addrs { // 先把完整 peers 列表填好，确保每份配置都引用同一份集群拓扑。
		peers[i] = cluster.PeerConfig{
			ID:         i,
			PeerAddr:   addr.peerAddr,
			ClientAddr: addr.clientAddr,
		}
		clientAddrs[i] = addr.clientAddr
	}

	paths := make([]string, len(addrs)) // 保存 3 份配置文件的绝对路径。
	for i, addr := range addrs {        // 再逐个节点生成“当前进程要启动哪一个节点”的 JSON 配置。
		cfg := cluster.NodeConfig{
			ID:         i,
			PeerAddr:   addr.peerAddr,
			ClientAddr: addr.clientAddr,
			DataDir:    filepath.Join(rootDir, "data", fmt.Sprintf("node-%d", i)),
			Peers:      peers,
		}

		data, err := json.MarshalIndent(cfg, "", "  ") // 用缩进 JSON，失败时打开文件更容易人工阅读。
		if err != nil {
			t.Fatalf("MarshalIndent(node=%d) failed: %v", i, err)
		}

		path := filepath.Join(configDir, fmt.Sprintf("node-%d.json", i)) // 配置文件名和节点 ID 一一对应。
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("WriteFile(%q) failed: %v", path, err)
		}

		paths[i] = path // 记录当前节点配置路径，后面启动真实进程时直接使用。
	}

	return paths, clientAddrs // 同时返回配置路径和 3 个 client addr。
} // writeProcessClusterConfigs 结束。

// startProcessClusterProcesses 负责用真实 kv-server 二进制启动 3 个子进程。
func startProcessClusterProcesses(
	t *testing.T,
	repoRoot string,
	binaryPath string,
	configPaths []string,
	logDir string,
) []*processHandle {
	t.Helper() // 把子进程启动失败归因到调用者。

	if err := os.MkdirAll(logDir, 0o755); err != nil { // 先保证日志目录存在，避免进程输出丢失。
		t.Fatalf("MkdirAll(%q) failed: %v", logDir, err)
	}

	processes := make([]*processHandle, len(configPaths)) // 为 3 个节点分别保存进程句柄。
	for i, configPath := range configPaths {              // 逐个节点启动真实 kv-server 进程。
		logPath := filepath.Join(logDir, fmt.Sprintf("node-%d.log", i)) // 每个节点独占一个合并日志文件。
		logFile, err := os.Create(logPath)                              // 提前创建日志文件，方便启动失败时也能读到输出。
		if err != nil {
			t.Fatalf("Create(%q) failed: %v", logPath, err)
		}

		cmd := exec.Command(binaryPath, "-config", configPath) // 真实按生产入口约定传入 -config 参数启动。
		cmd.Dir = repoRoot                                     // 固定工作目录到仓库根目录，保证路径和行为稳定。
		cmd.Stdout = logFile                                   // 标准输出写入节点专属日志文件。
		cmd.Stderr = logFile                                   // 标准错误也写入同一个日志文件，方便排查。

		if err := cmd.Start(); err != nil { // 只要 Start 失败，就说明当前节点连监听前置阶段都没过。
			_ = logFile.Close()
			t.Fatalf("start kv-server node %d failed: %v", i, err)
		}

		if err := logFile.Close(); err != nil { // 子进程拿到 fd 后，父进程可以关闭自己的文件句柄。
			t.Fatalf("close log file for node %d failed: %v", i, err)
		}

		handle := &processHandle{ // 为当前节点创建可清理、可等待、可打印日志的运行句柄。
			nodeID:  i,
			cmd:     cmd,
			logPath: logPath,
			done:    make(chan struct{}),
		}

		go func(h *processHandle) { // 后台等待子进程退出，把结果保存起来供 cleanup 判断。
			err := h.cmd.Wait()
			h.mu.Lock()
			h.waitErr = err
			h.mu.Unlock()
			close(h.done)
		}(handle)

		processes[i] = handle // 把当前节点句柄放回切片，供后续统一 stop。
	}

	return processes // 返回 3 个真实进程句柄。
} // startProcessClusterProcesses 结束。

// dialProcessClusterClient 负责连上 3 个真实 client addr，并组装多地址 gRPC client。
func dialProcessClusterClient(t *testing.T, addrs []string) ([]*gogrpc.ClientConn, *client.Client) {
	t.Helper() // 把连接失败归因到调用者。

	conns := make([]*gogrpc.ClientConn, len(addrs))    // 保存 3 个真实 gRPC 连接，便于 cleanup 时关闭。
	rpcClients := make([]client.RPCClient, len(addrs)) // 把 3 条连接包装成项目内统一的 RPCClient 接口。

	for i, addr := range addrs { // 明确逐个拨号，确保 3 个真实 client addr 都能连通。
		conn := dialProcessClientConn(t, addr)
		conns[i] = conn
		rpcClients[i] = client.NewGrpcClient(conn)
	}

	return conns, client.Make(rpcClients) // 返回底层连接和面向测试主流程的多地址 client。
} // dialProcessClusterClient 结束。

// dialProcessClientConn 负责建立一条到真实 client addr 的阻塞式 gRPC 连接。
func dialProcessClientConn(t *testing.T, addr string) *gogrpc.ClientConn {
	t.Helper() // 把拨号失败归因到调用者。

	ctx, cancel := context.WithTimeout(context.Background(), processClusterStartupTimeout) // 给单条真实连接一个明确的建连超时。
	defer cancel()                                                                         // 拨号结束后释放上下文资源。

	conn, err := gogrpc.DialContext( // 用阻塞式拨号确保返回时目标端口已经真正可连接。
		ctx,
		addr,
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
		gogrpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial real client addr %q failed: %v", addr, err)
	}

	return conn // 返回连到真实 kv-server gRPC 端口的连接。
} // dialProcessClientConn 结束。

// waitForProcessClusterWritable 负责等待真实 3 节点集群完成选主并具备写入能力。
func waitForProcessClusterWritable(t *testing.T, cli *client.Client, timeout time.Duration) {
	t.Helper() // 把等待失败归因到调用者。

	ctx, cancel := context.WithTimeout(context.Background(), timeout) // 用一个总超时包住“等集群可写”的全过程。
	err := cli.Put(ctx, "process-cluster/__ready__", []byte("ready")) // 用一条哨兵写入验证真实 leader 选举、复制和 apply 链路都已打通。
	cancel()                                                          // 无论成功失败都及时释放上下文资源。
	if err != nil {
		t.Fatalf("real process cluster did not become writable within %s: %v", timeout, err)
	}
} // waitForProcessClusterWritable 结束。

// stopProcessHandle 负责优雅关闭单个真实 kv-server 子进程，必要时回退到强制 kill。
func stopProcessHandle(proc *processHandle) {
	if proc == nil || proc.cmd == nil || proc.cmd.Process == nil { // 句柄不完整时直接返回，避免 cleanup 二次报错。
		return
	}

	if exited, _ := proc.waitResult(); exited { // 已经退出的进程不需要重复发信号。
		return
	}

	_ = proc.cmd.Process.Signal(syscall.SIGTERM) // 先发送 SIGTERM，让 kv-server 走自己的优雅关闭逻辑。

	select {
	case <-proc.done: // 能在超时前自然退出，说明优雅关闭成功。
		return
	case <-time.After(processClusterShutdownTimeout): // 超时仍未退出，则升级为强制 kill。
	}

	_ = proc.cmd.Process.Kill() // 对卡住的子进程做最后兜底，避免测试泄漏后台进程。

	select {
	case <-proc.done: // kill 后正常等到 Wait 返回即可。
	case <-time.After(processClusterShutdownTimeout): // 如果 kill 后仍未结束，就只能放弃等待，避免 cleanup 卡死。
	}
} // stopProcessHandle 结束。

// waitResult 负责无阻塞地读取子进程当前是否已经退出以及最终 Wait 错误。
func (p *processHandle) waitResult() (bool, error) {
	select {
	case <-p.done: // done 已关闭说明 cmd.Wait 已经返回。
		p.mu.Lock()
		defer p.mu.Unlock()
		return true, p.waitErr
	default: // done 尚未关闭说明子进程还在运行。
		return false, nil
	}
} // waitResult 结束。

// repoRootFromCurrentTestFile 负责根据当前测试文件路径反推出仓库根目录。
func repoRootFromCurrentTestFile(t *testing.T) string {
	t.Helper() // 把仓库根定位失败归因到调用者。

	_, file, _, ok := runtime.Caller(0) // 取到当前这个测试文件的绝对路径。
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}

	return filepath.Dir(filepath.Dir(file)) // 当前文件在 tests/ 下，所以向上两层就是仓库根目录。
} // repoRootFromCurrentTestFile 结束。
