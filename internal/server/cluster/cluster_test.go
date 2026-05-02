package cluster // cluster 测试与被测代码同包，方便直接构造 NodeConfig。

import ( // 引入测试配置读写需要的标准库。
	"encoding/json" // 把测试配置编码成 JSON 文件内容。
	"os"            // 把测试 JSON 写入临时目录。
	"path/filepath" // 拼接临时配置文件路径。
	"strings"       // 检查错误信息是否包含关键上下文。
	"testing"       // Go 标准测试框架。
) // import 结束。

// validNodeConfig 构造一份最小可用的三节点配置，用作其他测试的基线。
func validNodeConfig() NodeConfig {
	return NodeConfig{ // 返回当前节点为 node-0 的有效配置。
		ID:         0,                // 当前节点 ID 对应 peers[0]。
		PeerAddr:   "127.0.0.1:8001", // 当前节点监听 Raft peer RPC 的地址。
		ClientAddr: "127.0.0.1:9001", // 当前节点监听 KV client RPC 的地址。
		DataDir:    "data/node-0",    // 当前节点持久化数据目录。
		Peers: []PeerConfig{ // 完整集群 peer 列表，顺序必须和 raft peer 下标一致。
			{ID: 0, PeerAddr: "127.0.0.1:8001", ClientAddr: "127.0.0.1:9001"}, // node-0 的地址配置。
			{ID: 1, PeerAddr: "127.0.0.1:8002", ClientAddr: "127.0.0.1:9002"}, // node-1 的地址配置。
			{ID: 2, PeerAddr: "127.0.0.1:8003", ClientAddr: "127.0.0.1:9003"}, // node-2 的地址配置。
		}, // Peers 字段结束。
	} // NodeConfig 返回结束。
} // validNodeConfig 结束。

// writeNodeConfig 把测试配置写成 JSON 文件，并返回文件路径。
func writeNodeConfig(t *testing.T, cfg NodeConfig) string {
	t.Helper() // 标记测试 helper，让失败行号指向调用处。

	data, err := json.Marshal(cfg) // 把 NodeConfig 编码为 JSON 字节。
	if err != nil {                // 判断测试配置是否编码失败。
		t.Fatalf("json.Marshal(NodeConfig) error = %v", err) // 编码失败说明测试数据本身有问题。
	} // JSON 编码错误处理结束。

	path := filepath.Join(t.TempDir(), "node.json")         // 在测试临时目录中生成配置文件路径。
	if err := os.WriteFile(path, data, 0o644); err != nil { // 把 JSON 配置写到临时文件。
		t.Fatalf("WriteFile(config) error = %v", err) // 写文件失败直接终止测试。
	} // 写文件错误处理结束。

	return path // 返回可传给 LoadNodeConfig 的配置文件路径。
} // writeNodeConfig 结束。

// TestNodeConfigValidateAcceptsValidConfig 验证一份完整配置可以通过校验。
func TestNodeConfigValidateAcceptsValidConfig(t *testing.T) {
	cfg := validNodeConfig() // 准备一份合法配置。

	if err := cfg.Validate(); err != nil { // 调用配置校验逻辑。
		t.Fatalf("Validate() error = %v", err) // 合法配置不应该返回错误。
	} // 校验结果判断结束。
} // TestNodeConfigValidateAcceptsValidConfig 结束。

// TestNodeConfigValidateRejectsMissingPeers 验证缺少 peers 时会被拒绝。
func TestNodeConfigValidateRejectsMissingPeers(t *testing.T) {
	cfg := validNodeConfig() // 先基于合法配置构造测试输入。
	cfg.Peers = nil          // 删除 peers，模拟配置文件缺少集群成员列表。

	err := cfg.Validate() // 调用配置校验逻辑。
	if err == nil {       // 缺少 peers 必须返回错误。
		t.Fatal("Validate() accepted config without peers") // 如果没报错，说明会在后面访问 peers[c.ID] 时埋隐患。
	} // nil 错误判断结束。
} // TestNodeConfigValidateRejectsMissingPeers 结束。

// TestNodeConfigValidateRejectsMismatchedSelfPeer 验证当前节点地址必须和 peers 中自己的条目一致。
func TestNodeConfigValidateRejectsMismatchedSelfPeer(t *testing.T) {
	cfg := validNodeConfig()         // 先准备一份合法配置。
	cfg.PeerAddr = "127.0.0.1:18001" // 改坏当前节点 peer_addr，让它和 peers[0] 不一致。

	err := cfg.Validate() // 调用配置校验逻辑。
	if err == nil {       // 自身地址不一致必须返回错误。
		t.Fatal("Validate() accepted mismatched self peer_addr") // 如果没报错，不同进程可能用不一致的自描述启动。
	} // nil 错误判断结束。

	if !strings.Contains(err.Error(), "self peer_addr") { // 检查错误信息是否指出 self peer_addr 问题。
		t.Fatalf("Validate() error = %v, want self peer_addr context", err) // 错误上下文不清晰会增加排障成本。
	} // 错误信息判断结束。
} // TestNodeConfigValidateRejectsMismatchedSelfPeer 结束。

// TestLoadNodeConfigLoadsValidJSON 验证 LoadNodeConfig 能读取 JSON 并复用 Validate 校验。
func TestLoadNodeConfigLoadsValidJSON(t *testing.T) {
	want := validNodeConfig()        // 准备期望加载出的配置。
	path := writeNodeConfig(t, want) // 把配置写入临时 JSON 文件。

	got, err := LoadNodeConfig(path) // 从文件加载配置。
	if err != nil {                  // 合法 JSON 不应该加载失败。
		t.Fatalf("LoadNodeConfig() error = %v", err) // 打印加载错误方便定位。
	} // 加载错误处理结束。

	if got.ID != want.ID { // 校验当前节点 ID 是否正确恢复。
		t.Fatalf("ID = %d, want %d", got.ID, want.ID) // ID 不一致说明 JSON 映射有问题。
	} // ID 校验结束。

	if got.PeerAddr != want.PeerAddr { // 校验当前节点 peer 地址是否正确恢复。
		t.Fatalf("PeerAddr = %q, want %q", got.PeerAddr, want.PeerAddr) // peer 地址不一致会影响节点间通信。
	} // PeerAddr 校验结束。

	if got.ClientAddr != want.ClientAddr { // 校验当前节点 client 地址是否正确恢复。
		t.Fatalf("ClientAddr = %q, want %q", got.ClientAddr, want.ClientAddr) // client 地址不一致会影响客户端连接。
	} // ClientAddr 校验结束。

	if len(got.Peers) != len(want.Peers) { // 校验完整 peers 数量是否正确恢复。
		t.Fatalf("len(Peers) = %d, want %d", len(got.Peers), len(want.Peers)) // peers 数量不一致会影响 raft peer 数组。
	} // peers 数量校验结束。
} // TestLoadNodeConfigLoadsValidJSON 结束。
