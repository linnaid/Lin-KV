package main // main 包是 kv-server 可执行程序入口。

import ( // 引入命令行解析、进程信号和单节点启动依赖。
	"flag"      // 解析 -config 等命令行参数。
	"fmt"       // 输出启动信息和错误信息。
	"os"        // 访问 stderr/stdout 并控制进程退出码。
	"os/signal" // 接收 Ctrl-C 和系统终止信号。
	"syscall"   // 提供 SIGTERM 常量。

	"etcd-KV/internal/server/cluster" // 加载节点配置并启动单节点服务。
) // import 结束。

// main 是进程入口，负责把错误转换成清晰的 stderr 输出和退出码。
func main() {
	if err := run(); err != nil { // 执行真正的启动流程，并统一处理返回错误。
		fmt.Fprintf(os.Stderr, "kv-server error: %v\n", err) // 把启动失败原因输出到 stderr。
		os.Exit(1)                                           // 用非 0 退出码告诉脚本本进程启动失败。
	} // 错误处理结束。
} // main 结束。

// run 负责解析配置、启动当前节点，并阻塞等待退出信号。
func run() error {
	configPath := flag.String("config", "", "path to node config json") // 定义 -config 参数，指向当前节点 JSON 配置文件。
	flag.Parse()                                                        // 解析命令行参数。

	if *configPath == "" { // 配置文件路径是启动节点的必需参数。
		return fmt.Errorf("missing required -config") // 返回明确的参数错误，避免继续用空配置启动。
	} // -config 校验结束。

	cfg, err := cluster.LoadNodeConfig(*configPath) // 从 JSON 文件加载并校验当前节点配置。
	if err != nil {                                 // 判断配置加载或校验是否失败。
		return fmt.Errorf("load config: %w", err) // 包装配置错误，保留原始错误链。
	} // 配置加载错误处理结束。

	node, err := cluster.StartNode(cfg) // 根据配置启动当前这一个节点。
	if err != nil {                     // 判断节点启动是否失败。
		return fmt.Errorf("start node: %w", err) // 包装启动错误，方便定位监听、持久化或 raft 初始化问题。
	} // 节点启动错误处理结束。

	defer func() { // 注册退出清理逻辑，确保 run 返回前关闭节点资源。
		if err := node.Close(); err != nil { // 关闭 raft、gRPC server、listener 和 peer 连接。
			fmt.Fprintf(os.Stderr, "close node error: %v\n", err) // 关闭失败只记录，不覆盖主流程错误。
		} // 关闭错误处理结束。
	}() // defer 注册结束。

	fmt.Printf("node %d running: peer=%s client=%s data=%s\n", cfg.ID, cfg.PeerAddr, cfg.ClientAddr, cfg.DataDir) // 输出当前节点运行信息。
	sig := waitForShutdownSignal()                                                                                // 阻塞等待 Ctrl-C 或 SIGTERM。
	fmt.Printf("node %d stopping: signal=%s\n", cfg.ID, sig.String())                                             // 输出停止原因，方便脚本和日志排查。

	return nil // 正常收到退出信号后返回成功。
} // run 结束。

// waitForShutdownSignal 负责等待进程级退出信号，并在返回前停止 signal 通知。
func waitForShutdownSignal() os.Signal {
	sigCh := make(chan os.Signal, 1)                    // 创建带缓冲的信号通道，避免信号到达时无人接收而丢失。
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM) // 订阅 Ctrl-C 和 kill 默认使用的 SIGTERM。
	defer signal.Stop(sigCh)                            // 函数返回前取消信号订阅，避免泄漏通知目标。

	return <-sigCh // 阻塞直到收到退出信号，并把信号返回给调用方记录日志。
} // waitForShutdownSignal 结束。
