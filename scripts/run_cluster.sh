# 本地构建并启动 3 个独立 kv-server 进程
# 验证"一进程一节点"的集群部署方式

#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT_DIR}/bin"
BIN="${BIN_DIR}/kv-server"
LOG_DIR="${ROOT_DIR}/data/logs"

mkdir -p "${BIN_DIR}" "${LOG_DIR}"

cd "${ROOT_DIR}"

go build -o "${BIN}" ./cmd/kv-server

pids=()

# 负责脚本推出时停止所有节点
cleanup() {
    for pid in "${pids[@]}"; do
        if kill -0 "${pid}" 2>/dev/null; then
            kill "${pid}" 2>/dev/null || true
        fi
    done
}

trap cleanup INT TERM EXIT

for id in 0 1 2; do
    "${BIN}" -config "${ROOT_DIR}/configs/node-${id}.json" > "${LOG_DIR}/node-${id}.log" 2>&1 &
    pids+=("$!")
    echo "started node ${id}, pid=$!, log=${LOG_DIR}/node-${id}.log"
done

wait "${pids[@]}"