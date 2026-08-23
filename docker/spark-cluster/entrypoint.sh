#!/bin/bash
# ── AutoBatch Spark Cluster Entrypoint ─────────────────────────
# 根据 SPARK_MODE 环境变量启动 Master 或 Worker
# 信号处理：捕获 SIGTERM/SIGINT，优雅停止 Master/Worker
# ───────────────────────────────────────────────────────────────

set -euo pipefail

echo "=========================================="
echo " AutoBatch Spark Cluster - ${SPARK_MODE}"
echo " SPARK_HOME=${SPARK_HOME}"
echo "=========================================="

# ── 信号处理：优雅停止 ──────────────────────────────────────────
# Docker stop 发送 SIGTERM；捕获后调用对应 stop 脚本，避免僵尸进程
SOCAT_PID=""
cleanup() {
    echo "[INFO] 收到终止信号，正在停止 ..."
    case "${SPARK_MODE}" in
        master)
            if [ -x "${SPARK_HOME}/sbin/stop-master.sh" ]; then
                "${SPARK_HOME}/sbin/stop-master.sh" 2>/dev/null || true
            fi
            ;;
        worker)
            if [ -x "${SPARK_HOME}/sbin/stop-worker.sh" ]; then
                "${SPARK_HOME}/sbin/stop-worker.sh" 2>/dev/null || true
            fi
            # 终止 socat 代理
            if [ -n "${SOCAT_PID}" ] && kill -0 "${SOCAT_PID}" 2>/dev/null; then
                kill "${SOCAT_PID}" 2>/dev/null || true
            fi
            ;;
    esac
    echo "[INFO] 已停止，退出"
    exit 0
}
trap cleanup TERM INT

# 等待 Master 可达（Worker 模式下）
wait_for_master() {
    if [ -z "${SPARK_MASTER_URL}" ]; then
        echo "[WARN] SPARK_MASTER_URL 未设置，跳过等待"
        return
    fi

    MAX_RETRIES=30
    RETRY_INTERVAL=2
    MASTER_HOST=$(echo "${SPARK_MASTER_URL}" | sed -E 's|spark://([^:]+):.*|\1|')

    echo "[INFO] 等待 Master (${MASTER_HOST}:7077) 可达 ..."
    for i in $(seq 1 ${MAX_RETRIES}); do
        if bash -c "echo > /dev/tcp/${MASTER_HOST}/7077" 2>/dev/null; then
            echo "[INFO] Master 已就绪 (第 ${i} 次尝试)"
            return
        fi
        echo "[INFO] 等待 Master ... (${i}/${MAX_RETRIES})"
        sleep ${RETRY_INTERVAL}
    done

    # 修复：此前仅 WARN 后继续，会导致 Worker 启动后立即因 Master 不可达而反复重连
    # 改为 exit 1，让容器进入 failed 状态，由 docker restart 策略重试
    echo "[ERROR] Master 未能就绪（${MAX_RETRIES} 次重试后仍不可达），退出"
    exit 1
}

case "${SPARK_MODE}" in
    master)
        echo "[INFO] 启动 Spark Master ..."
        "${SPARK_HOME}/sbin/start-master.sh"

        echo "[INFO] Master 已启动，跟踪日志 ..."
        # tail -f 阻塞；trap 在此期间仍可被信号中断（bash 默认行为）
        tail -f "${SPARK_HOME}/logs/"*.out 2>/dev/null || \
        tail -f /dev/null
        ;;

    worker)
        # 设置 Worker 参数
        export SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-2}"
        export SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-2g}"

        # socat 代理：localhost:9000 -> minio:9000
        # 让 Worker 用 localhost:9000 访问 MinIO（与 Driver 一致）
        socat TCP-LISTEN:9000,fork,reuseaddr TCP:minio:9000 &
        SOCAT_PID=$!
        echo ">>> socat proxy started: localhost:9000 -> minio:9000 (pid=${SOCAT_PID})"

        # 等待 Master 就绪（失败立即退出）
        wait_for_master

        MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
        echo "[INFO] 启动 Spark Worker → ${MASTER_URL}"
        echo "[INFO] Worker 配置: cores=${SPARK_WORKER_CORES}, memory=${SPARK_WORKER_MEMORY}"

        "${SPARK_HOME}/sbin/start-worker.sh" "${MASTER_URL}"

        echo "[INFO] Worker 已启动，跟踪日志 ..."
        tail -f "${SPARK_HOME}/logs/"*.out 2>/dev/null || \
        tail -f /dev/null
        ;;

    *)
        echo "[ERROR] 未知的 SPARK_MODE='${SPARK_MODE}'，仅支持 master / worker"
        exit 1
        ;;
esac
