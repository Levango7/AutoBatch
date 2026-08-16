#!/bin/bash
# ── AutoBatch Spark Cluster Entrypoint ─────────────────────────
# 根据 SPARK_MODE 环境变量启动 Master 或 Worker
# ───────────────────────────────────────────────────────────────

set -e

echo "=========================================="
echo " AutoBatch Spark Cluster - ${SPARK_MODE}"
echo " SPARK_HOME=${SPARK_HOME}"
echo "=========================================="

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

    echo "[WARN] Master 未能就绪，继续启动（可能后续连接）"
}

case "${SPARK_MODE}" in
    master)
        echo "[INFO] 启动 Spark Master ..."
        "${SPARK_HOME}/sbin/start-master.sh"

        echo "[INFO] Master 已启动，跟踪日志 ..."
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
        echo ">>> socat proxy started: localhost:9000 -> minio:9000"

        # 等待 Master 就绪
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