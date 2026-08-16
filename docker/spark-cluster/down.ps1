# ── down.ps1 ───────────────────────────────────────────────────
# 停止并移除 AutoBatch Spark 集群容器
# ───────────────────────────────────────────────────────────────

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "[INFO] 停止 AutoBatch Spark 集群 ..." -ForegroundColor Cyan

docker compose down

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] 集群已停止并移除" -ForegroundColor Green
} else {
    Write-Host "[ERROR] 停止集群失败！" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "[提示] 如需清理镜像，执行:  docker rmi spark-cluster-spark-master spark-cluster-spark-worker-1 spark-cluster-spark-worker-2" -ForegroundColor DarkGray