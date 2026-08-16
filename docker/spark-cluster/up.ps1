# ── up.ps1 ─────────────────────────────────────────────────────
# 一键启动 AutoBatch Spark 集群
# 步骤: build → up → 等待 Master 就绪 → 连接 MinIO
# ───────────────────────────────────────────────────────────────

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "  AutoBatch Spark Cluster - 启动" -ForegroundColor Cyan
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: 构建并启动
Write-Host "[Step 1/4] 构建镜像并启动容器 ..." -ForegroundColor Cyan
docker compose up -d --build
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] docker compose up 失败！" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] 容器已启动" -ForegroundColor Green
Write-Host ""

# Step 2: 等待 Master Web UI 就绪
Write-Host "[Step 2/4] 等待 Spark Master 就绪 (http://localhost:8080) ..." -ForegroundColor Cyan
$MaxRetries = 30
$RetryInterval = 3
$MasterReady = $false

for ($i = 1; $i -le $MaxRetries; $i++) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080" -TimeoutSec 3 -UseBasicParsing -ErrorAction Stop
        if ($response.StatusCode -eq 200) {
            Write-Host "[OK] Spark Master Web UI 已就绪 (第 ${i} 次尝试)" -ForegroundColor Green
            $MasterReady = $true
            break
        }
    } catch {
        # 继续重试
    }
    Write-Host "  等待中 ... (${i}/${MaxRetries})" -ForegroundColor DarkGray
    Start-Sleep -Seconds $RetryInterval
}

if (-not $MasterReady) {
    Write-Host "[WARN] Master Web UI 未在预期时间内就绪，继续执行 ..." -ForegroundColor Yellow
}
Write-Host ""

# Step 3: 连接 MinIO 到集群网络
Write-Host "[Step 3/4] 连接 MinIO 到集群网络 ..." -ForegroundColor Cyan
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
& "$scriptDir\connect-minio.ps1"
Write-Host ""

# Step 4: 验证集群状态
Write-Host "[Step 4/4] 验证集群状态 ..." -ForegroundColor Cyan
Start-Sleep -Seconds 5

try {
    $workersJson = Invoke-RestMethod -Uri "http://localhost:8080/api/v1/workers" -TimeoutSec 5 -ErrorAction Stop
    $workerCount = ($workersJson | Measure-Object).Count
    Write-Host "  Master:  http://localhost:8080  [OK]" -ForegroundColor Green
    Write-Host "  Workers: ${workerCount} 个在线" -ForegroundColor Green

    foreach ($w in $workersJson) {
        $status = if ($w.status -eq "ALIVE") { "ALIVE" } else { $w.status }
        $color = if ($status -eq "ALIVE") { "Green" } else { "Red" }
        Write-Host "    - $($w.id): cores=$($w.cores), memory=$($w.memory), status=$status" -ForegroundColor $color
    }
} catch {
    Write-Host "  [WARN] 无法获取 Worker 列表，请手动检查 http://localhost:8080" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "  集群启动完成！" -ForegroundColor Green
Write-Host "  Master Web UI:  http://localhost:8080" -ForegroundColor White
Write-Host "  Master RPC:     spark://localhost:7077" -ForegroundColor White
Write-Host "  Worker-1 UI:    http://localhost:8081" -ForegroundColor White
Write-Host "  Worker-2 UI:    http://localhost:8082" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan