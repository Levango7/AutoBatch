# ── connect-minio.ps1 ──────────────────────────────────────────
# 将已运行的 MinIO 容器加入 autobatch-net 网络
# 使 Spark Worker 能通过容器名 "minio" 访问 MinIO (minio:9000)
# ───────────────────────────────────────────────────────────────

$ErrorActionPreference = "Stop"
$NetworkName = "autobatch-net"

# 查找 MinIO 容器
Write-Host "[INFO] 查找 MinIO 容器 ..." -ForegroundColor Cyan
$minioContainer = docker ps --filter "ancestor=minio/minio" --format "{{.Names}}" | Select-Object -First 1

if (-not $minioContainer) {
    # 备选：按容器名查找
    $minioContainer = docker ps --filter "name=minio" --format "{{.Names}}" | Select-Object -First 1
}

if (-not $minioContainer) {
    Write-Host "[ERROR] 未找到运行中的 MinIO 容器！请先启动 MinIO。" -ForegroundColor Red
    exit 1
}

Write-Host "[INFO] 找到 MinIO 容器: $minioContainer" -ForegroundColor Green

# 检查是否已在网络中
$existingNetworks = docker inspect $minioContainer --format "{{range .NetworkSettings.Networks}}{{.NetworkID}} {{end}}"
$targetNetworkId = docker network inspect $NetworkName --format "{{.Id}}" 2>$null

if ($existingNetworks -match [regex]::Escape($targetNetworkId)) {
    Write-Host "[INFO] MinIO 已在 ${NetworkName} 网络中，跳过连接" -ForegroundColor Yellow
} else {
    Write-Host "[INFO] 将 MinIO 加入 ${NetworkName} 网络 ..." -ForegroundColor Cyan
    docker network connect $NetworkName $minioContainer
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] MinIO 已加入 ${NetworkName} 网络" -ForegroundColor Green
    } else {
        Write-Host "[ERROR] 加入网络失败！" -ForegroundColor Red
        exit 1
    }
}

# 验证
Write-Host ""
Write-Host "[INFO] MinIO 网络信息:" -ForegroundColor Cyan
# 模板内绝不能含双引号：Windows PowerShell 5.1 向原生命令传参会剥离内嵌引号
# （CommandLineToArgvW 语义，2026-08-28 实测 {{if eq .NetworkID "<id>"}} 到达
# docker 时引号丢失 → template parsing error: bad number syntax → 脚本 exit 1，
# 导致 up.ps1 误报 connect-minio 失败）。改用无引号 range 模板枚举 网络名=IP。
$netInfo = docker inspect $minioContainer --format '{{range $name, $conf := .NetworkSettings.Networks}}{{$name}}={{$conf.IPAddress}} {{end}}'
Write-Host "  Networks: $netInfo"
if ($netInfo -notmatch [regex]::Escape($NetworkName)) {
    Write-Host "[ERROR] MinIO 未在 ${NetworkName} 网络中！" -ForegroundColor Red
    exit 1
}