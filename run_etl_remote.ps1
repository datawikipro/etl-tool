#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Builds etl-tool on remote server from GitHub source, then runs all STG->ODS ETL pipelines.

.DESCRIPTION
  1. git clone / pull etl-tool repo on remote server
  2. Build Docker image with sbt assembly (sbt downloads via internet)  
  3. Run all ETL pipelines as Docker containers on remote server

.USAGE
  .\run_etl_remote.ps1                  # build + run all tables
  .\run_etl_remote.ps1 -BuildOnly       # only build Docker image
  .\run_etl_remote.ps1 -RunOnly         # only run ETL (image already built)
  .\run_etl_remote.ps1 -Table wb_sales  # run single table
#>

param(
    [switch]$BuildOnly,
    [switch]$RunOnly,
    [string]$Table = ""
)

$ErrorActionPreference = "Stop"

$RemoteHost  = "chernousov_a@100.86.137.112"
$RemotePath  = "~/build/etl-tool"
$RepoUrl     = "git@github.com:datawikipro/etl-tool.git"
$ImageName   = "datawiki/etl-tool:latest"
$Branch      = "main"

$partitions = @{
    "stg_to_ods_ozon_campaigns.yaml"     = @("2026-03-02","2026-03-03","2026-03-04","2026-03-05","2026-03-06","2026-03-07","2026-03-08","2026-03-09","2026-03-10","2026-03-11","2026-03-12","2026-03-13","2026-03-14","2026-03-15","2026-03-16","2026-03-17","2026-05-14","2026-05-15","2026-05-16","2026-05-17","2026-05-18","2026-05-19","2026-05-20","2026-05-21","2026-05-22","2026-05-23","2026-05-24","2026-05-25","2026-05-27","2026-05-28","2026-05-29","2026-05-30")
    "stg_to_ods_wb_orders.yaml"          = @("2026-03-01","2026-03-05","2026-03-09","2026-03-12","2026-03-16","2026-03-20","2026-03-24","2026-03-28","2026-03-30")
    "stg_to_ods_wb_report_detail.yaml"   = @("2026-03-02","2026-03-16","2026-03-30")
    "stg_to_ods_wb_sales.yaml"           = @("2026-03-02","2026-03-04","2026-03-07","2026-03-09","2026-03-13","2026-03-16","2026-03-19","2026-03-22","2026-03-28","2026-03-30")
    "stg_to_ods_wb_stocks.yaml"          = @("2026-03-02","2026-03-06","2026-03-10","2026-03-13","2026-03-17","2026-03-21","2026-03-26","2026-03-30")
    "stg_to_ods_ym_banners.yaml"         = @("2026-03-02","2026-03-03","2026-03-04","2026-03-05","2026-03-06","2026-03-07","2026-03-08","2026-03-09","2026-03-10","2026-03-11","2026-03-12","2026-03-13","2026-03-14","2026-03-15","2026-03-16","2026-03-17","2026-03-18","2026-03-19","2026-03-20","2026-03-21","2026-03-22","2026-03-23","2026-03-24","2026-03-25","2026-03-26","2026-03-27","2026-03-28","2026-03-29","2026-03-30","2026-03-31","2026-04-01","2026-04-02","2026-04-03","2026-04-04","2026-04-05","2026-04-06","2026-04-07","2026-04-08","2026-04-09","2026-04-10","2026-04-11","2026-04-12","2026-04-13","2026-04-14","2026-04-15","2026-04-16","2026-04-17","2026-04-18","2026-04-20","2026-04-21","2026-04-22","2026-04-23","2026-04-24","2026-04-25","2026-04-26","2026-04-27","2026-04-28","2026-04-29","2026-04-30","2026-05-01","2026-05-02","2026-05-04","2026-05-05","2026-05-06","2026-05-07","2026-05-08","2026-05-09","2026-05-10","2026-05-12","2026-05-13","2026-05-14","2026-05-15","2026-05-16","2026-05-17","2026-05-18","2026-05-19","2026-05-20","2026-05-21","2026-05-22","2026-05-23","2026-05-24","2026-05-25","2026-05-26","2026-05-28","2026-05-29","2026-05-30","2026-05-31","2026-06-01","2026-06-03","2026-06-04","2026-06-05","2026-06-06","2026-06-07","2026-06-08")
    "stg_to_ods_ym_campaign_objects.yaml"= @("2026-03-02","2026-03-03","2026-03-04","2026-03-05","2026-03-06","2026-03-07","2026-03-08","2026-03-09","2026-03-10","2026-03-11","2026-03-12","2026-03-13","2026-03-14","2026-03-15","2026-03-16","2026-03-17","2026-03-18","2026-03-19","2026-03-20","2026-03-21","2026-03-22","2026-03-23","2026-03-24","2026-03-25","2026-03-26","2026-03-27","2026-03-28","2026-03-29","2026-03-30","2026-03-31","2026-04-01","2026-04-02","2026-04-03","2026-04-04","2026-04-05","2026-04-06","2026-04-07","2026-04-08","2026-04-10","2026-04-11","2026-04-12","2026-04-13","2026-04-14","2026-04-15","2026-04-16","2026-04-17","2026-04-18","2026-04-19","2026-04-20","2026-04-21","2026-04-22","2026-04-23","2026-04-24","2026-04-25","2026-04-26","2026-04-27","2026-04-28","2026-04-29","2026-04-30","2026-05-01","2026-05-02","2026-05-04","2026-05-06","2026-05-07","2026-05-08","2026-05-09","2026-05-10","2026-05-13","2026-05-14","2026-05-15","2026-05-16","2026-05-17","2026-05-18","2026-05-19","2026-05-20","2026-05-21","2026-05-22","2026-05-23","2026-05-24","2026-05-25","2026-05-26","2026-05-29","2026-05-30","2026-05-31","2026-06-01","2026-06-03","2026-06-04","2026-06-05","2026-06-06","2026-06-07","2026-06-08")
    "stg_to_ods_ym_campaigns.yaml"       = @("2026-03-02","2026-03-03","2026-03-04","2026-03-05","2026-03-06","2026-03-07","2026-03-08","2026-03-09","2026-03-10","2026-03-11","2026-03-12","2026-03-13","2026-03-14","2026-03-15","2026-03-16","2026-03-17","2026-03-18","2026-03-19","2026-03-20","2026-03-21","2026-03-22","2026-03-23","2026-03-24","2026-03-25","2026-03-26","2026-03-27","2026-03-28","2026-03-29","2026-03-30","2026-03-31","2026-04-01","2026-04-02","2026-04-03","2026-04-04","2026-04-05","2026-04-06","2026-04-07","2026-04-08","2026-04-09","2026-04-10","2026-04-11","2026-04-12","2026-04-13","2026-04-14","2026-04-15","2026-04-16","2026-04-17","2026-04-18","2026-04-20","2026-04-21","2026-04-22","2026-04-23","2026-04-24","2026-04-25","2026-04-26","2026-04-27","2026-04-28","2026-04-29","2026-04-30","2026-05-01","2026-05-02","2026-05-04","2026-05-06","2026-05-07","2026-05-08","2026-05-09","2026-05-10","2026-05-12","2026-05-13","2026-05-14","2026-05-15","2026-05-16","2026-05-17","2026-05-18","2026-05-19","2026-05-20","2026-05-21","2026-05-22","2026-05-23","2026-05-24","2026-05-25","2026-05-26","2026-05-27","2026-05-28","2026-05-29","2026-05-30","2026-05-31","2026-06-01","2026-06-03","2026-06-04","2026-06-05","2026-06-06","2026-06-07","2026-06-08")
    "stg_to_ods_ym_keywords.yaml"        = @("2026-03-02","2026-03-03","2026-03-04","2026-03-05","2026-03-06","2026-03-07","2026-03-08","2026-03-09","2026-03-10","2026-03-11","2026-03-12","2026-03-13","2026-03-14","2026-03-15","2026-03-16","2026-03-17","2026-03-18","2026-03-19","2026-03-20","2026-03-21","2026-03-22","2026-03-23","2026-03-24","2026-03-25","2026-03-26","2026-03-27","2026-03-28","2026-03-29","2026-03-30","2026-03-31","2026-04-01","2026-04-02","2026-04-03","2026-04-04","2026-04-05","2026-04-06","2026-04-07","2026-04-08","2026-04-10","2026-04-11","2026-04-12","2026-04-13","2026-04-14","2026-04-15","2026-04-16","2026-04-17","2026-04-18","2026-04-19","2026-04-20","2026-04-21","2026-04-22","2026-04-23","2026-04-24","2026-04-25","2026-04-26","2026-04-27","2026-04-28","2026-04-29","2026-04-30","2026-05-01","2026-05-02","2026-05-03","2026-05-04","2026-05-06","2026-05-07","2026-05-08","2026-05-09","2026-05-10","2026-05-12","2026-05-13","2026-05-14","2026-05-15","2026-05-16","2026-05-17","2026-05-18","2026-05-19","2026-05-20","2026-05-21","2026-05-22","2026-05-23","2026-05-24","2026-05-25","2026-05-26","2026-05-27","2026-05-28","2026-05-29","2026-05-30","2026-05-31","2026-06-01","2026-06-03","2026-06-04","2026-06-05","2026-06-06","2026-06-07","2026-06-08")
    "stg_to_ods_ym_statistics.yaml"      = @("2026-03-02","2026-03-03","2026-03-04","2026-03-05","2026-03-06","2026-03-07","2026-03-08","2026-03-09","2026-03-10","2026-03-11","2026-03-12","2026-03-13","2026-03-14","2026-03-15","2026-03-16","2026-03-17","2026-03-18","2026-03-19","2026-03-20","2026-03-21","2026-03-22","2026-03-23","2026-03-24","2026-03-25","2026-03-26","2026-03-27","2026-03-28","2026-03-29","2026-03-30","2026-03-31","2026-04-01","2026-04-02","2026-04-03","2026-04-04","2026-04-05","2026-04-06","2026-04-07","2026-04-08","2026-04-10","2026-04-11","2026-04-12","2026-04-13","2026-04-14","2026-04-15","2026-04-16","2026-04-17","2026-04-18","2026-04-20","2026-04-21","2026-04-22","2026-04-23","2026-04-24","2026-04-25","2026-04-26","2026-04-27","2026-04-28","2026-04-29","2026-04-30","2026-05-01","2026-05-02","2026-05-03","2026-05-04","2026-05-05","2026-05-06","2026-05-07","2026-05-08","2026-05-09","2026-05-10","2026-05-12","2026-05-13","2026-05-14","2026-05-15","2026-05-16","2026-05-17","2026-05-18","2026-05-19","2026-05-20","2026-05-21","2026-05-22","2026-05-23","2026-05-24","2026-05-25","2026-05-26","2026-05-28","2026-05-29","2026-05-30","2026-05-31","2026-06-01","2026-06-03","2026-06-04","2026-06-05","2026-06-06","2026-06-07","2026-06-08")
}

# ── Phase 1: Clone/update repo on remote server ───────────────────────────────
if (-not $RunOnly) {
    Write-Host "`n[Phase 1] Cloning/updating etl-tool repo on remote server..." -ForegroundColor Cyan

    # Get GitHub token from local gh CLI
    $ghToken = (& gh auth token 2>$null)
    if (-not $ghToken) {
        Write-Host "  WARNING: gh auth token not found, trying env var GITHUB_TOKEN" -ForegroundColor Yellow
        $ghToken = $env:GITHUB_TOKEN
    }
    if (-not $ghToken) {
        Write-Host "FATAL: No GitHub token. Run 'gh auth login' or set GITHUB_TOKEN env var." -ForegroundColor Red
        exit 1
    }
    $httpsUrl = "https://datawikipro:${ghToken}@github.com/datawikipro/etl-tool.git"

    $cloneCmd = @"
set -e
if [ -d $RemotePath/.git ]; then
    echo 'Updating existing repo...'
    cd $RemotePath
    git remote set-url origin $httpsUrl
    git fetch origin
    git reset --hard origin/$Branch
else
    echo 'Cloning repo...'
    mkdir -p ~/build
    git clone $httpsUrl $RemotePath
    cd $RemotePath
    git checkout $Branch
fi
echo "HEAD: `$(git log --oneline -1)"
"@
    ssh -o ServerAliveInterval=30 $RemoteHost $cloneCmd
    if ($LASTEXITCODE -ne 0) { Write-Host "FATAL: git clone/pull failed" -ForegroundColor Red; exit 1 }
    Write-Host "  Repo sync: OK" -ForegroundColor Green

    # ── Phase 2: Build Docker image on remote ─────────────────────────────────
    Write-Host "`n[Phase 2] Building Docker image on remote server (sbt assembly + Spark)..." -ForegroundColor Cyan
    Write-Host "  (~15-30 min on first build, dependencies cached on subsequent runs)" -ForegroundColor DarkGray

    $buildCmd = @"
set -e
cd $RemotePath
echo '=== Building Docker image (sbt assembly + Spark runtime) ==='
docker build --provenance=false -f Dockerfile.build -t $ImageName . 2>&1
echo '=== Build complete ==='
docker images $ImageName
"@
    ssh -o ServerAliveInterval=30 -o ServerAliveCountMax=20 $RemoteHost $buildCmd
    if ($LASTEXITCODE -ne 0) { Write-Host "FATAL: Docker build failed" -ForegroundColor Red; exit 1 }
    Write-Host "  Docker build: OK" -ForegroundColor Green

    if ($BuildOnly) {
        Write-Host "`nBuild complete. Run .\run_etl_remote.ps1 -RunOnly to execute ETL." -ForegroundColor Green
        exit 0
    }
}

# ── Phase 3: Run ETL pipelines on remote server ───────────────────────────────
Write-Host "`n[Phase 3] Launching ETL pipelines on remote server..." -ForegroundColor Cyan

$selectedPipelines = $partitions.Keys | Sort-Object
if ($Table -ne "") {
    $selectedPipelines = $selectedPipelines | Where-Object { $_ -like "*$Table*" }
    if (-not $selectedPipelines) {
        Write-Host "ERROR: No pipeline matching '$Table'" -ForegroundColor Red; exit 1
    }
}

Write-Host "  Pipelines: $($selectedPipelines.Count)" -ForegroundColor DarkGray

$containerNames = @()
foreach ($cfg in $selectedPipelines) {
    $parts          = $partitions[$cfg] -join " "
    $containerName  = "etl-" + ($cfg -replace "stg_to_ods_|\.yaml","")
    $containerNames += $containerName

    $runCmd = @"
docker rm -f $containerName 2>/dev/null || true
docker run -d --name $containerName --restart=no \
  --entrypoint bash \
  -e SPARK_LOCAL_HOSTNAME=localhost \
  -e SPARK_LOCAL_IP=127.0.0.1 \
  -e 'JAVA_TOOL_OPTIONS=-Xmx6g -Xms2g -Dfile.encoding=UTF-8' \
  $ImageName \
  /app/run_all_partitions.sh $cfg $parts
"@
    ssh $RemoteHost $runCmd | Out-Null
    Write-Host "  Started: $containerName" -ForegroundColor Green
}

Write-Host "`n=== All pipelines launched on remote server ===" -ForegroundColor Green
Write-Host ""
Write-Host "Monitor progress:" -ForegroundColor Cyan
Write-Host "  ssh $RemoteHost 'docker ps --filter name=etl-'" -ForegroundColor DarkGray
Write-Host "  ssh $RemoteHost 'docker logs -f etl-wb_orders'" -ForegroundColor DarkGray
Write-Host "  ssh $RemoteHost 'docker logs etl-ym_campaigns | grep -E ""OK|FAIL|Done""'" -ForegroundColor DarkGray
Write-Host ""
Write-Host "Container names:" -ForegroundColor Cyan
$containerNames | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
