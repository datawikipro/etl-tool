#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Copies all STG tables to ODS via etl-tool running in Docker (local mode).
  Uses Hadoop Iceberg catalog - no Hive Metastore needed.
  
.USAGE
  .\run_ods_copy_docker.ps1                          # all tables, default date
  .\run_ods_copy_docker.ps1 -Partition 2026-03-01   # specific partition
  .\run_ods_copy_docker.ps1 -Table stg_to_ods_wb_orders  # single table
#>

param(
    [string]$Partition = "2026-01-01",
    [string]$Table = ""   # filter by config name, empty = all
)

$ErrorActionPreference = "Stop"
$EtlDir = "C:\Users\chernousov_a\IdeaProjects\etl\etl-tool"
$JarPath = "$EtlDir\target\scala-3.4.2\etl-tool.jar"

# Check JAR exists
if (-not (Test-Path $JarPath)) {
    Write-Error "etl-tool.jar not found at $JarPath. Build it first with: sbt assembly"
}

# Pipeline configs to run (using hadoop catalog connection)
$pipelines = @(
    "stg_to_ods_wb_orders.yaml",
    "stg_to_ods_wb_report_detail.yaml",
    "stg_to_ods_wb_sales.yaml",
    "stg_to_ods_wb_stocks.yaml",
    "stg_to_ods_ym_banners.yaml",
    "stg_to_ods_ym_campaign_objects.yaml",
    "stg_to_ods_ym_campaigns.yaml",
    "stg_to_ods_ym_keywords.yaml",
    "stg_to_ods_ym_statistics.yaml",
    "yandex_stg_to_ods_campaigns.yaml"
)

# Filter if table specified
if ($Table) {
    $pipelines = $pipelines | Where-Object { $_ -like "*$Table*" }
    if (-not $pipelines) {
        Write-Error "No pipeline found matching: $Table"
    }
}

Write-Host "=== ODS Copy via Docker ===" -ForegroundColor Cyan
Write-Host "Partition: $Partition"
Write-Host "Pipelines: $($pipelines.Count)"
Write-Host ""

$success = 0
$failed  = 0
$t_start = Get-Date

foreach ($cfg in $pipelines) {
    Write-Host "[$([int](($pipelines.IndexOf($cfg)+1)))/$($pipelines.Count)] Running: $cfg" -ForegroundColor Yellow
    $t0 = Get-Date

    # Override connection in pipeline config to use Hadoop catalog
    # We pass configs/ as a volume so the container sees local configs
    $dockerArgs = @(
        "run", "--rm",
        # Mount etl-tool directory with compiled JAR and configs
        "-v", "${EtlDir}:/build/etl-tool",
        # Mount spark config
        "-v", "${EtlDir}\spark-conf:/opt/spark/conf:ro",
        # S3 SSL cert
        "-v", "${EtlDir}\yandex_s3.crt:/app/yandex_s3.crt:ro",
        # Environment
        "-e", "SPARK_LOCAL_HOSTNAME=localhost",
        "-e", "SPARK_LOCAL_IP=127.0.0.1",
        "-e", "JAVA_TOOL_OPTIONS=-Xmx6g -Xms2g -Dcom.amazonaws.sdk.disableCertChecking=true -Dfile.encoding=UTF-8",
        # Use run_etl.sh which imports S3 cert then runs etl-tool.jar
        "datawiki/etl-tool-dev:local",
        "bash", "/build/etl-tool/run_etl.sh",
        "--config", "/build/etl-tool/configs/pipelines/$cfg",
        "--partition", $Partition,
        "--run_id", "manual_$(Get-Date -Format 'yyyyMMddHHmm')"
    )

    try {
        $output = & docker @dockerArgs
        $exit = $LASTEXITCODE
        if ($exit -eq 0) {
            $elapsed = [math]::Round(((Get-Date) - $t0).TotalSeconds, 1)
            Write-Host "  OK in ${elapsed}s" -ForegroundColor Green
            $success++
        } else {
            Write-Host "  FAILED (exit $exit)" -ForegroundColor Red
            $output | Select-Object -Last 20 | ForEach-Object { Write-Host "  $_" }
            $failed++
        }
    } catch {
        Write-Host "  ERROR: $_" -ForegroundColor Red
        $failed++
    }
    Write-Host ""
}

$total = [math]::Round(((Get-Date) - $t_start).TotalSeconds, 1)
Write-Host "=== Done in ${total}s: $success OK, $failed FAILED ===" -ForegroundColor Cyan
