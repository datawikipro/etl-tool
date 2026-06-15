#!/usr/bin/env pwsh
# This script runs all remaining stg -> ods ETL pipelines sequentially to avoid remote server out-of-memory errors.

$ErrorActionPreference = "Stop"

$tables = @(
    "wb_report_detail",
    "wb_sales",
    "wb_stocks",
    "ym_banners",
    "ym_campaign_objects",
    "ym_campaigns",
    "ym_keywords",
    "ym_statistics"
)

Write-Host "=== Starting Sequential ETL Pipeline Runs ===" -ForegroundColor Cyan

foreach ($t in $tables) {
    Write-Host "`n--------------------------------------------------" -ForegroundColor DarkGray
    Write-Host "Starting pipeline: $t..." -ForegroundColor Green
    
    # Run the ETL tool remote script for the current table
    # We use -RunOnly because the image is already built
    .\run_etl_remote.ps1 -Table $t -RunOnly
    
    Write-Host "Waiting for etl-$t to complete..." -ForegroundColor Yellow
    ssh chernousov_a@100.86.137.112 "docker wait etl-$t"
    
    Write-Host "Checking final status of etl-$t..." -ForegroundColor Cyan
    $status = ssh chernousov_a@100.86.137.112 "docker inspect etl-$t --format '{{.State.ExitCode}}'"
    if ($status -eq "0") {
        Write-Host "SUCCESS: $t completed with exit code 0" -ForegroundColor Green
    } else {
        Write-Host "FAILED: $t completed with non-zero exit code: $status" -ForegroundColor Red
    }
}

Write-Host "`n=== All pipelines completed ===" -ForegroundColor Cyan
