#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Вспомогательный скрипт для сборки и запуска etl-tool в Docker.

.DESCRIPTION
  build   — собрать Docker-образ (multi-stage, ~10-15 мин первый раз)
  run     — запустить задачу (передайте аргументы sparkRun после --)
  shell   — открыть bash внутри контейнера для отладки
  logs    — показать логи последнего запуска
  clean   — удалить образ и тома

.EXAMPLES
  .\docker-run.ps1 build
  .\docker-run.ps1 run -- --config /opt/etl-tool/config/myTask.yaml
  .\docker-run.ps1 run -- --config /opt/etl-tool/config/myTask.yaml --debug true
  .\docker-run.ps1 shell
  .\docker-run.ps1 clean
#>

param(
    [Parameter(Position=0, Mandatory=$true)]
    [ValidateSet("build","run","shell","logs","clean")]
    [string]$Command,

    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$Rest
)

$ErrorActionPreference = "Stop"
$ComposeFile = Join-Path $PSScriptRoot "docker-compose.yaml"

switch ($Command) {

    "build" {
        Write-Host "==> Сборка образа etl-tool (это займёт несколько минут)..." -ForegroundColor Cyan
        docker compose -f $ComposeFile build etl-tool
        Write-Host "==> Готово!" -ForegroundColor Green
    }

    "run" {
        # Strip leading "--" separator if present
        $args = if ($Rest -and $Rest[0] -eq "--") { $Rest[1..($Rest.Length-1)] } else { $Rest }

        if (-not $args) {
            Write-Error "Укажите аргументы после --. Пример:`n  .\docker-run.ps1 run -- --config /opt/etl-tool/config/myTask.yaml"
        }

        Write-Host "==> Запуск задачи: $($args -join ' ')" -ForegroundColor Cyan
        docker compose -f $ComposeFile run --rm etl-tool @args
    }

    "shell" {
        Write-Host "==> Открываю bash в контейнере etl-tool..." -ForegroundColor Cyan
        docker compose -f $ComposeFile run --rm --entrypoint bash etl-tool
    }

    "logs" {
        docker compose -f $ComposeFile logs --tail=200 etl-tool
    }

    "clean" {
        Write-Host "==> Удаляю контейнеры, образы и тома..." -ForegroundColor Yellow
        docker compose -f $ComposeFile down -v --rmi local
        Write-Host "==> Готово!" -ForegroundColor Green
    }
}
