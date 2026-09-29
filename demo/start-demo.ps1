# Start the whole pipeline in demo mode and open the control centre.
#   .\demo\start-demo.ps1              core stack + demo control centre
#   .\demo\start-demo.ps1 -Monitoring  also Prometheus, Pushgateway, kafka-exporter, Grafana
param([switch]$Monitoring)

$ErrorActionPreference = "Continue"   # docker writes progress to stderr
Set-Location (Split-Path $PSScriptRoot -Parent)

$files = @("-f", "docker-compose.yml", "-f", "demo/docker-compose.demo.yml")
$profileArgs = @()
if ($Monitoring) { $profileArgs = @("--profile", "monitoring") }

# The control centre produces the vitals now; two producers would double every reading.
docker compose stop vital-producer

docker compose @files @profileArgs up -d
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }

Write-Host "Waiting for the control centre on http://localhost:8050 ..."
for ($i = 0; $i -lt 60; $i++) {
    try {
        Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 http://localhost:8050/demo/api/state | Out-Null
        Write-Host "Ready."
        Start-Process "http://localhost:8050"
        exit 0
    } catch { Start-Sleep -Seconds 2 }
}
Write-Warning "Control centre did not answer yet. Check: docker compose logs demo-control"
