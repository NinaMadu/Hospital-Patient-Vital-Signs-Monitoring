# Leave demo mode: stop the control centre and bring back the normal always-on producer.
#   .\demo\stop-demo.ps1          back to the normal stack (keeps all data)
#   .\demo\stop-demo.ps1 -All     stop every container (data is kept in the volumes)
param([switch]$All)

$ErrorActionPreference = "Continue"   # docker writes progress to stderr
Set-Location (Split-Path $PSScriptRoot -Parent)
$files = @("-f", "docker-compose.yml", "-f", "demo/docker-compose.demo.yml")

docker compose @files rm -sf demo-control
if ($All) {
    docker compose --profile monitoring stop
} else {
    # Recreates airflow without the demo's API setting and starts vital-producer again.
    docker compose up -d
}
