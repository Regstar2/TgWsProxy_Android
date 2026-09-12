[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkerDomain,
    [ValidateSet("Auto", "IPv4", "IPv6")]
    [string]$IPFamily = "IPv4",
    [string]$DeviceSerial = "",
    [int]$TimeoutSeconds = 240
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$runner = Join-Path $PSScriptRoot "test-worker-network-probe.ps1"
$transports = @(
    "OkHttp",
    "OkHttpNoCompression",
    "OkHttpRandom",
    "OkHttpNoCompressionRandom"
)

$first = $true
foreach ($transport in $transports) {
    Write-Host ""
    Write-Host "=== $transport / $IPFamily ==="
    $args = @(
        "-WorkerDomain", $WorkerDomain,
        "-Transport", $transport,
        "-IPFamily", $IPFamily,
        "-TimeoutSeconds", $TimeoutSeconds
    )
    if ($DeviceSerial) {
        $args += @("-DeviceSerial", $DeviceSerial)
    }
    if (-not $first) {
        $args += "-SkipBuild"
    }
    & $runner @args
    if ($LASTEXITCODE -ne 0) {
        throw "Probe failed for transport $transport"
    }
    $first = $false
}

Write-Host ""
Write-Host "OkHttp compression matrix complete."
