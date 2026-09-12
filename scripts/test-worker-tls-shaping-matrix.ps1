[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkerDomain,
    [ValidateSet("Auto", "IPv4", "IPv6")]
    [string]$IPFamily = "IPv4",
    [string]$DeviceSerial = "",
    [int]$TimeoutSeconds = 240,
    [switch]$IncludeControls
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$probeScript = Join-Path $PSScriptRoot "test-worker-network-probe.ps1"
if (-not (Test-Path $probeScript)) {
    throw "Probe script not found: $probeScript"
}

$transports = @(
    "Go",
    "GoDynamic",
    "GoSplit1200",
    "GoSplit4K",
    "GoSplit16K",
    "GoPaced4K"
)
if ($IncludeControls) {
    $transports += @("UTLS", "OkHttp")
}

Write-Host "Worker TLS shaping matrix"
Write-Host "Domain:    $WorkerDomain"
Write-Host "IP family: $IPFamily"
Write-Host "Profiles:  $($transports -join ', ')"
Write-Host ""

$first = $true
foreach ($transport in $transports) {
    Write-Host ""
    Write-Host "============================================================"
    Write-Host "Transport: $transport"
    Write-Host "============================================================"

    $args = @{
        WorkerDomain = $WorkerDomain
        IPFamily = $IPFamily
        Transport = $transport
        TimeoutSeconds = $TimeoutSeconds
    }
    if ($DeviceSerial) {
        $args.DeviceSerial = $DeviceSerial
    }
    if (-not $first) {
        $args.SkipBuild = $true
    }

    & $probeScript @args
    if ($LASTEXITCODE -ne 0) {
        throw "Probe failed to launch for transport $transport."
    }
    $first = $false
}

Write-Host ""
Write-Host "TLS shaping matrix complete."
Write-Host "Logs: $(Join-Path $repoRoot 'artifacts\worker-tests\worker-network-probe-*-*.txt')"
