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

    # Use hashtable splatting so PowerShell binds these as named parameters.
    # Array splatting would pass '-WorkerDomain', the domain, etc. positionally,
    # causing the domain value to be bound to IPFamily.
    $runnerArgs = @{
        WorkerDomain   = $WorkerDomain
        Transport      = $transport
        IPFamily       = $IPFamily
        TimeoutSeconds = $TimeoutSeconds
    }
    if ($DeviceSerial) {
        $runnerArgs.DeviceSerial = $DeviceSerial
    }
    if (-not $first) {
        $runnerArgs.SkipBuild = $true
    }

    & $runner @runnerArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Probe failed for transport $transport"
    }
    $first = $false
}

Write-Host ""
Write-Host "OkHttp compression matrix complete."
