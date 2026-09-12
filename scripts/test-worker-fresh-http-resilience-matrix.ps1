[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkerDomain,
    [ValidateSet("Auto", "IPv4", "IPv6")]
    [string]$IPFamily = "IPv4",
    [switch]$IncludeBaseline,
    [string]$DeviceSerial = "",
    [int]$TimeoutSeconds = 420
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$runner = Join-Path $PSScriptRoot "test-worker-network-probe.ps1"
if (-not (Test-Path $runner)) { throw "Probe runner not found: $runner" }

$profiles = @()
if ($IncludeBaseline) { $profiles += "FreshHTTP" }
$profiles += @("FreshHTTPRetry8K", "FreshHTTPPaced8K", "FreshHTTPRetry12K")

$first = $true
foreach ($profile in $profiles) {
    Write-Host ""
    Write-Host "=== $profile / $IPFamily ==="
    $params = @{
        WorkerDomain = $WorkerDomain
        IPFamily = $IPFamily
        Transport = $profile
        TimeoutSeconds = $TimeoutSeconds
    }
    if ($DeviceSerial) { $params.DeviceSerial = $DeviceSerial }
    if (-not $first) { $params.SkipBuild = $true }
    & $runner @params
    if ($LASTEXITCODE -ne 0) { throw "$profile probe failed with exit code $LASTEXITCODE" }
    $first = $false
}

Write-Host ""
Write-Host "Fresh HTTP resilience matrix completed."
