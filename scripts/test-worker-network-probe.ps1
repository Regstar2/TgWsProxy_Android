[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkerDomain,
    [ValidateSet("Auto", "IPv4", "IPv6")]
    [string]$IPFamily = "Auto",
    [ValidateSet("Go", "OkHttp", "UTLS")]
    [string]$Transport = "Go",
    [switch]$SkipBuild,
    [string]$DeviceSerial = "",
    [int]$TimeoutSeconds = 240
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$WorkerDomain = $WorkerDomain.Trim()
if (-not $WorkerDomain) { throw "WorkerDomain is required." }
$familyArg = $IPFamily.ToLowerInvariant()
$transportArg = $Transport.ToLowerInvariant()

$sdkCandidates = @(
    $env:ANDROID_SDK_ROOT,
    $env:ANDROID_HOME,
    "C:\Android\SDK"
)
if ($env:LOCALAPPDATA) {
    $sdkCandidates += (Join-Path $env:LOCALAPPDATA "Android\Sdk")
}
$adb = $null
foreach ($candidate in $sdkCandidates) {
    if ($candidate -and (Test-Path (Join-Path $candidate "platform-tools\adb.exe"))) {
        $env:ANDROID_SDK_ROOT = $candidate
        $env:ANDROID_HOME = $candidate
        $adb = Join-Path $candidate "platform-tools\adb.exe"
        break
    }
}
if (-not $adb) { throw "adb.exe not found. Set ANDROID_SDK_ROOT to your Android SDK." }

if (-not $SkipBuild) {
    & (Join-Path $PSScriptRoot "build-apk.ps1") -Configuration Debug
    if ($LASTEXITCODE -ne 0) { throw "Debug APK build failed." }
}
$apk = Join-Path $repoRoot "app\build\outputs\apk\debug\app-debug.apk"
if (-not (Test-Path $apk)) { throw "Debug APK not found: $apk" }

$deviceLines = & $adb devices
if ($LASTEXITCODE -ne 0) { throw "adb devices failed." }
$devices = @($deviceLines | Where-Object { $_ -match '^\S+\s+device$' } |
    ForEach-Object { ($_ -split '\s+')[0] })
if ($DeviceSerial) {
    if ($DeviceSerial -notin $devices) { throw "Device is not connected/authorized: $DeviceSerial" }
} else {
    if ($devices.Count -ne 1) { throw "Connect one authorized device or pass -DeviceSerial." }
    $DeviceSerial = $devices[0]
}

& $adb -s $DeviceSerial install -r $apk
if ($LASTEXITCODE -ne 0) { throw "APK installation failed." }

$logDir = Join-Path $repoRoot "artifacts\worker-tests"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runtimeLog = Join-Path $logDir "worker-network-probe-$transportArg-$familyArg-$stamp.txt"
$metaLog = Join-Path $logDir "worker-network-probe-$transportArg-$familyArg-$stamp.meta.txt"
$commit = (& git rev-parse HEAD).Trim()
$model = (& $adb -s $DeviceSerial shell getprop ro.product.model).Trim()
$androidVersion = (& $adb -s $DeviceSerial shell getprop ro.build.version.release).Trim()
@(
    "commit=$commit",
    "worker_domain=$WorkerDomain",
    "ip_family=$familyArg",
    "transport=$transportArg",
    "apk=$apk",
    "apk_sha256=$((Get-FileHash $apk -Algorithm SHA256).Hash)",
    "device=$model",
    "android=$androidVersion",
    "skip_build=$SkipBuild"
) | Set-Content -Encoding UTF8 $metaLog

Write-Host "IMPORTANT: diagnostic /diag/* endpoints must already be deployed to the same Worker."
Write-Host "Running probe against: $WorkerDomain"
Write-Host "Transport: $transportArg"
Write-Host "IP family: $familyArg"

& $adb -s $DeviceSerial logcat -c
& $adb -s $DeviceSerial shell am force-stop com.amurcanov.tgwsproxy
& $adb -s $DeviceSerial shell am start -n "com.amurcanov.tgwsproxy/.WorkerNetworkProbeActivity" --es domain $WorkerDomain --es family $familyArg --es transport $transportArg
if ($LASTEXITCODE -ne 0) { throw "Could not start WorkerNetworkProbeActivity." }

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$complete = $false
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 2
    $dump = & $adb -s $DeviceSerial logcat -d -v threadtime "TgWsProxy:V" "TgWsProxyProbe:V" "AndroidRuntime:E" "*:S"
    $dump | Set-Content -Encoding UTF8 $runtimeLog
    if ($dump -match 'PROBE_DONE') {
        $complete = $true
        break
    }
}

$finalDump = & $adb -s $DeviceSerial logcat -d -v threadtime "TgWsProxy:V" "TgWsProxyProbe:V" "AndroidRuntime:E" "*:S"
$finalDump | Set-Content -Encoding UTF8 $runtimeLog

Write-Host ""
Write-Host "=== Probe summary ==="
$finalDump | Select-String "Worker network probe|MTProto Worker transport ready|PROBE_" | ForEach-Object { $_.Line }
Write-Host ""
Write-Host "Runtime log: $runtimeLog"
Write-Host "Metadata:    $metaLog"

if (-not $complete) {
    throw "Probe did not finish within $TimeoutSeconds seconds. Inspect $runtimeLog for the last successful size and transport diagnostics."
}
