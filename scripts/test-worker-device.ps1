[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [string]$DeviceSerial = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

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
if ($LASTEXITCODE -ne 0) {
    throw "APK installation failed. Check the adb error; existing app data was not removed."
}

$logDir = Join-Path $repoRoot "artifacts\worker-tests"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runtimeLog = Join-Path $logDir "worker-runtime-$stamp.txt"
$adbLog = Join-Path $logDir "adb-$stamp.txt"
$buildLog = Join-Path $logDir "build-$stamp.txt"
$commit = (& git rev-parse HEAD).Trim()
$model = (& $adb -s $DeviceSerial shell getprop ro.product.model).Trim()
$androidVersion = (& $adb -s $DeviceSerial shell getprop ro.build.version.release).Trim()
@(
    "commit=$commit",
    "apk=$apk",
    "apk_sha256=$((Get-FileHash $apk -Algorithm SHA256).Hash)",
    "device=$model",
    "android=$androidVersion",
    "skip_build=$SkipBuild"
) | Set-Content -Encoding UTF8 $buildLog

$capture = Start-Process -FilePath $adb -ArgumentList @(
    "-s", $DeviceSerial, "logcat", "-v", "threadtime", "-T", "1",
    "TgWsProxy:V", "AndroidRuntime:E", "*:S"
) -RedirectStandardOutput $runtimeLog -RedirectStandardError $adbLog -PassThru -NoNewWindow
try {
    & $adb -s $DeviceSerial shell am start -n "com.amurcanov.tgwsproxy/.MainActivity"
    if ($LASTEXITCODE -ne 0) { throw "Could not open TgWsProxy." }
    Write-Host "Use the updated Worker (worker_revision=worker-stream-v2)."
    Write-Host "This APK tests fixed TLS records; confirm tls_record_sizing=fixed in transport ready."
    Write-Host "Enable runtime logging, Worker only, preconnect OFF, PRESERVE_ORIGINAL_DST."
    Write-Host "Restart the proxy, connect Telegram, upload and download a file larger than 20 MiB."
    Write-Host "Keep Cloudflare live logs open with WORKER_DIAGNOSTICS=1."
    Write-Host "Android log: $runtimeLog"
    Read-Host "After testing, press Enter to stop log capture" | Out-Null
}
finally {
    if (-not $capture.HasExited) { Stop-Process -Id $capture.Id -ErrorAction SilentlyContinue }
    Write-Host "Runtime log: $runtimeLog"
    Write-Host "Build metadata: $buildLog"
    Write-Host "adb errors: $adbLog"
}
