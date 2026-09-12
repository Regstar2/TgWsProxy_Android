[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

function Invoke-CheckedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label,

        [Parameter(Mandatory = $true)]
        [string]$FilePath,

        [string[]]$Arguments = @(),

        [string]$WorkingDirectory = $root
    )

    Write-Host "`n==> $Label"
    Push-Location $WorkingDirectory
    try {
        & $FilePath @Arguments
        $exitCode = $LASTEXITCODE
        if ($exitCode -ne 0) {
            throw "$Label failed with exit code $exitCode."
        }
    }
    finally {
        Pop-Location
    }
}

$go = Get-Command go -ErrorAction Stop
$pythonLauncher = Get-Command py -ErrorAction Stop
$windowsPowerShell = Get-Command powershell.exe -ErrorAction Stop
$gradle = Join-Path $root 'gradlew.bat'
if (-not (Test-Path $gradle)) {
    throw "Gradle wrapper not found: $gradle"
}

$nativeDir = Join-Path $root 'native\tgwsproxy'
if (-not (Test-Path $nativeDir)) {
    throw "Native runtime directory not found: $nativeDir"
}

$pythonRequirements = Join-Path $PSScriptRoot 'requirements-ci.txt'
if (-not (Test-Path $pythonRequirements)) {
    throw "Python CI requirements not found: $pythonRequirements"
}

$releaseAudit = Join-Path $PSScriptRoot 'audit-release.ps1'
if (-not (Test-Path $releaseAudit)) {
    throw "Release audit script not found: $releaseAudit"
}

$releaseScript = Join-Path $PSScriptRoot 'release.ps1'
if (-not (Test-Path $releaseScript)) {
    throw "Release script not found: $releaseScript"
}

Write-Host "`n==> Audit release metadata, localization and tracked private files"
& $releaseAudit -ExpectedVersion '1.10.13' -ExpectedVersionCode 51
if ($LASTEXITCODE -ne 0) {
    throw "Release audit failed with exit code $LASTEXITCODE."
}

Invoke-CheckedCommand `
    -Label 'Validate release audit under Windows PowerShell 5.1' `
    -FilePath $windowsPowerShell.Source `
    -Arguments @(
        '-NoProfile',
        '-ExecutionPolicy', 'Bypass',
        '-File', $releaseAudit,
        '-ExpectedVersion', '1.10.13',
        '-ExpectedVersionCode', '51'
    )

Write-Host "`n==> Validate release-script metadata preflight"
& $releaseScript -Version 'v1.10.13' -PreflightOnly
if ($LASTEXITCODE -ne 0) {
    throw "Release-script preflight failed with exit code $LASTEXITCODE."
}

Invoke-CheckedCommand -Label 'Install Python build dependencies' -FilePath $pythonLauncher.Source -Arguments @('-3', '-m', 'pip', 'install', '--disable-pip-version-check', '--requirement', $pythonRequirements)
Invoke-CheckedCommand -Label 'Verify Go module' -FilePath $go.Source -Arguments @('mod', 'verify') -WorkingDirectory $nativeDir
Invoke-CheckedCommand -Label 'Run native Go tests' -FilePath $go.Source -Arguments @('test', './...') -WorkingDirectory $nativeDir
Invoke-CheckedCommand -Label 'Run Android unit tests and build debug APK' -FilePath $gradle -Arguments @('--no-daemon', 'testDebugUnitTest', 'assembleDebug', '--stacktrace')

$debugApk = Join-Path $root 'app\build\outputs\apk\debug\app-debug.apk'
if (-not (Test-Path $debugApk)) {
    throw "Expected debug APK was not produced: $debugApk"
}

$nativeLib = Join-Path $root 'app\src\main\jniLibs\arm64-v8a\libtgwsproxy.so'
if (-not (Test-Path $nativeLib)) {
    throw "Expected native library was not produced: $nativeLib"
}

$auditScript = Join-Path $PSScriptRoot 'audit-apk-icons.ps1'
if (-not (Test-Path $auditScript)) {
    throw "APK audit script not found: $auditScript"
}

Write-Host "`n==> Audit packaged APK resources"
& $auditScript -ApkPath $debugApk
if ($LASTEXITCODE -ne 0) {
    throw "APK audit failed with exit code $LASTEXITCODE."
}

Write-Host "`nCI checks completed successfully."
Write-Host "Debug APK: $debugApk"
