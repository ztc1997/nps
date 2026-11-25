# One-click cross-compile and package Linux amd64 server (nps)
# Usage: powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\build.linux_amd64.server.ps1
# Optional env vars: OUTPUT_DIR=dist ARCHIVE_NAME=linux_amd64_server.tar.gz
$ErrorActionPreference = 'Stop'

$Root = $PSScriptRoot
Set-Location $Root

$OutputDir = if ($env:OUTPUT_DIR) { $env:OUTPUT_DIR } else { 'dist' }
$ArchiveName = if ($env:ARCHIVE_NAME) { $env:ARCHIVE_NAME } else { 'linux_amd64_server.tar.gz' }
if (-not [System.IO.Path]::IsPathRooted($OutputDir)) {
    $OutputDir = Join-Path $Root $OutputDir
}
$Archive = Join-Path $OutputDir $ArchiveName

foreach ($required in @('conf', 'web\views', 'web\static', 'cmd\nps\nps.go')) {
    if (-not (Test-Path (Join-Path $Root $required))) {
        throw "Missing $required. Run this script from the project root."
    }
}

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw 'go not found. Install Go 1.24+ and add it to PATH.'
}
if (-not (Get-Command tar.exe -ErrorAction SilentlyContinue)) {
    throw 'tar.exe not found. Use Windows 10+ built-in C:\Windows\System32\tar.exe'
}

Write-Host "==> Go: $(& go version)"
Write-Host '==> Building linux/amd64 server ...'

$Stage = Join-Path $env:TEMP ('nps-linux-amd64-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $Stage -Force | Out-Null

$oldCGO = $env:CGO_ENABLED
$oldGOOS = $env:GOOS
$oldGOARCH = $env:GOARCH
try {
    $env:CGO_ENABLED = '0'
    $env:GOOS = 'linux'
    $env:GOARCH = 'amd64'

    $binary = Join-Path $Stage 'nps'
    & go build -ldflags '-s -w -extldflags -static' -o $binary ./cmd/nps/nps.go
    if ($LASTEXITCODE -ne 0) { throw 'go build failed' }

    # Verify Linux amd64 ELF: 7f ELF, class=2 (64bit), machine=0x3e (x86-64)
    $fs = [System.IO.File]::OpenRead($binary)
    try {
        $header = New-Object byte[] 20
        [void]$fs.Read($header, 0, 20)
    } finally {
        $fs.Dispose()
    }
    if ($header[0] -ne 0x7F -or $header[1] -ne 0x45 -or $header[2] -ne 0x4C -or $header[3] -ne 0x46 -or $header[4] -ne 0x02) {
        throw 'Build output is not a 64-bit Linux ELF'
    }
    if ([BitConverter]::ToUInt16($header, 18) -ne 0x3E) {
        throw 'Build output is not amd64'
    }

    Write-Host '==> Staging release files ...'
    Copy-Item -Path (Join-Path $Root 'conf') -Destination (Join-Path $Stage 'conf') -Recurse -Force
    New-Item -ItemType Directory -Path (Join-Path $Stage 'web') -Force | Out-Null
    Copy-Item -Path (Join-Path $Root 'web\views') -Destination (Join-Path $Stage 'web') -Recurse -Force
    Copy-Item -Path (Join-Path $Root 'web\static') -Destination (Join-Path $Stage 'web') -Recurse -Force

    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
    if (Test-Path $Archive) { Remove-Item $Archive -Force }

    Write-Host "==> Packing $Archive ..."
    & tar.exe -czf $Archive -C $Stage nps conf web
    if ($LASTEXITCODE -ne 0) { throw 'tar failed' }
} finally {
    $env:CGO_ENABLED = $oldCGO
    $env:GOOS = $oldGOOS
    $env:GOARCH = $oldGOARCH
    if (Test-Path $Stage) { Remove-Item $Stage -Recurse -Force }
}

$result = Get-Item $Archive
Write-Host ("Done: {0} ({1:N2} MB)" -f $result.FullName, ($result.Length / 1MB))
Write-Host 'Contains: nps, conf/, web/views/, web/static/'
