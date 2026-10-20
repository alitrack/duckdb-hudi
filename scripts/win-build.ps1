# Builds the loadable extension + CLI on Windows (MSVC + Ninja), mirroring
# `GEN=ninja make release` from the extension template.
#
# github.com's windows-latest image ships the full VS2022 (Enterprise), CMake
# and Python but no GNU make, so we drive CMake directly with the same
# arguments the template Makefile uses:
#   cmake $(GENERATOR) ... -DCMAKE_BUILD_TYPE=Release -S duckdb -B build/release
#
# protoc for prost-build (hudi -> Lance): $PROTOC from the environment, else
# protoc on PATH, else the pinned prebuilt that scripts/bootstrap_protoc.sh
# downloads into third_party/protoc (the same 3-tier fallback CMakeLists uses).
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..'))

# Enter the MSVC developer environment (cl.exe, link.exe, Windows SDK).
# Enter-VsDevShell (unlike calling VsDevCmd.bat) persists the env into this
# pwsh session. The VS edition varies (Enterprise on github runners,
# BuildTools locally), so locate it via vswhere.
$vswhere = 'C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe'
$vsPath = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
if (-not $vsPath) { throw 'no MSVC C++ workload found via vswhere' }
$devShell = Join-Path $vsPath 'Common7\Tools\Microsoft.VisualStudio.DevShell.dll'
if (Test-Path $devShell) {
  Import-Module $devShell
  Enter-VsDevShell -VsInstallPath $vsPath -SkipAutomaticLocation
} else {
  # BuildTools has no DevShell module: call VsDevCmd.bat and copy its env in.
  $tmp = New-TemporaryFile
  cmd /c "`"$vsPath\Common7\Tools\VsDevCmd.bat`" -arch=x64 -host_arch=x64 && set" > $tmp
  Get-Content $tmp | ForEach-Object {
    if ($_ -match '^([^=]+)=(.*)$') { Set-Item "Env:\$($matches[1])" $matches[2] }
  }
  Remove-Item $tmp
}
if (-not (Get-Command cl -ErrorAction SilentlyContinue)) { throw 'cl.exe not found after env setup' }

# Resolve protoc before anything else; bootstrap into third_party/protoc if
# neither the environment nor PATH provides one (pure PowerShell: no bash).
if (-not $env:PROTOC) {
  $onPath = Get-Command protoc -ErrorAction SilentlyContinue
  if ($onPath) { $env:PROTOC = $onPath.Source }
}
if (-not $env:PROTOC) {
  $root = Join-Path (Get-Location) 'third_party\protoc'
  $env:PROTOC = Join-Path $root 'bin\protoc.exe'
  if (-not (Test-Path $env:PROTOC)) {
    $ver = '26.1'
    Write-Output "downloading protoc-$ver-win64.zip"
    Invoke-WebRequest "https://github.com/protocolbuffers/protobuf/releases/download/v$ver/protoc-$ver-win64.zip" `
      -OutFile (Join-Path $root 'protoc.zip') -UseBasicParsing
    New-Item -ItemType Directory -Force -Path $root | Out-Null
    Expand-Archive (Join-Path $root 'protoc.zip') -DestinationPath $root -Force
    Remove-Item (Join-Path $root 'protoc.zip')
  }
  if (-not (Test-Path $env:PROTOC)) { throw 'protoc bootstrap failed' }
  $env:PROTOC_INCLUDE = Join-Path $root 'include'
}

$build = Join-Path (Get-Location) 'build\release'
& ninja --version
cmake -S duckdb -B $build -G Ninja -DCMAKE_BUILD_TYPE=Release `
  -DDUCKDB_EXTENSION_CONFIGS=(Join-Path (Get-Location) 'extension_config.cmake')
if ($LASTEXITCODE -ne 0) { throw 'cmake configure failed' }

# Loadable extension (builds the Rust staticlib first via its dependency).
cmake --build $build --target hudi_loadable_extension --parallel
if ($LASTEXITCODE -ne 0) { throw 'hudi_loadable_extension build failed' }

# CLI for the smoke test; duckdb's CMake target is named `shell` and its
# output is duckdb.exe in the build root.
cmake --build $build --target shell --parallel
if ($LASTEXITCODE -ne 0) { throw 'shell (duckdb CLI) build failed' }

Write-Output 'WIN_BUILD_DONE'
