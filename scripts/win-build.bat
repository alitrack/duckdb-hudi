@echo off
rem hudi Windows build recipe (CI + local). Mirrors the verified local build.
setlocal
cd /d "%~dp0.."
set "REPO=%CD%"

rem Locate VS with MSVC via vswhere (no parenthesized block, no (x86) in any
rem cmd string - those trip up the cmd parser inside this batch file).
set "VSWHERE=C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe"
if not exist "%VSWHERE%" (
  echo [win-build] ERROR: vswhere not found
  exit /b 1
)
"%VSWHERE%" -latest -prerelease -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath > "%TEMP%\hudi_vs.txt" 2>nul
set /p VSROOT=<"%TEMP%\hudi_vs.txt"
if "%VSROOT%"=="" (
  echo [win-build] ERROR: no VS with MSVC tools found
  exit /b 1
)
echo [win-build] VS: %VSROOT%
call "%VSROOT%\Common7\Tools\VsDevCmd.bat" -arch=x64 -host_arch=x64
if errorlevel 1 (
  echo [win-build] ERROR: VsDevCmd failed
  exit /b 1
)
where cl >nul 2>nul || (echo [win-build] ERROR: cl not found & exit /b 1)

where ninja >nul 2>nul || pip install ninja
ninja --version

bash scripts/inflate_fixtures.sh

if not exist build\release mkdir build\release
rem ENABLE_UNITTEST_CPP_TESTS=FALSE: the pinned 2.0 core's new stable C++ API
rem unit tests (tools/cpp/tests/test_cpp_api_*.cpp) do not compile under MSVC
rem (C2248 on the private duckdb::cxx::Instance ctor). This job only builds the
rem extension + CLI and runs a shell smoke test, never core's C++ unit tests,
rem so they are skipped. Linux/macOS still build and run the full suite.
cmake -S duckdb -B build\release -G Ninja -DDUCKDB_EXTENSION_CONFIGS=%REPO%/extension_config.cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_UNITTEST_CPP_TESTS=FALSE > win_configure.log 2>&1
if errorlevel 1 (
  type win_configure.log
  exit /b 1
)
findstr /c:"Load extension 'hudi'" win_configure.log >nul
if errorlevel 1 (
  echo [win-build] ERROR: hudi not registered by cmake
  findstr /n "extension" win_configure.log
  exit /b 1
)

cmake --build build\release --parallel
if errorlevel 1 exit /b 1

dir /s /b build\release\hudi*.duckdb_extension 2>nul
if errorlevel 1 (
  echo [win-build] ERROR: hudi extension not found
  exit /b 1
)
if not exist build\release\duckdb.exe (
  echo [win-build] ERROR: duckdb CLI not found
  exit /b 1
)
echo [win-build] BUILD_DONE
