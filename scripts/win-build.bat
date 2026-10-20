@echo off
rem Builds the loadable hudi extension + the duckdb CLI (release), MSVC +
rem Ninja. This bat file is the CI build recipe; it mirrors the locally
rem verified Windows build. cmd is used deliberately: the cmake arguments
rem pass through unescaped, so -DDUCKDB_EXTENSION_CONFIGS stays a single
rem argument (PowerShell wrapper scripts had mangled it, and the template
rem Makefile's quote handling is bash-oriented).
setlocal
cd /d "%~dp0.."
set "REPO=%CD%"

rem --- Locate Visual Studio 2022 (any edition, or BuildTools) ---
set "VSCMD="
for %%v in (
  "C:\Program Files\Microsoft Visual Studio\2022\Enterprise"
  "C:\Program Files\Microsoft Visual Studio\2022\Professional"
  "C:\Program Files\Microsoft Visual Studio\2022\Community"
  "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools"
) do if exist "%%~v\Common7\Tools\VsDevCmd.bat" set "VSCMD=%%~v\Common7\Tools\VsDevCmd.bat"
if not defined VSCMD (
  echo [win-build] ERROR: Visual Studio 2022 not found
  exit /b 1
)
call "%VSCMD%" -arch=x64 -host_arch=x64
if errorlevel 1 exit /b 1
where cl >nul 2>nul || (echo [win-build] ERROR: cl.exe not on PATH & exit /b 1)

rem --- Ninja (windows-latest ships CMake, not Ninja) ---
where ninja >nul 2>nul
if errorlevel 1 (
  pip install ninja
  where ninja >nul 2>nul || (echo [win-build] ERROR: ninja unavailable & exit /b 1)
)
ninja --version

rem --- Inflate the zipped test fixtures (smoke + future sqllogictests) ---
bash scripts/inflate_fixtures.sh

rem --- Configure: hudi must register (look for "Load extension 'hudi'") ---
if not exist build\release mkdir build\release
cmake -S duckdb -B build\release -G Ninja ^
  -DDUCKDB_EXTENSION_CONFIGS=%REPO%/extension_config.cmake ^
  -DCMAKE_BUILD_TYPE=Release > win_configure.log 2>&1
if errorlevel 1 (
  type win_configure.log
  exit /b 1
)
findstr /c:"Load extension 'hudi'" win_configure.log >nul
if errorlevel 1 (
  echo [win-build] ERROR: hudi was NOT registered by cmake configure
  findstr /n "extension" win_configure.log
  exit /b 1
)

rem --- Build everything (duckdb CLI + hudi loadable extension) ---
cmake --build build\release --parallel
if errorlevel 1 exit /b 1

rem --- Fail loudly if the artifacts are missing ---
dir /s /b build\release\hudi*.duckdb_extension 2>nul
if errorlevel 1 (
  echo [win-build] ERROR: hudi.duckdb_extension not found under build\release
  exit /b 1
)
if not exist build\release\duckdb.exe (
  echo [win-build] ERROR: build\release\duckdb.exe not found
  exit /b 1
)
echo [win-build] BUILD_DONE
