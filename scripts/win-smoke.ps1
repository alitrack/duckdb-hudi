# Windows smoke test for the built hudi extension.
#
# Deliberately narrower than scripts/gate.sh: on Windows the hudi-rs kernel's
# local object_store does not walk two-level hive-style partition directories
# (tracked upstream in apache/hudi-rs), so the hive fixture is
# asserted to return 0 rows (documented known limitation) rather than data.
# Everything that works on Windows is asserted positively; bare local paths
# must be passed as file:// URIs because the kernel's URL parser rejects
# Windows drive paths without a scheme.
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..'))

$build = Join-Path (Get-Location) 'build\release'
$ext = Join-Path $build 'extension\hudi\hudi.duckdb_extension'
$db = Join-Path $build 'duckdb'
if ($IsWindows) { $db += '.exe' }
$fix = Join-Path (Get-Location) 'test\fixture'

if (-not (Test-Path $ext)) { throw "extension not found: $ext" }
if (-not (Test-Path $db)) { throw 'duckdb CLI not found: ' + $db }

function Uri([string]$path) { 'file:///' + ($path -replace '\\', '/') }

function Sql([string]$sql) {
  & $db -unsigned -csv -c "LOAD '$($ext -replace '\\', '/')'; $sql" 2>$null |
    Select-Object -Last 1 | ForEach-Object { $_.Trim() }
}

$plain = Uri (Join-Path $fix 'cow_v6_nonpartitioned')
$simple = Uri (Join-Path $fix 'cow_v6_simplekeygen_nonhivestyle')
$hive = Uri (Join-Path $fix 'cow_v6_complexkeygen_hivestyle')
$mdt = Uri (Join-Path $fix 'mor_v9_mdt_simple\v9_txns_simple_meta')

$pass = 0; $fail = 0
function Check([string]$name, [string]$got, [string]$want) {
  if ($got -ceq $want) {
    Write-Output ("  ok   {0} = {1}" -f $name, $got)
    $script:pass++
  } else {
    Write-Output ("  FAIL {0} got='{1}' want='{2}'" -f $name, $got, $want)
    $script:fail++
  }
}

Write-Output '== load =='
Check 'extension loads' (Sql "SELECT extension_name FROM duckdb_extensions() WHERE loaded AND extension_name='hudi'") 'hudi'

Write-Output '== non-partitioned COW =='
Check 'snapshot rows' (Sql "SELECT count(*) FROM read_hudi('$plain')") '4'
Check 'snapshot sum' (Sql "SELECT sum(`"intField`") FROM read_hudi('$plain')") '120000'

Write-Output '== single-level non-hive partitions + pushdown =='
Check 'full rows' (Sql "SELECT count(*) FROM read_hudi('$simple')") '4'
Check 'filtered rows' (Sql "SELECT count(*) FROM read_hudi('$simple') WHERE byteField = 10") '2'
# EXPLAIN (FORMAT json) keeps each stat on one line, unlike the ASCII box which
# wraps "Hudi Filters: byteField = 10" across the column width.
$explain = & $db -unsigned -c "LOAD '$($ext -replace '\\', '/')'; EXPLAIN (FORMAT json) SELECT count(*) FROM read_hudi('$simple') WHERE byteField = 10" 2>$null
if ($explain -match 'Hudi Filters.*byteField = 10' -and $explain -match 'Partitions.*1 / 3') {
  Write-Output '  ok   pushdown in EXPLAIN (Hudi Filters + Partitions 1/3)'
  $pass++
} else {
  Write-Output '  FAIL pushdown in EXPLAIN'
  $fail++
}

Write-Output '== metadata functions =='
$info = Sql "SELECT hudi_info('$mdt')"
if ($info -match 'commits=12') {
  Write-Output ('  ok   hudi_info commits=12 = ' + $info)
  $pass++
} else {
  Write-Output ('  FAIL hudi_info got=' + $info)
  $fail++
}
Check 'hudi_commits rows' (Sql "SELECT count(*) FROM hudi_commits('$mdt')") '12'

Write-Output '== known limitation: hive two-level partitions =='
# Until the upstream Windows path handling (cf. apache/hudi-rs#445) is fixed,
# expect 0 rows on Windows (4 on linux/osx).
Check 'hive rows (documented limitation)' (Sql "SELECT count(*) FROM read_hudi('$hive')") '0'

Write-Output ("== win smoke: {0} passed, {1} failed ==" -f $pass, $fail)
if ($fail -gt 0) { exit 1 }
Write-Output 'WIN_SMOKE_ALL_PASS'
