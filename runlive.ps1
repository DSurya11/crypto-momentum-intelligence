param(
    [int]$TickCount = 1,
    [int]$TopN = 50,
    [string]$MarketApi = "coinstats",
    [int]$IngestMaxPools = 15,
    [int]$IngestMaxPagesPerPool = 2,
    [int]$IngestMaxTradesPerPool = 30,
    [int]$IngestLookbackHours = 24,
    [int]$VerifyAfterMinutes = 0,
    [int]$VerifyMinutes = 5,
    [switch]$Loop,
    [int]$LoopIntervalMinutes = 5
)

$pythonExe = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$scriptPath = Join-Path $PSScriptRoot "run_full_live_cycle.py"

if (!(Test-Path $pythonExe)) {
    Write-Error "Python executable not found at $pythonExe"
    exit 1
}

$argsList = @(
    "-u",
    $scriptPath,
    "--tick-count", "$TickCount",
    "--top-n", "$TopN",
    "--market-api", "$MarketApi",
    "--ingest-max-pools", "$IngestMaxPools",
    "--ingest-max-pages-per-pool", "$IngestMaxPagesPerPool",
    "--ingest-max-trades-per-pool", "$IngestMaxTradesPerPool",
    "--ingest-lookback-hours", "$IngestLookbackHours"
)

if ($VerifyAfterMinutes -gt 0) {
    $argsList += @("--verify-after-minutes", "$VerifyAfterMinutes", "--verify-minutes", "$VerifyMinutes")
}

if ($Loop) {
    $argsList += @("--loop", "--loop-interval-minutes", "$LoopIntervalMinutes")
}

$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"
& $pythonExe @argsList
