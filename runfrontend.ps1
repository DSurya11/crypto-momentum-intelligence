param(
    [switch]$Install
)

$appPath = Join-Path $PSScriptRoot "frontend\alpha-whisperer-pro"
if (!(Test-Path $appPath)) {
    Write-Error "Frontend folder not found: $appPath"
    exit 1
}

Push-Location $appPath
try {
    if ($Install -or !(Test-Path (Join-Path $appPath "node_modules"))) {
        npm i
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    npm run dev
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
