$pythonExe = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (!(Test-Path $pythonExe)) {
    Write-Error "Python executable not found at $pythonExe"
    exit 1
}

& $pythonExe -m uvicorn backend.api:app --host 127.0.0.1 --port 8001 --reload
