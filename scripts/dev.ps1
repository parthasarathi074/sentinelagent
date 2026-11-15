param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("install", "test", "lint", "format-check", "build", "check")]
    [string]$Task
)

$ErrorActionPreference = "Stop"

$Python = Join-Path $PSScriptRoot "..\.venv\Scripts\python.exe"
$Python = [System.IO.Path]::GetFullPath($Python)

if (-not (Test-Path $Python)) {
    throw "Project virtual environment not found. Create .venv first."
}

switch ($Task) {
    "install" {
        & $Python -m pip install -e ".[dev]"
    }
    "test" {
        & $Python -m pytest -q
    }
    "lint" {
        & $Python -m ruff check src tests scripts
    }
    "format-check" {
        & $Python -m ruff format --check src tests scripts
    }
    "build" {
        & $Python -m pip install build
        if ($LASTEXITCODE -ne 0) {
            throw "Build dependency installation failed."
        }

        & $Python -m build
        if ($LASTEXITCODE -ne 0) {
            throw "Package build failed."
        }
    }
    "check" {
        & $Python -m ruff check src tests scripts
        if ($LASTEXITCODE -ne 0) {
            throw "Ruff check failed."
        }

        & $Python -m pytest -q
        if ($LASTEXITCODE -ne 0) {
            throw "Tests failed."
        }

        & $Python -m pip install build
        if ($LASTEXITCODE -ne 0) {
            throw "Build dependency installation failed."
        }

        & $Python -m build
        if ($LASTEXITCODE -ne 0) {
            throw "Package build failed."
        }
    }
}

if ($LASTEXITCODE -ne 0) {
    throw "Task '$Task' failed with exit code $LASTEXITCODE."
}