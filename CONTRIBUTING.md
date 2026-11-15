# Contributing to SentinelAgent

Thank you for your interest in contributing to SentinelAgent.

SentinelAgent is a prototype security framework for multi-agent data pipelines. Contributions should preserve the existing architecture and clearly distinguish implemented functionality from planned integrations.

## Development environment

Requirements:
- Python 3.13 or later
- Git
- Windows PowerShell or a compatible terminal

Create a virtual environment:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install development dependencies:

```powershell
python -m pip install -e ".[dev]"
```

## Development checks

Run tests:

```powershell
python -m pytest -q
```

Run Ruff:

```powershell
python -m ruff check src tests scripts
```

Check formatting:

```powershell
python -m ruff format --check src tests scripts
```

Build the package:

```powershell
python -m pip install build
python -m build
```

## Contribution guidelines

- Inspect existing code before making changes.
- Prefer small, focused changes.
- Preserve existing public interfaces unless a change is necessary.
- Add or update tests when behavior changes.
- Do not commit secrets, credentials, local databases, or runtime data.
- Do not claim optional integrations are active unless implemented and configured.
- Document prototype limitations accurately.

## Pull requests

Before submitting a pull request:

1. Explain the problem and proposed solution.
2. Describe the files and behavior changed.
3. Run the relevant tests and developer checks.
4. Report actual test results.
5. Document known limitations.

All changes should be reviewed before merging.