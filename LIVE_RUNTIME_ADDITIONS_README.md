# Live runtime integration — additions only

New files in this archive:

- `src/sentinelagent/runtime_server.py`
- `scripts/run_live_runtime.py`
- `tests/test_runtime_server.py`
- `docs/live-runtime.md`
- `config/local-agents.example.json`

No existing files are replaced. Extract into the existing repository root.

Verify with `py -3.14 -m pytest -q`. Launch with
`py -3.14 .\scripts\run_live_runtime.py`; this uses a local-development event
stream and the same in-process pipeline/registry objects as its dashboard.
