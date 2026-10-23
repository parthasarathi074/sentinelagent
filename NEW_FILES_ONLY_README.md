# SentinelAgent live-dashboard integration — additions only

This archive contains only three new paths. It does not replace any existing
project file:

- `src/sentinelagent/live_dashboard.py`
- `tests/test_live_dashboard.py`
- `docs/live-dashboard-integration.md`

## Apply

Extract the archive into the root of your existing `sentinelagent` repository.
When prompted, there should be no existing-file overwrite prompts because all
paths in this archive are new.

## Verify (PowerShell)

```powershell
py -3.14 -m pytest -q
```

The full suite in the working test copy reported **241 passed, 5 subtests
passed** after adding these files.

## Important integration note

The current `scripts/run_dashboard.py` is still the standalone demo launcher.
The new `LiveDashboardSnapshotProvider` connects the dashboard API to real
`AnalysisPipeline` and `AgentRegistry` instances when used in the same process
that owns them. See `docs/live-dashboard-integration.md` for the integration
pattern. Do not create a second pipeline/registry for the dashboard and expect
it to observe state held in another process's memory.

The project currently has no general-purpose immutable audit log exposed by
the pipeline, so the provider does not fabricate one. An audit source can be
attached via `audit_provider`.
