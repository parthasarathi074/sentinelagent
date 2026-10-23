# Persistent audit trail — additions only

This package adds the following new files and does not replace existing files:

- `src/sentinelagent/persistent_audit.py`
- `scripts/run_audited_runtime.py`
- `tests/test_persistent_audit.py`
- `tests/test_audited_runtime.py`
- `docs/persistent-audit-trail.md`

To use durable auditing, start `scripts/run_audited_runtime.py` instead of the
non-audited `scripts/run_live_runtime.py`. The new runner retains the same local
edge-request API and dashboard address, so stop any other server already using
port 8765 first.

Run tests with `py -3.14 -m pytest -q` from the repository root.
