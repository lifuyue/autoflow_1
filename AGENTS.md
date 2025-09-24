# Repository Guidelines

## Project Structure & Module Organization
- `autoflow/` hosts the Tkinter GUI, orchestration core, service modules, configuration assets, and templates; runtime artefacts live under `autoflow/work/` with the canonical `inbox/`, `out/`, `tmp/`, `logs/`, and `shot/` folders.
- `autoflow_io/` and `autoflow_persist/` expose shared I/O and persistence utilities consumed by both GUI and CLI flows; treat their public APIs as stable.
- `tests/` mirrors the package layout with reusable fixtures in `tests/fixtures/`; `data/`, `examples/`, and `tools/` carry sample inputs and helper CLIs that should be refreshed whenever behaviour changes.

## Build, Test, and Development Commands
- `python -m autoflow.main` launches the GUI and streams logs to `work/logs/`.
- `python -m autoflow.cli process-forms --help` reveals batch pipelines; rely on the CLI while iterating transformations.
- `pytest -q` runs the default suite, `pytest -m "not slow"` accelerates feedback, and `pytest -m online` enables network fixtures.
- `autoflow/build_win.bat` builds `out/auto_flow.exe` through PyInstaller; smoke-test the artifact before release.

## Coding Style & Naming Conventions
- Target Python 3.13 with four-space indentation, `snake_case` files/functions, PascalCase classes, ALL_CAPS constants, and intent-led widget names.
- Every callable requires type hints plus Google-style docstrings (Args/Returns/Raises); reserve comments for non-obvious control flow.
- Log through the shared logger (`logs/agent.log` by default) and keep timestamps ISO 8601 for auditability.

## Testing Guidelines
- Store feature tests in `tests/` using `test_*.py` files and `test_case_description` names; include Given/When/Then context inline for readability.
- Use pandas fixtures for data validation and mock browser layers to keep Playwright paths deterministic in CI.
- Honour markers from `pytest.ini`; label network cases with `@pytest.mark.online` and quarantine expensive paths under `@pytest.mark.slow`.

## Commit & Pull Request Guidelines
- Prefer concise, action-led commit titles similar to `表格操作模块化`; avoid bundling unrelated files.
- PRs must outline scope, linked issues, verification steps (`pytest`, CLI or GUI command), and attach UI evidence in `work/shot/` when applicable.
- Confirm temporary files remain in `tmp/`, document new YAML defaults inline, and flag finance-sensitive changes for additional review.

## Configuration & Resilience Notes
- Keep secrets out of Git; reference credentials through `autoflow/config/profiles.yaml` and environment variables, never literals.
- On request failures, persist the fallback artefact in `tmp/` (e.g., `tmp/last_response.json`) and surface next actions via logs or GUI prompts.
