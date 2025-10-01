# Repository Guidelines

## Project Structure & Module Organization
- `autoflow/` hosts the GUI (`main.py`) and orchestrator modules; runtime artefacts stay in `autoflow/work/` (`inbox/`, `out/`, `logs/`, `shot/`, `tmp/`).
- `autoflow/core/` coordinates pipelines and logging, while `autoflow/services/` holds download, form-processing, upload, and browser automation subpackages.
- `autoflow_io/` and `autoflow_persist/` provide shared I/O and persistence helpers; expose new utilities through explicit public functions.
- `tests/` mirrors package names with fixtures in `tests/fixtures/`; reusable inputs live in `data/` and demonstrations in `examples/`.

## Build, Test, and Development Commands
- `python -m venv .venv && pip install -r autoflow/requirements.txt` installs Python 3.13 dependencies.
- `python -m autoflow.main` launches the Tkinter GUI and streams logs to `autoflow/work/logs/`.
- `python -m autoflow.cli process-forms --input work/inbox/*.xlsx --output work/out` runs the CLI pipeline for batch jobs.
- `python -m autoflow.cli get-rate --date 2025-01-02 --from USD --to CNY` fetches USD/CNY reference rates for validations.
- `pytest -q` runs smoke tests; `pytest -m "not slow"` accelerates local feedback; append `-m online` when network fixtures are enabled.
- `autoflow/build_win.bat` builds the PyInstaller executable under `dist/AutoFlow.exe`; smoke-test the artifact on Windows.

## Coding Style & Naming Conventions
- Adopt four-space indentation, `snake_case` modules and functions, PascalCase classes, and ALL_CAPS constants; keep widget identifiers descriptive.
- Type-annotate public callables and prefer Google-style docstrings; only add inline comments when control flow is non-obvious.
- Log via the shared logger with ISO-8601 timestamps; avoid stray `print` calls outside CLI entrypoints.

## Testing Guidelines
- Name specs `test_*.py`; keep Given/When/Then notes inside tests to document intent.
- Replace external dependencies with fakes or fixtures from `tests/fixtures/`; never exercise live services in CI.
- Honour `pytest.ini` markers: label long scenarios `@pytest.mark.slow`, network cases `@pytest.mark.online`, and document any additions.

## Commit & Pull Request Guidelines
- Write imperative, scoped commit titles keyed to the touched module (e.g., `Refine form_processor validations`); avoid bundling unrelated changes.
- Pull requests should state intent, list verification commands (`python -m autoflow.main`, `pytest -q`, relevant CLI runs), and reference issues or task IDs.
- Attach GUI screenshots or CLI artefacts from `autoflow/work/shot/` or `autoflow/work/logs/` for UX or pipeline changes, and call out configuration edits.

## Security & Configuration Tips
- Keep credentials in environment variables or secure stores; never hardcode secrets in profiles or templates.
- Update `autoflow/config/profiles.yaml`, `mapping.yaml`, and `selectors/*.yaml` atomically and document new keys inline.
- Clear sensitive temporaries in `autoflow/work/tmp/` during reviews and handle fallback artefacts like `last_response.json` as confidential.
