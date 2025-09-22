"""
RESPONSIBILITIES
- Run dependency and filesystem health checks for persistence stores.
- Provide both a callable API and a small CLI for quick diagnostics.
PROCESS OVERVIEW
1. persist_healthcheck() aggregates health from rates/pdf/xlsx stores.
2. CLI prints per-store status along with remediation hints.
3. Future automation can consume the structured results to block deployments.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import typer

from autoflow_persist.stores.base_store import PersistHealth
from autoflow_persist.stores.pdf_store import PDFStore
from autoflow_persist.stores.rates_store import RatesStore
from autoflow_persist.stores.xlsx_store import XLSXStore
from autoflow_persist.utils.log import get_logger

app = typer.Typer(help="Run persistence layer health checks.")
logger = get_logger("tools.persist_health")


def persist_healthcheck(root: Path | None = None) -> Dict[str, PersistHealth]:
    """Return per-store health diagnostic results."""

    stores = {
        "rates": RatesStore(root),
        "pdf": PDFStore(root),
        "xlsx": XLSXStore(root),
    }
    results: Dict[str, PersistHealth] = {}
    for name, store in stores.items():
        try:
            results[name] = store.healthcheck()
        except Exception as exc:  # noqa: BLE001 - capture unexpected failures
            logger.error("Healthcheck failed for %s: %s", name, exc)
            results[name] = PersistHealth(
                dependencies={},
                writable_paths={},
                locked_paths=[],
                issues=[str(exc)],
            )
    return results


@app.command("run")
def run_command(
    root: Path | None = typer.Option(None, help="Alternate persistence root."),
) -> None:
    """Execute health checks and pretty-print the outcome."""

    results = persist_healthcheck(root)
    for name, health in results.items():
        status = "OK" if health.is_healthy() else "FAIL"
        typer.echo(f"[{status}] {name} store")
        if not health.dependencies:
            typer.echo("  dependencies: (not evaluated)")
        else:
            for dep, ok in health.dependencies.items():
                typer.echo(f"  dependency {dep}: {'OK' if ok else 'MISSING'}")
        for path, ok in health.writable_paths.items():
            typer.echo(f"  writable {path}: {'yes' if ok else 'no'}")
        if health.locked_paths:
            typer.echo(f"  locked: {', '.join(health.locked_paths)}")
        if health.issues:
            typer.echo("  issues:")
            for issue in health.issues:
                typer.echo(f"    - {issue}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
