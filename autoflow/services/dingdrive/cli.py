"""Typer CLI entry points for DingTalk Drive."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from autoflow.core.logger import get_logger

from .client import DingDriveClient

LOGGER = get_logger()

app = typer.Typer(name="dingdrive", help="Operate DingTalk Drive resources.")


def _resolve_client(profile: str) -> DingDriveClient:
    client = DingDriveClient.from_profile(profile)
    return client


def _handle_error(exc: Exception) -> None:
    LOGGER.error("dingdrive operation failed: %s", exc, exc_info=True)
    typer.secho(f"Error: {exc}", fg=typer.colors.RED)
    raise typer.Exit(code=1)


@app.command("ls")
def cmd_list(
    parent: str = typer.Option("root", "--parent", help="Parent folder id"),
    profile: str = typer.Option("dingdrive", "--profile", help="DingDrive profile name"),
) -> None:
    """List child items under a folder."""

    client = _resolve_client(profile)
    try:
        items = client.list_children(parent)
    except Exception as exc:
        if isinstance(exc, typer.Exit):
            raise
        _handle_error(exc)
    else:
        if not items:
            typer.echo("<empty>")
        else:
            for item in items:
                typer.echo(
                    f"{item.get('item_type','?'):6} {item.get('name','--'):40} {item.get('id')}"
                )
    finally:
        client.close()


@app.command("mkdir")
def cmd_mkdir(
    name: str = typer.Option(..., "--name", help="New folder name"),
    parent: str = typer.Option("root", "--parent", help="Parent folder id"),
    profile: str = typer.Option("dingdrive", "--profile", help="DingDrive profile name"),
) -> None:
    """Create a folder in DingTalk Drive."""

    client = _resolve_client(profile)
    try:
        folder_id = client.create_folder(parent, name)
        typer.echo(folder_id)
    except Exception as exc:
        if isinstance(exc, typer.Exit):
            raise
        _handle_error(exc)
    finally:
        client.close()


@app.command("upload")
def cmd_upload(
    file: Path = typer.Option(..., "--file", exists=True, dir_okay=False, help="Local file path"),
    parent: str = typer.Option("root", "--parent", help="Destination folder id"),
    name: Optional[str] = typer.Option(None, "--name", help="Override uploaded file name"),
    profile: str = typer.Option("dingdrive", "--profile", help="DingDrive profile name"),
) -> None:
    """Upload a file to DingTalk Drive."""

    client = _resolve_client(profile)
    try:
        file_id = client.upload_file(parent, str(file), name=name)
        typer.echo(file_id)
    except Exception as exc:
        if isinstance(exc, typer.Exit):
            raise
        _handle_error(exc)
    finally:
        client.close()


@app.command("download")
def cmd_download(
    item_id: str = typer.Option(..., "--id", help="File identifier"),
    out: Path = typer.Option(..., "--out", help="Destination path or directory"),
    profile: str = typer.Option("dingdrive", "--profile", help="DingDrive profile name"),
) -> None:
    """Download a file from DingTalk Drive."""

    client = _resolve_client(profile)
    try:
        written = client.download_file(item_id, str(out))
        typer.echo(written)
    except Exception as exc:
        if isinstance(exc, typer.Exit):
            raise
        _handle_error(exc)
    finally:
        client.close()


@app.command("rename")
def cmd_rename(
    item_id: str = typer.Option(..., "--id", help="Item identifier"),
    name: str = typer.Option(..., "--name", help="New name"),
    profile: str = typer.Option("dingdrive", "--profile", help="DingDrive profile name"),
) -> None:
    """Rename a drive item."""

    client = _resolve_client(profile)
    try:
        client.rename(item_id, name)
    except Exception as exc:
        if isinstance(exc, typer.Exit):
            raise
        _handle_error(exc)
    finally:
        client.close()


@app.command("mv")
def cmd_move(
    item_id: str = typer.Option(..., "--id", help="Item identifier"),
    parent: str = typer.Option(..., "--parent", help="Target folder id"),
    profile: str = typer.Option("dingdrive", "--profile", help="DingDrive profile name"),
) -> None:
    """Move an item to another folder."""

    client = _resolve_client(profile)
    try:
        client.move(item_id, parent)
    except Exception as exc:
        if isinstance(exc, typer.Exit):
            raise
        _handle_error(exc)
    finally:
        client.close()


@app.command("rm")
def cmd_rm(
    item_id: str = typer.Option(..., "--id", help="File or folder identifier"),
    profile: str = typer.Option("dingdrive", "--profile", help="DingDrive profile name"),
) -> None:
    """Delete a file or folder."""

    client = _resolve_client(profile)
    try:
        client.delete(item_id)
    except Exception as exc:
        if isinstance(exc, typer.Exit):
            raise
        _handle_error(exc)
    finally:
        client.close()


@app.command("preview")
def cmd_preview(
    item_id: str = typer.Option(..., "--id", help="File identifier"),
    profile: str = typer.Option("dingdrive", "--profile", help="DingDrive profile name"),
) -> None:
    """Fetch a preview URL if available."""

    client = _resolve_client(profile)
    try:
        preview_url = client.get_preview_url(item_id)
        if preview_url:
            typer.echo(preview_url)
        else:
            typer.echo("<no-preview>")
    except Exception as exc:
        if isinstance(exc, typer.Exit):
            raise
        _handle_error(exc)
    finally:
        client.close()


__all__ = ["app"]
