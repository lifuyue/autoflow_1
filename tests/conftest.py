from __future__ import annotations

import faulthandler
import socket
import sys
import threading
import traceback
from pathlib import Path
from types import FrameType
from typing import Dict

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

faulthandler.enable()  # Ensure crashes emit tracebacks.
socket.setdefaulttimeout(10)

from autoflow.services.fees_fetcher import pbc_client


def _snapshot_thread_stacks() -> Dict[int, str]:
    frames: Dict[int, FrameType] = sys._current_frames()  # type: ignore[attr-defined]
    stacks: Dict[int, str] = {}
    for ident, frame in frames.items():
        stacks[ident] = "".join(traceback.format_stack(frame))
    return stacks


@pytest.fixture(autouse=True, scope="session")
def _thread_diagnostics() -> None:
    """Dump live non-daemon threads at the end of the test session."""

    pbc_client.reset_request_config()
    pbc_client.reset_metrics()
    yield

    stacks = _snapshot_thread_stacks()
    lingering: list[threading.Thread] = []
    for thread in threading.enumerate():
        if thread.daemon or thread is threading.current_thread():
            continue
        thread.join(timeout=2)
        if thread.is_alive():
            lingering.append(thread)

    if lingering:
        print("\n[pytest] lingering threads detected:", file=sys.stderr)
        for thread in lingering:
            stack = stacks.get(thread.ident, "<no stack>\n")
            print(
                f"- Thread {thread.name} (ident={thread.ident}) still alive after tests", file=sys.stderr
            )
            print(stack, file=sys.stderr)
    pbc_client.reset_request_config()
