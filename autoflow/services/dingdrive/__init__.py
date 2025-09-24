"""DingTalk Drive service integration."""

from .client import DingDriveClient
from .cli import app as dingdrive_app

__all__ = [
    "DingDriveClient",
    "dingdrive_app",
]
