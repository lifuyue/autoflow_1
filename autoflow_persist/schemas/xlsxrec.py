"""
RESPONSIBILITIES
- Define placeholder schema for structured XLSX ingestion records.
PROCESS OVERVIEW
1. XLSX ingestion will capture metadata via XlsxIndexRecord.
2. to_dict() converts native types to string-friendly payloads for XLSX.
3. Detailed extraction logic remains TODO for future iterations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import MutableMapping


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


@dataclass(slots=True)
class XlsxIndexRecord:
    source_sha256: str
    source_path: str
    sheet_name: str
    rows: int
    mapping_version: str | None = None
    download_url: str | None = None
    created_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> MutableMapping[str, object]:
        return {
            "source_sha256": self.source_sha256,
            "source_path": self.source_path,
            "sheet_name": self.sheet_name,
            "rows": self.rows,
            "mapping_version": self.mapping_version or "",
            "download_url": self.download_url or "",
            "created_at": self.created_at.isoformat(),
        }
