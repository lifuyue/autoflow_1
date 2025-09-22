"""
RESPONSIBILITIES
- Define placeholder schema objects for future PDF indexing results.
PROCESS OVERVIEW
1. PDF ingestion will populate PDFIndexRecord.
2. to_dict() exposes canonical keys used for the placeholder XLSX store.
3. Concrete logic remains TODO until PDF comparison pipeline matures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import MutableMapping


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


@dataclass(slots=True)
class PDFIndexRecord:
    file_sha256: str
    file_name: str
    local_path: str
    parsed_ok: bool
    invoice_no: str | None = None
    amount: str | None = None
    currency: str | None = None
    download_url: str | None = None
    created_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> MutableMapping[str, object]:
        return {
            "file_sha256": self.file_sha256,
            "file_name": self.file_name,
            "local_path": self.local_path,
            "parsed_ok": "yes" if self.parsed_ok else "no",
            "invoice_no": self.invoice_no or "",
            "amount": self.amount or "",
            "currency": self.currency or "",
            "download_url": self.download_url or "",
            "created_at": self.created_at.isoformat(),
        }
