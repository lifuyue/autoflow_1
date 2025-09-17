"""CFETS backed USD/CNY midpoint retrieval."""

from __future__ import annotations

import logging
import re
from decimal import Decimal, ROUND_HALF_UP

from bs4 import BeautifulSoup

from . import pbc_client

LOGGER = logging.getLogger(__name__)

NOTICE_URL = (
    "https://www.chinamoney.org.cn/chinese/ccprnoticecontent/index.html?searchDate={date}"
)

RATE_PATTERN = re.compile(r"1美元对人民币(\d+\.\d{4})元")
DATE_PATTERN = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")


def get_usd_cny_midpoint_from_notice(
    sess, target_date: str
) -> tuple[Decimal, str, str]:  # noqa: D401 - signature defined by specification
    """Return USD/CNY midpoint published on CFETS daily notice."""

    # The shared PBOC client manages TLS diagnostics and deadlines.
    del sess
    url = NOTICE_URL.format(date=target_date)
    try:
        response = pbc_client._request(url)
    except pbc_client.CertHostnameMismatch:
        raise
    except pbc_client.FetchTimeout:
        raise
    except pbc_client.PBOCClientError as exc:  # noqa: SLF001 - intentional internal reuse
        raise LookupError(f"CFETS notice unavailable for {target_date}") from exc

    if not response.text.strip():
        raise LookupError(f"CFETS notice empty for {target_date}")

    soup = BeautifulSoup(response.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    rate_match = RATE_PATTERN.search(text)
    if not rate_match:
        raise LookupError(f"USD/CNY midpoint not present for {target_date}")

    rate = Decimal(rate_match.group(1)).quantize(Decimal("0.0001"), ROUND_HALF_UP)

    date_match = DATE_PATTERN.search(text)
    if date_match:
        year, month, day = (int(part) for part in date_match.groups())
        source_date = f"{year:04d}-{month:02d}-{day:02d}"
    else:
        LOGGER.debug("CFETS notice lacked explicit date; defaulting to target %s", target_date)
        source_date = target_date

    return rate, source_date, "cfets_notice"
