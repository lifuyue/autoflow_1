"""Authentication client for DingTalk Drive enterprise applications."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Optional

import requests
from requests import Response
from requests.exceptions import RequestException, Timeout

from autoflow.core.logger import get_logger

from .config import (
    DingDriveConfig,
    load_client_id,
    load_client_secret,
    load_retry_config,
    load_timeout,
)
from .models import DriveAuthError

LOGGER = get_logger()


@dataclass(slots=True)
class TokenState:
    """Cached authentication token details."""

    value: str
    expires_at: float


class AuthClient:
    """Fetch and cache DingTalk Drive access tokens with thread safety."""

    def __init__(
        self,
        config: DingDriveConfig,
        *,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._config = config
        self._session = session or requests.Session()
        self._session.verify = config.verify_tls
        self._session.trust_env = config.trust_env
        if config.proxies:
            self._session.proxies.update(config.proxies)
        self._lock = threading.RLock()
        self._token_state: TokenState | None = None
        self._retry_config = load_retry_config(config)
        self._timeout = load_timeout(config)

    @property
    def session(self) -> requests.Session:
        """Expose the session used for token retrieval."""

        return self._session

    def get_token(self, *, force_refresh: bool = False) -> str:
        """Return a cached access token, refreshing when necessary."""

        with self._lock:
            if not force_refresh and self._token_state and self._token_state.expires_at - time.monotonic() > 60:
                return self._token_state.value
            return self._refresh_locked()

    def invalidate(self) -> None:
        """Invalidate the cached token forcing a refresh on next access."""

        with self._lock:
            self._token_state = None

    # Internal helpers -------------------------------------------------

    def _refresh_locked(self) -> str:
        client_id = load_client_id(self._config)
        client_secret = load_client_secret(self._config)
        params = {"appkey": client_id, "appsecret": client_secret}
        attempts = max(1, self._retry_config.max_attempts)
        backoff = max(0.05, self._retry_config.backoff_ms / 1000.0)
        max_backoff = max(backoff, self._retry_config.max_backoff_ms / 1000.0)
        last_error: DriveAuthError | None = None
        for attempt in range(1, attempts + 1):
            try:
                response = self._session.get(
                    self._config.auth_url,
                    params=params,
                    timeout=self._timeout,
                )
            except Timeout as exc:  # pragma: no cover - network failure path
                LOGGER.warning(
                    "dingdrive.auth token_request_timeout attempt=%d", attempt, exc_info=exc
                )
                last_error = DriveAuthError("Timeout while requesting DingTalk token")
            except RequestException as exc:  # pragma: no cover - network failure path
                LOGGER.warning(
                    "dingdrive.auth token_request_error attempt=%d error=%s",
                    attempt,
                    type(exc).__name__,
                    exc_info=exc,
                )
                last_error = DriveAuthError("Failed to request DingTalk token")
            else:
                try:
                    token_state = self._parse_response(response)
                except DriveAuthError as exc:
                    last_error = exc
                else:
                    self._token_state = token_state
                    LOGGER.info(
                        "dingdrive.auth token_refreshed expires_in=%.0fs attempt=%d",
                        token_state.expires_at - time.monotonic(),
                        attempt,
                    )
                    return token_state.value

            if attempt < attempts:
                sleep_for = min(max_backoff, backoff * (2 ** (attempt - 1)))
                time.sleep(sleep_for)

        if last_error is None:  # pragma: no cover - defensive
            raise DriveAuthError("Unable to obtain DingTalk token")
        raise last_error

    def _parse_response(self, response: Response) -> TokenState:
        if response.status_code != 200:
            raise DriveAuthError(
                f"DingTalk token endpoint returned HTTP {response.status_code}",
                status_code=response.status_code,
            )
        try:
            payload = response.json()
        except ValueError as exc:  # noqa: BLE001
            raise DriveAuthError("DingTalk token endpoint returned invalid JSON") from exc

        errcode = payload.get("errcode")
        if errcode not in (0, None):
            errmsg = payload.get("errmsg") or "unknown error"
            raise DriveAuthError(f"DingTalk token error: {errmsg}", payload={"errcode": errcode})
        token_value = payload.get("access_token")
        if not token_value:
            raise DriveAuthError("DingTalk token response missing access_token")
        expires_in = float(payload.get("expires_in", 7200))
        expires_at = time.monotonic() + max(60.0, expires_in)
        return TokenState(value=str(token_value), expires_at=expires_at)


__all__ = ["AuthClient", "TokenState"]
