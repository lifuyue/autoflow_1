"""HTTP utilities for DingTalk Drive interactions."""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Iterable, Mapping

import requests
from requests import Response
from requests.exceptions import ConnectionError, RequestException, Timeout

from autoflow.core.logger import get_logger
from .config import DingDriveConfig
from .models import DriveAuthError, DriveNotFound, DriveRequestError, DriveRetryableError

LOGGER = get_logger()
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
AUTHORIZATION_HEADER = "x-acs-dingtalk-access-token"
USER_AGENT = "Autoflow-DingDrive/1.0"


class DingTalkAuth:
    """Handle DingTalk app credential to access-token exchange."""

    def __init__(
        self,
        config: DingDriveConfig,
        *,
        session: requests.Session | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._session = session or requests.Session()
        self._session.verify = config.verify_tls
        self._session.trust_env = config.trust_env
        if config.proxies:
            self._session.proxies.update(config.proxies)
        self._session.headers.setdefault("User-Agent", USER_AGENT)
        self._logger = logger or LOGGER
        self._token: str | None = None
        self._expiry: float = 0.0
        self._lock = threading.Lock()

    def get_access_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid access token, refreshing if needed."""

        with self._lock:
            if not force_refresh and self._token and time.monotonic() < self._expiry - 60:
                return self._token
            token, ttl = self._fetch_token()
            self._token = token
            self._expiry = time.monotonic() + max(ttl, 60)
            return token

    def invalidate(self) -> None:
        """Force the next call to fetch a fresh token."""

        with self._lock:
            self._token = None
            self._expiry = 0.0

    def _fetch_token(self) -> tuple[str, float]:
        params = {"appkey": self._config.app_key, "appsecret": self._config.app_secret}
        try:
            response = self._session.get(
                self._config.auth_url,
                params=params,
                timeout=self._config.timeout_sec,
            )
        except Timeout as exc:
            raise DriveAuthError("Timeout while requesting DingTalk access token") from exc
        except RequestException as exc:
            raise DriveAuthError("Unable to request DingTalk access token") from exc

        if response.status_code != 200:
            raise DriveAuthError(
                f"Failed to obtain DingTalk access token: HTTP {response.status_code}",
                status_code=response.status_code,
            )
        try:
            payload = response.json()
        except ValueError as exc:  # noqa: BLE001
            raise DriveAuthError("Invalid token response from DingTalk") from exc

        if payload.get("errcode") not in (0, None):
            raise DriveAuthError(
                f"DingTalk token error: {payload.get('errmsg', 'unknown error')}",
                payload=payload,
            )
        token = payload.get("access_token")
        if not token:
            raise DriveAuthError("access_token missing in DingTalk response")
        expires_in = float(payload.get("expires_in", 7200))
        self._logger.debug("Fetched DingTalk access token (expires in %.0fs)", expires_in)
        return token, expires_in


class HttpClient:
    """Requests wrapper with retry and error mapping for DingTalk APIs."""

    def __init__(
        self,
        config: DingDriveConfig,
        *,
        session: requests.Session | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._session = session or requests.Session()
        self._session.verify = config.verify_tls
        self._session.trust_env = config.trust_env
        self._session.headers.setdefault("User-Agent", USER_AGENT)
        if config.proxies:
            self._session.proxies.update(config.proxies)
        self._logger = logger or LOGGER

    @property
    def session(self) -> requests.Session:
        """Expose the underlying session for advanced flows (uploads)."""

        return self._session

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, str] | None = None,
        json_body: Mapping[str, object] | None = None,
        data: object | None = None,
        expected_status: Iterable[int] = (200,),
        stream: bool = False,
        timeout: float | None = None,
        allow_retry: bool = True,
        use_base_url: bool = True,
    ) -> Response:
        """Perform an HTTP request with retry semantics."""

        url = path if not use_base_url else self._compose_url(path)
        attempts = self._config.retries.max_attempts
        backoff = self._config.retries.backoff_ms / 1000.0
        max_backoff = self._config.retries.max_backoff_ms / 1000.0
        timeout_value = timeout or self._config.timeout_sec

        last_exc: RequestException | None = None
        expected = tuple(expected_status)

        for attempt in range(1, attempts + 1):
            try:
                response = self._session.request(
                    method,
                    url,
                    headers=dict(headers or {}),
                    params=dict(params or {}),
                    json=json_body,
                    data=data,
                    timeout=timeout_value,
                    stream=stream,
                )
            except Timeout as exc:
                last_exc = exc
                self._logger.warning("DingDrive request timeout (%s %s) attempt %d", method, url, attempt)
                if self._should_retry(attempt, attempts, allow_retry):
                    self._sleep_with_backoff(backoff, max_backoff, attempt)
                    continue
                raise DriveRetryableError("Request timed out", payload={"url": url}) from exc
            except (ConnectionError, RequestException) as exc:
                last_exc = exc
                self._logger.warning("DingDrive request error (%s %s): %s", method, url, exc)
                if self._should_retry(attempt, attempts, allow_retry):
                    self._sleep_with_backoff(backoff, max_backoff, attempt)
                    continue
                raise DriveRetryableError("Request failed", payload={"url": url}) from exc

            if response.status_code in expected:
                return response

            if response.status_code == 401:
                raise DriveAuthError("Unauthorized", status_code=401, payload=_safe_json(response))
            if response.status_code == 404:
                raise DriveNotFound("Resource not found", status_code=404, payload=_safe_json(response))

            if allow_retry and response.status_code in RETRYABLE_STATUS and self._should_retry(attempt, attempts, True):
                self._logger.warning(
                    "Retryable response from DingTalk (%s) attempt %d/%d", response.status_code, attempt, attempts
                )
                self._sleep_with_backoff(backoff, max_backoff, attempt)
                continue

            raise DriveRequestError(
                f"Unexpected status {response.status_code}",
                status_code=response.status_code,
                payload=_safe_json(response),
            )

        raise DriveRetryableError("Exhausted retries", payload={"url": url}) from last_exc

    def _compose_url(self, path: str) -> str:
        base = self._config.base_url.rstrip("/")
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{base}{path}"

    def _should_retry(self, attempt: int, max_attempts: int, allow_retry: bool) -> bool:
        return allow_retry and attempt < max_attempts

    def _sleep_with_backoff(self, base: float, maximum: float, attempt: int) -> None:
        delay = min(maximum, base * (2 ** (attempt - 1)))
        jitter = random.uniform(0, delay / 2)
        time.sleep(delay + jitter)


def _safe_json(response: Response) -> dict[str, object]:
    try:
        return response.json()
    except ValueError:
        text = response.text
        if len(text) > 200:
            text = text[:200] + "..."
        return {"body": text}


__all__ = ["HttpClient", "DingTalkAuth", "AUTHORIZATION_HEADER"]
