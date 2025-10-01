"""HTTP utilities for DingTalk Drive integrations."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Iterable, Mapping, MutableMapping

import requests
from requests import Response
from requests.exceptions import ConnectionError, RequestException, Timeout

from autoflow.core.logger import get_logger

from .auth import AuthClient
from .config import DingDriveConfig, load_retry_config, load_timeout
from .models import DriveAuthError, DriveNotFound, DriveRequestError, DriveRetryableError

LOGGER = get_logger()

AUTHORIZATION_HEADER = "x-acs-dingtalk-access-token"
USER_AGENT = "Autoflow-DingDrive/1.0"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
OSS_SIGNATURE_ERRORS = {"SignatureDoesNotMatch", "RequestTimeTooSkewed"}


@dataclass(slots=True)
class RequestDiagnostics:
    """Captured diagnostics for troubleshooting."""

    method: str
    url: str
    header_keys: tuple[str, ...]
    status: int | None
    server_date: str | None


class HttpClient:
    """Request helper wrapping retries, auth, and diagnostics."""

    def __init__(
        self,
        config: DingDriveConfig,
        *,
        session: requests.Session | None = None,
        auth_client: AuthClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._session = session or requests.Session()
        self._session.verify = config.verify_tls
        self._session.trust_env = config.trust_env
        if config.proxies:
            self._session.proxies.update(config.proxies)
        self._session.headers.setdefault("User-Agent", USER_AGENT)
        self._auth = auth_client or AuthClient(config, session=self._session)
        self._logger = logger or LOGGER
        self._retry_config = load_retry_config(config)
        self._timeout = load_timeout(config)

    @property
    def session(self) -> requests.Session:
        """Expose the reusable session (needed for streaming downloads)."""

        return self._session

    @property
    def auth_client(self) -> AuthClient:
        """Return the authentication helper used by this client."""

        return self._auth

    def request_openapi(
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
    ) -> Response:
        """Perform an OpenAPI request with automatic access token injection."""

        url = self._compose_url(path)
        return self._request(
            method,
            url,
            headers=headers,
            params=params,
            json_body=json_body,
            data=data,
            stream=stream,
            expected_status=expected_status,
            timeout=timeout,
            allow_retry=allow_retry,
            attach_token=True,
        )

    def request_oss(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        data: object | None = None,
        expected_status: Iterable[int] = (200, 201, 204),
        stream: bool = False,
        timeout: float | None = None,
        allow_retry: bool = True,
    ) -> Response:
        """Perform a direct OSS request (no token injection)."""

        return self._request(
            method,
            url,
            headers=headers,
            data=data,
            stream=stream,
            expected_status=expected_status,
            timeout=timeout,
            allow_retry=allow_retry,
            attach_token=False,
        )

    # Internal helpers -------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None,
        params: Mapping[str, str] | None = None,
        json_body: Mapping[str, object] | None = None,
        data: object | None,
        stream: bool,
        expected_status: Iterable[int],
        timeout: float | None,
        allow_retry: bool,
        attach_token: bool,
    ) -> Response:
        attempts = self._retry_config.max_attempts if allow_retry else 1
        base_backoff = max(0.05, self._retry_config.backoff_ms / 1000.0)
        max_backoff = max(base_backoff, self._retry_config.max_backoff_ms / 1000.0)
        timeout_value = timeout or self._timeout
        expected = tuple(expected_status)
        refresh_token_next = False

        for attempt in range(1, attempts + 1):
            last_error: DriveRetryableError | DriveRequestError | DriveAuthError | None = None
            request_headers: MutableMapping[str, str] = dict(headers or {})
            if attach_token:
                token = self._auth.get_token(force_refresh=refresh_token_next)
                request_headers[AUTHORIZATION_HEADER] = token
                refresh_token_next = False

            diagnostics = RequestDiagnostics(
                method=method,
                url=self._redact_url(url),
                header_keys=tuple(sorted(request_headers.keys())),
                status=None,
                server_date=None,
            )

            try:
                response = self._session.request(
                    method,
                    url,
                    headers=request_headers,
                    params=dict(params or {}),
                    json=json_body,
                    data=data,
                    timeout=timeout_value,
                    stream=stream,
                )
            except Timeout as exc:
                last_error = DriveRetryableError("Request timed out", payload={"url": diagnostics.url})
                self._logger.warning(
                    "dingdrive.http timeout method=%s url=%s attempt=%d",
                    diagnostics.method,
                    diagnostics.url,
                    attempt,
                    exc_info=exc,
                )
            except (ConnectionError, RequestException) as exc:
                last_error = DriveRetryableError("Request failed", payload={"url": diagnostics.url})
                self._logger.warning(
                    "dingdrive.http connection_error method=%s url=%s attempt=%d error=%s",
                    diagnostics.method,
                    diagnostics.url,
                    attempt,
                    type(exc).__name__,
                    exc_info=exc,
                )
            else:
                diagnostics = diagnostics.__class__(
                    method=diagnostics.method,
                    url=diagnostics.url,
                    header_keys=diagnostics.header_keys,
                    status=response.status_code,
                    server_date=response.headers.get("Date"),
                )
                status = response.status_code
                payload = self._safe_json(response)
                if status in expected:
                    return response

                if status == 401 and attach_token:
                    self._auth.invalidate()
                    self._logger.info(
                        "dingdrive.http unauthorized method=%s url=%s -- refreshing token",
                        diagnostics.method,
                        diagnostics.url,
                    )
                    last_error = DriveAuthError("Unauthorized", status_code=status, payload=payload)
                    refresh_token_next = True
                elif status == 404:
                    raise DriveNotFound("Resource not found", status_code=status, payload=payload)
                elif status == 403 or self._contains_signature_error(payload):
                    self._log_forbidden(diagnostics, payload)
                    raise DriveAuthError("Forbidden", status_code=status, payload=payload)
                elif allow_retry and status in RETRYABLE_STATUS:
                    self._logger.warning(
                        "dingdrive.http retryable_status method=%s url=%s status=%d",
                        diagnostics.method,
                        diagnostics.url,
                        status,
                    )
                    last_error = DriveRetryableError(
                        "Retryable response",
                        status_code=status,
                        payload=payload,
                    )
                else:
                    raise DriveRequestError(
                        f"Unexpected status {status}",
                        status_code=status,
                        payload=payload,
                    )

            if attempt < attempts:
                self._sleep_with_backoff(base_backoff, max_backoff, attempt)
                if refresh_token_next:
                    continue
                refresh_token_next = attach_token and isinstance(last_error, DriveAuthError)
                continue
            if last_error is not None:
                raise last_error
            raise DriveRetryableError("Exhausted retries", payload={"url": diagnostics.url})

        raise DriveRetryableError("Exhausted retries", payload={"url": self._redact_url(url)})

    def _log_forbidden(self, diagnostics: RequestDiagnostics, payload: dict[str, object]) -> None:
        hint = "Sync server/client time, verify app scope per DingDrive handbook"
        self._logger.error(
            "dingdrive.http forbidden method=%s url=%s status=%s header_keys=%s server_date=%s hint=%s payload_code=%s",
            diagnostics.method,
            diagnostics.url,
            diagnostics.status,
            ",".join(diagnostics.header_keys),
            diagnostics.server_date,
            hint,
            payload.get("code") or payload.get("errorCode"),
        )

    def _contains_signature_error(self, payload: Mapping[str, object]) -> bool:
        code = str(payload.get("code") or payload.get("errorCode") or "")
        message = str(payload.get("message") or payload.get("msg") or "")
        if code in OSS_SIGNATURE_ERRORS:
            return True
        body = str(payload.get("body") or "")
        haystack = f"{message} {body}"
        return any(keyword in haystack for keyword in OSS_SIGNATURE_ERRORS)

    def _redact_url(self, url: str) -> str:
        if "?" in url:
            return url.split("?")[0]
        return url

    def _compose_url(self, path: str) -> str:
        base = self._config.base_url.rstrip("/")
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{base}{path}"

    def _sleep_with_backoff(self, base: float, maximum: float, attempt: int) -> None:
        delay = min(maximum, base * (2 ** (attempt - 1)))
        jitter = random.uniform(0, delay / 2)
        time.sleep(delay + jitter)

    def _safe_json(self, response: Response) -> dict[str, object]:
        try:
            return response.json()
        except ValueError:
            text = response.text
            if len(text) > 200:
                text = text[:200] + "..."
            return {"body": text}


__all__ = ["HttpClient", "AUTHORIZATION_HEADER"]
