"""HTTP client cho RIPE Atlas: retry có giới hạn, exponential backoff full jitter, Retry-After.

Phân loại lỗi:
- 429: retry; ưu tiên header Retry-After (giây hoặc HTTP-date), giới hạn bởi backoff_max_s.
- 5xx và lỗi mạng (connect/read timeout, stream bị ngắt): retry với backoff.
- 4xx khác: lỗi vĩnh viễn, không retry (tham số sai, measurement không tồn tại...).
- Response vượt max_response_bytes: không retry; cần chia nhỏ nhóm probe hoặc cửa sổ.

Hash: `body_sha256` là SHA-256 của toàn bộ body đã giải mã Content-Encoding (bytes mà
`requests` trả về), tính TRƯỚC khi dlt serialize và nén gzip.
"""

from __future__ import annotations

import hashlib
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlencode

import requests

from ingestion.logs import log_event
from ingestion.metrics import record_http
from ingestion.settings import API_BASE_URL, HttpSettings

RETRYABLE_EXCEPTIONS = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ContentDecodingError,
)
SECRET_PARAMS = {"key"}


class AtlasHttpError(RuntimeError):
    def __init__(self, message: str, *, status: int | None, url: str, retryable: bool):
        super().__init__(message)
        self.status = status
        self.url = url
        self.retryable = retryable


class ResponseTooLarge(AtlasHttpError):
    pass


@dataclass
class ResponseInfo:
    url: str  # đã loại tham số bí mật
    params: dict[str, Any]
    status: int
    attempts: int
    bytes: int
    body_sha256: str
    fetched_at: str
    duration_s: float
    retry_statuses: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def sanitize_params(params: dict[str, Any]) -> dict[str, Any]:
    return {k: ("<redacted>" if k in SECRET_PARAMS else v) for k, v in params.items()}


def sanitized_url(path: str, params: dict[str, Any]) -> str:
    return f"{API_BASE_URL}{path}?{urlencode(sanitize_params(params))}"


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    now = now or datetime.now(timezone.utc)
    return max(0.0, (when - now).total_seconds())


class AtlasHttp:
    def __init__(
        self,
        settings: HttpSettings,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: Callable[[], float] = random.random,
    ):
        self.settings = settings
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "ripe-atlas-lakehouse/0.2 (+batch research pipeline)")
        self._sleep = sleep
        self._rng = rng

    def backoff_delay(self, attempt: int, retry_after: float | None) -> float:
        cap = self.settings.backoff_max_s
        if retry_after is not None:
            return min(retry_after, cap)
        return self._rng() * min(cap, self.settings.backoff_base_s * 2 ** (attempt - 1))

    def get_bytes(self, path: str, params: dict[str, Any]) -> tuple[bytes, ResponseInfo]:
        """GET và đọc toàn bộ body (giới hạn kích thước). Retry toàn bộ request khi stream bị ngắt."""
        url = sanitized_url(path, params)
        retry_statuses: list[str] = []
        started = time.monotonic()
        for attempt in range(1, self.settings.max_attempts + 1):
            retry_after = None
            try:
                with self.session.get(
                    API_BASE_URL + path,
                    params=params,
                    stream=True,
                    timeout=(self.settings.connect_timeout_s, self.settings.read_timeout_s),
                ) as response:
                    status = response.status_code
                    if status == 200:
                        body = self._read_capped(response, url)
                        info = ResponseInfo(
                            url=url,
                            params=sanitize_params(params),
                            status=status,
                            attempts=attempt,
                            bytes=len(body),
                            body_sha256=hashlib.sha256(body).hexdigest(),
                            fetched_at=datetime.now(timezone.utc).isoformat(),
                            duration_s=round(time.monotonic() - started, 3),
                            retry_statuses=retry_statuses,
                        )
                        log_event("ingest", "http_ok", url=url, attempts=attempt, bytes=len(body), duration_s=info.duration_s)
                        record_http(path, status, info.duration_s, retry_statuses, len(body))
                        return body, info
                    if status == 429 or status >= 500:
                        retry_after = parse_retry_after(response.headers.get("Retry-After"))
                        retry_statuses.append(str(status))
                        error = AtlasHttpError(f"HTTP {status} for {url}", status=status, url=url, retryable=True)
                    else:
                        raise AtlasHttpError(f"HTTP {status} for {url}", status=status, url=url, retryable=False)
            except RETRYABLE_EXCEPTIONS as exc:
                retry_statuses.append(type(exc).__name__)
                error = AtlasHttpError(f"{type(exc).__name__} for {url}: {exc}", status=None, url=url, retryable=True)
            if attempt == self.settings.max_attempts:
                log_event("ingest", "http_failed", url=url, attempts=attempt, retry_statuses=retry_statuses)
                record_http(path, error.status, round(time.monotonic() - started, 3), retry_statuses[:-1])
                raise error
            delay = self.backoff_delay(attempt, retry_after)
            log_event("ingest", "http_retry", url=url, attempt=attempt, reason=retry_statuses[-1], delay_s=round(delay, 2))
            self._sleep(delay)
        raise AssertionError("unreachable")

    def get_json(self, path: str, params: dict[str, Any]) -> tuple[Any, bytes, ResponseInfo]:
        import json

        body, info = self.get_bytes(path, params)
        try:
            return json.loads(body), body, info
        except ValueError as exc:
            raise AtlasHttpError(f"Invalid JSON from {info.url}", status=200, url=info.url, retryable=False) from exc

    def _read_capped(self, response: requests.Response, url: str) -> bytes:
        limit = self.settings.max_response_bytes
        chunks, total = [], 0
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            total += len(chunk)
            if total > limit:
                raise ResponseTooLarge(
                    f"Response exceeded {limit} bytes for {url}; reduce PROBE_CHUNK_SIZE",
                    status=200, url=url, retryable=False,
                )
            chunks.append(chunk)
        return b"".join(chunks)
