import hashlib
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest
import requests

from ingestion.api_client import AtlasHttp, AtlasHttpError, ResponseTooLarge, parse_retry_after, sanitized_url
from ingestion.settings import HttpSettings


class FakeResponse:
    def __init__(self, status=200, body=b"", headers=None, fail_midway=False):
        self.status_code, self.body, self.headers, self.fail_midway = status, body, headers or {}, fail_midway

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, chunk_size):
        yield self.body[:3]
        if self.fail_midway:
            raise requests.exceptions.ChunkedEncodingError("connection broken")
        yield self.body[3:]


class FakeSession:
    def __init__(self, outcomes):
        self.outcomes, self.calls, self.headers = list(outcomes), [], {}

    def get(self, url, params, stream, timeout):
        self.calls.append((url, dict(params), timeout))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def client(outcomes, **overrides):
    settings = HttpSettings(**{"max_attempts": 4, "backoff_base_s": 2.0, "backoff_max_s": 60.0, **overrides})
    sleeps = []
    http = AtlasHttp(settings, session=FakeSession(outcomes), sleep=sleeps.append, rng=lambda: 0.5)
    return http, sleeps


def test_success_hashes_exact_body():
    http, sleeps = client([FakeResponse(body=b'{"a":1}\n')])
    body, info = http.get_bytes("probes/", {"format": "json"})
    assert body == b'{"a":1}\n' and info.attempts == 1 and sleeps == []
    assert info.body_sha256 == hashlib.sha256(body).hexdigest()


def test_429_honours_retry_after():
    http, sleeps = client([FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(body=b"ok")])
    _, info = http.get_bytes("x/", {})
    assert sleeps == [7.0] and info.attempts == 2 and info.retry_statuses == ["429"]


def test_retry_after_is_capped_by_backoff_max():
    http, sleeps = client([FakeResponse(429, headers={"Retry-After": "3600"}), FakeResponse(body=b"ok")])
    http.get_bytes("x/", {})
    assert sleeps == [60.0]


def test_5xx_uses_exponential_backoff_with_full_jitter():
    http, sleeps = client([FakeResponse(503), FakeResponse(502), FakeResponse(body=b"ok")])
    http.get_bytes("x/", {})
    assert sleeps == [0.5 * 2.0, 0.5 * 4.0]


def test_other_4xx_fails_immediately():
    http, sleeps = client([FakeResponse(404)])
    with pytest.raises(AtlasHttpError) as err:
        http.get_bytes("measurements/9999/", {})
    assert err.value.status == 404 and not err.value.retryable and sleeps == []


def test_network_errors_and_broken_stream_are_retried_then_exhausted():
    http, sleeps = client([
        requests.ConnectionError("refused"),
        FakeResponse(body=b"abcdef", fail_midway=True),
        requests.Timeout("read timeout"),
        FakeResponse(503),
    ])
    with pytest.raises(AtlasHttpError) as err:
        http.get_bytes("x/", {})
    assert err.value.retryable and len(sleeps) == 3


def test_oversized_response_is_not_retried():
    http, _ = client([FakeResponse(body=b"x" * 5000)], max_response_bytes=1024)
    with pytest.raises(ResponseTooLarge):
        http.get_bytes("x/", {})


def test_parse_retry_after_http_date_and_garbage():
    now = datetime(2026, 11, 18, tzinfo=timezone.utc)
    assert parse_retry_after(format_datetime(now + timedelta(seconds=30), usegmt=True), now) == 30
    assert parse_retry_after("soon", now) is None and parse_retry_after(None) is None


def test_sanitized_url_redacts_api_key():
    url = sanitized_url("probes/", {"key": "secret", "page": 2})
    assert "secret" not in url and "page=2" in url
