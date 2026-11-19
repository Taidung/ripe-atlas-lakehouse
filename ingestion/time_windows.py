"""Cửa sổ logic [start, end) theo giờ và quy tắc biên của API results.

API results coi cả `start` lẫn `stop` là inclusive (đã đo, docs/api-survey.md §2), nên cửa sổ
[S, E) được gọi bằng start=S, stop=E-1 rồi lọc lại phía client.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from ingestion.settings import REPORT_TZ

HOUR = timedelta(hours=1)


@dataclass(frozen=True, order=True)
class Window:
    start: datetime  # tz-aware, UTC
    end: datetime

    def __post_init__(self) -> None:
        for value in (self.start, self.end):
            if value.tzinfo is None:
                raise ValueError("Window bounds must be timezone-aware")
        if self.end <= self.start:
            raise ValueError("Window end must be after start")

    @property
    def start_ts(self) -> int:
        return int(self.start.timestamp())

    @property
    def end_ts(self) -> int:
        return int(self.end.timestamp())

    def api_params(self) -> dict[str, int]:
        """Tham số start/stop inclusive tương ứng cửa sổ nửa mở [start, end)."""
        return {"start": self.start_ts, "stop": self.end_ts - 1}

    def contains(self, ts: int) -> bool:
        return self.start_ts <= ts < self.end_ts

    @property
    def local_start(self) -> datetime:
        return self.start.astimezone(REPORT_TZ)

    @property
    def ictrl_dt(self) -> date:
        """Ngày kiểm soát (Asia/Ho_Chi_Minh) của cửa sổ; dùng cho partition và báo cáo."""
        return self.local_start.date()

    @property
    def path_fragment(self) -> str:
        local = self.local_start
        return f"window_date={local:%Y-%m-%d}/window_hour={local:%H}"

    @property
    def label(self) -> str:
        return f"{self.start:%Y-%m-%dT%H:%M:%SZ}"


def parse_instant(value: str) -> datetime:
    """Parse ISO-8601 có offset bắt buộc; trả về UTC."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include a UTC offset: {value}")
    return parsed.astimezone(timezone.utc)


def floor_hour(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def hourly_windows(start: datetime, end: datetime) -> list[Window]:
    """Chia [start, end) thành các cửa sổ 1 giờ. Hai biên phải tròn giờ."""
    start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    if start != floor_hour(start) or end != floor_hour(end):
        raise ValueError("start and end must be aligned to whole hours")
    if end <= start:
        raise ValueError("end must be after start")
    windows, cursor = [], start
    while cursor < end:
        windows.append(Window(cursor, cursor + HOUR))
        cursor += HOUR
    return windows


def incremental_windows(now: datetime, overlap_hours: int) -> list[Window]:
    """Giờ đã kết thúc gần nhất cộng `overlap_hours` giờ trước đó để thu dữ liệu đến muộn."""
    last_end = floor_hour(now)
    return hourly_windows(last_end - HOUR * (overlap_hours + 1), last_end)


def ensure_completed(windows: list[Window], now: datetime) -> None:
    incomplete = [w.label for w in windows if w.end > now]
    if incomplete:
        raise ValueError(f"Windows not yet complete: {incomplete}")
