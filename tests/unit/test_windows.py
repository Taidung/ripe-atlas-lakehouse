from datetime import datetime, timezone

import pytest

from ingestion.windows import Window, hourly_windows, incremental_windows, parse_instant

UTC = timezone.utc


def test_api_params_map_half_open_window_to_inclusive_bounds():
    w = Window(datetime(2026, 11, 18, 13, tzinfo=UTC), datetime(2026, 11, 18, 14, tzinfo=UTC))
    assert w.api_params() == {"start": 1795006800, "stop": 1795006800 + 3599}
    assert w.contains(1795006800) and w.contains(1795010399)
    assert not w.contains(1795010400) and not w.contains(1795006799)


@pytest.mark.parametrize(
    "hour_utc, ictrl, fragment",
    [
        (16, "2026-11-18", "window_date=2026-11-18/window_hour=23"),  # 23:00 giờ VN
        (17, "2026-11-19", "window_date=2026-11-19/window_hour=00"),  # qua nửa đêm GMT+7
    ],
)
def test_ictrl_dt_uses_asia_ho_chi_minh(hour_utc, ictrl, fragment):
    w = Window(datetime(2026, 11, 18, hour_utc, tzinfo=UTC), datetime(2026, 11, 18, hour_utc + 1, tzinfo=UTC))
    assert w.ictrl_dt.isoformat() == ictrl
    assert w.path_fragment == fragment


def test_incremental_windows_cover_last_hour_plus_overlap():
    windows = incremental_windows(datetime(2026, 11, 18, 10, 25, tzinfo=UTC), overlap_hours=3)
    assert [w.start.hour for w in windows] == [6, 7, 8, 9]
    assert windows[-1].end == datetime(2026, 11, 18, 10, tzinfo=UTC)


def test_hourly_windows_require_aligned_bounds():
    with pytest.raises(ValueError):
        hourly_windows(datetime(2026, 11, 18, 10, 30, tzinfo=UTC), datetime(2026, 11, 18, 12, tzinfo=UTC))
    assert len(hourly_windows(datetime(2026, 11, 18, 10, tzinfo=UTC), datetime(2026, 11, 18, 12, tzinfo=UTC))) == 2


def test_parse_instant_requires_offset_and_converts_to_utc():
    assert parse_instant("2026-11-18T20:00:00+07:00") == datetime(2026, 11, 18, 13, tzinfo=UTC)
    assert parse_instant("2026-11-18T13:00:00Z") == datetime(2026, 11, 18, 13, tzinfo=UTC)
    with pytest.raises(ValueError):
        parse_instant("2026-11-18T13:00:00")
