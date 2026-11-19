"""CLI ingestion: nhãn run_type của lần chạy (quyết định cảnh báo nào nhìn thấy lần chạy đó)."""

import pytest

from ingestion import cli


class Stop(Exception):
    pass


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["metadata"], "manual"),  # chạy tay không bao giờ mang nhãn của lịch chính
        (["metadata", "--run-type", "scheduled"], "scheduled"),  # DAG daily_metadata truyền rõ
        (["incremental"], "scheduled"),
        (["results", "--start", "2026-11-19T00:00:00Z", "--end", "2026-11-19T01:00:00Z"], "manual"),
        (["backfill", "--start", "2026-11-19T00:00:00Z", "--end", "2026-11-19T01:00:00Z"], "backfill"),
        (["backfill", "--start", "2026-11-19T00:00:00Z", "--end", "2026-11-19T01:00:00Z", "--run-type", "reconcile"], "reconcile"),
    ],
)
def test_run_type_default_and_override(monkeypatch, argv, expected):
    seen = []

    def fake_context(args, run_type):
        seen.append(run_type)
        raise Stop

    monkeypatch.setattr(cli, "make_context", fake_context)
    with pytest.raises(Stop):
        cli.main(argv)
    assert seen == [expected]


def test_run_type_rejects_unknown_value():
    with pytest.raises(SystemExit):
        cli.main(["metadata", "--run-type", "nightly"])
