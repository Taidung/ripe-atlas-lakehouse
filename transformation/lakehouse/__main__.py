"""CLI của runner biến đổi (chạy trong container ripe-atlas-transform, Airflow gọi qua TransformOperator).

  python -m lakehouse bronze      --entity ping_result_raw --start ... --end ...   | --entity metadata
  python -m lakehouse silver      --start ... --end ... | --dates d1,d2 | --repair-unknown | --dims-only
  python -m lakehouse gold        --start ... --end ... | --dates d1,d2
  python -m lakehouse crosscheck  --date 2026-11-19
  python -m lakehouse maintenance [--before 2026-11-19] [--dry-run]

Mã thoát 2 (silver/gold): có ngày bị chặn vì lỗi chất lượng critical -> Airflow không retry.
"""

from __future__ import annotations

import argparse
import importlib
import sys

from lakehouse.common import add_run_type_arg
from lakehouse.metrics import job_metrics

COMMANDS = ("bronze", "silver", "gold", "crosscheck", "maintenance")


def parse(argv: list[str] | None = None):
    """-> (module của lệnh, args đã kiểm tra)."""
    parser = argparse.ArgumentParser(prog="python -m lakehouse", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    modules = {}
    for name in COMMANDS:
        module = importlib.import_module(f"lakehouse.{name}")
        modules[name] = module
        p = sub.add_parser(name, help=(module.__doc__ or "").splitlines()[0])
        module.add_args(p)
        add_run_type_arg(p)
    args = parser.parse_args(argv)
    module = modules[args.command]
    if hasattr(module, "validate"):
        module.validate(parser, args)
    return module, args


def main(argv: list[str] | None = None) -> int:
    module, args = parse(argv)
    stage = module.stage_name(args) if hasattr(module, "stage_name") else args.command
    if stage is None:  # lệnh kiểm chứng: không đẩy metric
        return module.run(args, None)
    with job_metrics(stage, args.run_type) as m:
        code = module.run(args, m)
        m.ok = code == 0
        return code


if __name__ == "__main__":
    sys.exit(main())
