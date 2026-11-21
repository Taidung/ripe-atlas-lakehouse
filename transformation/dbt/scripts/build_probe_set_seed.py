"""Sinh seed dbt `probe_set_member.csv` (probe_set_version, af, probe_id) từ config/probe_sets/*.json.

  python transformation/dbt/scripts/build_probe_set_seed.py

Probe set là bất biến (sha256 của file nằm trong mọi manifest); thêm version mới thì chạy lại script và commit CSV.
CI kiểm tra CSV khớp với JSON (git diff --exit-code).
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def main() -> None:
    rows = []
    for path in sorted((ROOT / "config" / "probe_sets").glob("*.json")):
        ps = json.loads(path.read_text(encoding="utf-8"))
        for af, key in ((4, "probes_v4"), (6, "probes_v6")):
            rows += [(ps["version"], af, pid) for pid in sorted(ps[key])]
    out = ROOT / "transformation" / "dbt" / "seeds" / "probe_set_member.csv"
    with out.open("w", encoding="utf-8", newline="\n") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["probe_set_version", "af", "probe_id"])
        writer.writerows(rows)
    print(f"{out.relative_to(ROOT)}: {len(rows)} dòng")


if __name__ == "__main__":
    main()
