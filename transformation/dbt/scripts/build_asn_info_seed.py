"""Sinh seed dbt `asn_info.csv` (asn, as_holder, isp_name, isp_type): tên nhà mạng cho mọi ASN của probe trong
config/probe_sets/*.json.

  python transformation/dbt/scripts/build_asn_info_seed.py

- ASN: asn_v4 và asn_v6 hiện tại của từng probe (RIPE Atlas probes API).
- as_holder: tên đăng ký của ASN (RIPEstat as-overview), giữ nguyên để tra cứu.
- isp_name, theo thứ tự ưu tiên: tên viết tay cho ASN Việt Nam (VN_ISPS); tên mạng trên PeeringDB (nhà mạng tự khai,
  gọn: "Telia Sweden", "Verizon"); cuối cùng là as_holder bỏ mã handle ở đầu ("KPN KPN B.V." -> "KPN B.V.").
- isp_type: ASN Việt Nam theo VN_ISPS; ASN khác map từ info_types của PeeringDB (PEERINGDB_TYPES); trống nếu ASN không
  có trên PeeringDB hoặc không khai loại.

ASN của probe có thể đổi theo thời gian. Gold hiện ASN chưa có trong seed là 'AS<n>': khi đó, hoặc khi đổi probe set,
chạy lại script rồi commit CSV. Cần mạng; CI không chạy script này.
"""

from __future__ import annotations

import csv
import json
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "transformation" / "dbt" / "seeds" / "asn_info.csv"
ATLAS = "https://atlas.ripe.net/api/v2/probes/"
RIPESTAT = "https://stat.ripe.net/data/as-overview/data.json"
PEERINGDB = "https://www.peeringdb.com/api/net"

# info_types của PeeringDB -> loại mạng, xét theo thứ tự (mạng khai nhiều loại lấy loại đứng trước).
PEERINGDB_TYPES = [
    ("Cable/DSL/ISP", "ISP"),
    ("NSP", "ISP"),
    ("Content", "Hosting/DC"),
    ("Educational/Research", "Giáo dục/Nghiên cứu"),
    ("Enterprise", "Doanh nghiệp"),
    ("Non-Profit", "Khác"),
    ("Network Services", "Khác"),
]

# ASN Việt Nam của các probe trong probe set (tra RIPEstat 2026-11-22). Một nhà mạng có thể có nhiều ASN.
VN_ISPS: dict[int, tuple[str, str]] = {
    7552: ("Viettel", "ISP"),
    18403: ("FPT Telecom", "ISP"),
    45899: ("VNPT", "ISP"),
    135905: ("VNPT", "ISP"),
    38733: ("CMC Telecom", "ISP"),
    45903: ("CMC Telecom", "ISP"),
    131429: ("MobiFone", "ISP"),
    38731: ("Viettel IDC", "Hosting/DC"),
    56150: ("VHost", "Hosting/DC"),
    63734: ("365 Online (GreenCloudVPS)", "Hosting/DC"),
    135932: ("VNData", "Hosting/DC"),
    135951: ("Webico", "Hosting/DC"),
    140825: ("HostingViet", "Hosting/DC"),
    149107: ("TrumVPS", "Hosting/DC"),
    150818: ("HyperCore", "Hosting/DC"),
    150873: ("ThuongCloud", "Hosting/DC"),
    140787: ("LAMA", "Khác"),
    150826: ("VR Việt Nam", "Khác"),
    151942: ("Phan Hữu Bảo", "Khác"),
    153003: ("Lá Chắn Số", "Khác"),
    206098: ("Rose Net", "Khác"),
}


def get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "ripe-atlas-lakehouse seed builder"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.load(resp)
        except OSError:
            if attempt == 3:
                raise
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def probe_asns() -> set[int]:
    ids = sorted(
        {
            pid
            for path in (ROOT / "config" / "probe_sets").glob("*.json")
            for key in ("probes_v4", "probes_v6")
            for pid in json.loads(path.read_text(encoding="utf-8"))[key]
        }
    )
    asns: set[int] = set()
    for i in range(0, len(ids), 100):
        chunk = ",".join(map(str, ids[i : i + 100]))
        data = get_json(f"{ATLAS}?id__in={chunk}&page_size=100&fields=id,asn_v4,asn_v6")
        asns |= {p[k] for p in data["results"] for k in ("asn_v4", "asn_v6") if p.get(k)}
    return asns | set(VN_ISPS)


def peeringdb_networks(asns: list[int]) -> dict[int, dict]:
    nets: dict[int, dict] = {}
    for i in range(0, len(asns), 100):
        chunk = ",".join(map(str, asns[i : i + 100]))
        for net in get_json(f"{PEERINGDB}?asn__in={chunk}&fields=asn,name,info_type,info_types")["data"]:
            nets[net["asn"]] = net
    return nets


def holder_name(holder: str) -> str:
    """as_holder dạng "<HANDLE> - <mô tả>" hoặc "<HANDLE> <mô tả>": bỏ handle, giữ mô tả."""
    if " - " in holder:
        return holder.split(" - ", 1)[1].strip()
    head, _, rest = holder.partition(" ")
    return rest.strip() if rest and (head.isupper() or "-" in head) else holder


def peeringdb_type(net: dict) -> str:
    types = set(net.get("info_types") or []) | {net.get("info_type") or ""}
    return next((label for pdb, label in PEERINGDB_TYPES if pdb in types), "")  # mạng không khai loại: để trống


def main() -> None:
    asns = sorted(probe_asns())
    nets = peeringdb_networks(asns)
    rows = []
    for asn in asns:
        holder = get_json(f"{RIPESTAT}?resource=AS{asn}&sourceapp=ripe-atlas-lakehouse")["data"].get("holder") or ""
        if asn in VN_ISPS:
            name, kind = VN_ISPS[asn]
        elif asn in nets:
            name, kind = nets[asn]["name"].strip(), peeringdb_type(nets[asn])
        else:
            name, kind = holder_name(holder) or f"AS{asn}", ""
        rows.append((asn, holder, name, kind))
    with OUT.open("w", encoding="utf-8", newline="\n") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["asn", "as_holder", "isp_name", "isp_type"])
        writer.writerows(rows)
    print(f"{OUT.relative_to(ROOT)}: {len(rows)} ASN, {sum(1 for a in asns if a in nets)} co tren PeeringDB")


if __name__ == "__main__":
    main()
