"""Thành phần CHẤT LƯỢNG DỮ LIỆU (data quality).

Module:
  results.py    Kiểu kết quả kiểm tra thống nhất (dict) + mức độ critical/warn + hàm tổng hợp.
  gx_suites.py  Bộ expectation Great Expectations cho từng bảng (Silver fact/reply/dim, Gold target).
  gx_runner.py  Chạy một suite qua GX Checkpoint, lưu validation result + Data Docs vào artifacts/gx.
  checks.py     Kiểm tra viết bằng Spark mà GX không diễn đạt gọn: SCD2, đối soát Bronze/Silver, nhất quán Gold.
  recorder.py   Ghi kết quả kiểm tra vào bảng nessie.ops.dq_result.

Quy ước mức độ:
  critical = vi phạm hợp đồng dữ liệu -> KHÔNG công bố partition (giữ bản đã công bố trước đó).
  warn     = bất thường cần xem nhưng vẫn công bố (published_with_warnings).
Kết quả đo mạng xấu (mất gói, all_lost) là dữ liệu HỢP LỆ, không phải lỗi chất lượng.
"""
