#!/usr/bin/env bash
# Smoke test tương thích Spark -> Iceberg/Nessie -> Trino.
#   1. Spark ghi bảng nessie.smoke.compat_check (3 dòng, có kiểu timestamp/decimal/array).
#   2. Trino đọc lại: cùng số dòng, cùng tổng, cùng snapshot id hiện tại.
#   3. Trino đọc được mọi bảng bronze/silver đang có và đếm khớp với Spark.
# Yêu cầu: docker compose --profile analytics up -d (core + trino).
set -euo pipefail
cd "$(dirname "$0")/.."

# Ivy in dòng ":: loading settings ..." ra stdout khi resolve spark.jars.packages, nên lọc bỏ.
spark_sql() { docker compose exec -T spark-iceberg spark-sql -S -e "$1" 2>/dev/null | tr -d '\r' | grep -v '^:: ' || true; }
trino_sql() { docker compose exec -T trino trino --output-format TSV --execute "$1" | tr -d '\r'; }

echo "[1/3] Spark ghi nessie.smoke.compat_check"
spark_sql "CREATE NAMESPACE IF NOT EXISTS nessie.smoke;
CREATE OR REPLACE TABLE nessie.smoke.compat_check (
  id BIGINT, event_time TIMESTAMP, rtt DECIMAL(10,3), replies ARRAY<DOUBLE>, label STRING
) USING iceberg PARTITIONED BY (days(event_time));
INSERT INTO nessie.smoke.compat_check VALUES
  (1, TIMESTAMP '2026-11-18 16:59:59', 10.500, array(10.1, 10.9), 'utc-before-vn-midnight'),
  (2, TIMESTAMP '2026-11-18 17:00:00', 20.250, array(20.25), 'vn-midnight'),
  (3, TIMESTAMP '2026-11-18 17:00:01', NULL, array(), 'all-loss');" >/dev/null

spark_snapshot=$(spark_sql "SELECT snapshot_id FROM nessie.smoke.compat_check.snapshots ORDER BY committed_at DESC LIMIT 1")
spark_stats=$(spark_sql "SELECT count(*), sum(rtt), sum(size(replies)) FROM nessie.smoke.compat_check" | tr '\t' ' ')

echo "[2/3] Trino đọc lại"
trino_snapshot=$(trino_sql 'SELECT snapshot_id FROM nessie.smoke."compat_check$snapshots" ORDER BY committed_at DESC LIMIT 1')
trino_stats=$(trino_sql "SELECT count(*), sum(rtt), sum(cardinality(replies)) FROM nessie.smoke.compat_check" | tr '\t' ' ')
echo "  snapshot  spark=$spark_snapshot trino=$trino_snapshot"
echo "  count/sum spark=[$spark_stats] trino=[$trino_stats]"
[ -n "$spark_snapshot" ] && [ "$spark_snapshot" = "$trino_snapshot" ] || { echo "FAIL: snapshot khác nhau"; exit 1; }
[ "$spark_stats" = "$trino_stats" ] || { echo "FAIL: số liệu khác nhau"; exit 1; }

echo "[3/3] Đối chiếu các bảng bronze/silver hiện có"
fail=0
for ns in bronze silver; do
  for t in $(trino_sql "SHOW TABLES FROM nessie.$ns" 2>/dev/null); do
    s=$(spark_sql "SELECT count(*) FROM nessie.$ns.$t")
    r=$(trino_sql "SELECT count(*) FROM nessie.$ns.$t")
    status=OK; [ "$s" = "$r" ] || { status=FAIL; fail=1; }
    echo "  $status nessie.$ns.$t spark=$s trino=$r"
  done
done
[ "$fail" = 0 ] || exit 1
echo "PASS: Spark và Trino thấy cùng dữ liệu qua cùng Nessie catalog"
