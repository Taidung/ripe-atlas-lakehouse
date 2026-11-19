#!/usr/bin/env bash
# Integration test: lỗi chất lượng critical phải chặn công bố Silver, và dữ liệu đã công bố được giữ nguyên.
# Chạy trên một nhánh Nessie tạm (dq-test-<ts>) nên không làm bẩn nhánh main:
#   1. Tạo nhánh từ main.
#   2. Trên nhánh: chèn vào Bronze một phiên bản mới (fetched_at muộn hơn) của một result thật, với rcvd > sent.
#   3. Chạy silver_build trên nhánh -> kỳ vọng exit 2, partition_status = blocked, fact trên nhánh giữ snapshot cũ.
#   4. Kiểm tra main không đổi, rồi xóa nhánh.
# Yêu cầu: stack core đang chạy, Silver ngày DAY đã có trên main.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
DAY="${1:-2026-11-19}"
BRANCH="dqtest$(date -u +%Y%m%d%H%M%S)"
NESSIE=http://localhost:19120/api/v2

spark_sql() { docker compose exec -T spark-iceberg spark-sql -S --conf "spark.sql.catalog.nessie.ref=$1" -e "$2" 2>/dev/null | tr -d '\r'; }
ref_hash() { curl -fsS "$NESSIE/trees/$1" | tr -d '\n ' | grep -o '"hash":"[0-9a-f]*"' | head -1 | cut -d'"' -f4; }
main_hash=$(ref_hash main)
cleanup() { curl -fsS -X DELETE "$NESSIE/trees/$BRANCH@$(ref_hash "$BRANCH")" >/dev/null 2>&1 && echo "  (đã xóa nhánh $BRANCH)" || echo "  CẢNH BÁO: chưa xóa được nhánh $BRANCH"; }
trap cleanup EXIT

echo "[1/4] Tạo nhánh $BRANCH từ main@${main_hash:0:12}"
curl -fsS -X POST "$NESSIE/trees?name=$BRANCH&type=BRANCH" -H 'Content-Type: application/json' \
  -d "{\"type\":\"BRANCH\",\"name\":\"main\",\"hash\":\"$main_hash\"}" >/dev/null

main_before=$(spark_sql main "SELECT snapshot_id FROM nessie.silver.fact_ping_result.snapshots ORDER BY committed_at DESC LIMIT 1")

echo "[2/4] Chèn bản ghi lỗi (rcvd > sent) vào bronze.ping_result_raw trên nhánh"
spark_sql "$BRANCH" "
INSERT INTO nessie.bronze.ping_result_raw
SELECT sha2(concat_ws('|', measurement_id, probe_id, event_ts, sha2(bad, 256)), 256), measurement_id, probe_id, af, event_ts, event_time,
       stored_ts, ictrl_dt, window_start, bad, sha2(bad, 256), 'changed', fw, mver, source_object, manifest_path, 'dq-test',
       dlt_load_id, fetched_at + INTERVAL 1 DAY, probe_set_version, 'dq-test', current_timestamp()
FROM (SELECT *, regexp_replace(raw_json, '\"rcvd\":[0-9]+', '\"rcvd\":9') AS bad FROM nessie.bronze.ping_result_raw
      WHERE ictrl_dt = DATE'$DAY' AND raw_json LIKE '%\"sent\":3%' LIMIT 1)" >/dev/null

echo "[3/4] Chạy silver_build trên nhánh (kỳ vọng: exit 2, blocked)"
set +e
docker compose exec -T spark-iceberg spark-submit --conf "spark.sql.catalog.nessie.ref=$BRANCH" \
  /opt/app/spark/jobs/silver_build.py --dates "$DAY" 2>&1 | grep -E '"event": "(day_blocked|day_written)"' | cut -c1-400
code=${PIPESTATUS[0]}
set -e
status=$(spark_sql "$BRANCH" "SELECT concat_ws(',', collect_set(status)) FROM nessie.ops.partition_status WHERE table_name='nessie.silver.fact_ping_result' AND ictrl_dt=DATE'$DAY'")
branch_snapshot=$(spark_sql "$BRANCH" "SELECT snapshot_id FROM nessie.silver.fact_ping_result.snapshots ORDER BY committed_at DESC LIMIT 1")
main_after=$(spark_sql main "SELECT snapshot_id FROM nessie.silver.fact_ping_result.snapshots ORDER BY committed_at DESC LIMIT 1")

echo "[4/4] Kết quả"
echo "  exit code silver_build     = $code (kỳ vọng 2)"
echo "  partition_status trên nhánh = $status (kỳ vọng blocked)"
echo "  fact snapshot trên nhánh    = $branch_snapshot (kỳ vọng = main trước test: $main_before)"
echo "  fact snapshot main sau test = $main_after"
[ "$code" = 2 ] && [ "$status" = blocked ] && [ "$branch_snapshot" = "$main_before" ] && [ "$main_after" = "$main_before" ] \
  && echo "PASS: lỗi critical chặn công bố, dữ liệu đã công bố giữ nguyên, main không bị ảnh hưởng" \
  || { echo "FAIL"; exit 1; }
