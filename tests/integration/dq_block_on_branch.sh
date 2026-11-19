#!/usr/bin/env bash
# Integration test: lỗi chất lượng critical phải chặn công bố Silver, và bản đã công bố vẫn đọc được.
# Chạy trên một nhánh Nessie tạm (dqtest<ts>) nên không làm bẩn nhánh main:
#   1. Tạo nhánh từ main.
#   2. Trên nhánh: chèn vào Bronze một phiên bản mới (fetched_at muộn hơn) của một result thật, với rcvd > sent.
#   3. Chạy silver_build trên nhánh -> kỳ vọng exit 2, status = blocked, fact giữ snapshot cũ, và
#      ops.partition_status VẪN trỏ tới snapshot đã công bố trước đó (không bị ghi đè thành NULL).
#   4. Chạy gold_build trên nhánh cho DAY -> Gold dựng từ snapshot Silver cũ, mart không rỗng.
#   5. Chạy gold_build trên nhánh cho một ngày không có Silver -> không ghi đè mart (không có commit mới).
#   6. Kiểm tra main không đổi, rồi xóa nhánh.
# Yêu cầu: stack core đang chạy, Silver và Gold ngày DAY đã có trên main. Job trong test không đẩy metric.
set -euo pipefail
cd "$(dirname "$0")/../.."
export MSYS_NO_PATHCONV=1
DAY="${1:-2026-11-19}"
EMPTY_DAY="${2:-2026-11-01}"   # ngày không có dữ liệu Silver
BRANCH="dqtest$(date -u +%Y%m%d%H%M%S)"
NESSIE=http://localhost:19120/api/v2
FACT=nessie.silver.fact_ping_result
TARGET=nessie.gold.quality_hourly_target

spark_sql() { docker compose exec -T spark-iceberg spark-sql -S --conf "spark.sql.catalog.nessie.ref=$1" -e "$2" 2>/dev/null | tr -d '\r'; }
spark_job() {  # spark_job <ref> <script> <args...>: in các sự kiện chính, trả về exit code của job
  local ref=$1 script=$2; shift 2
  set +e
  # PUSHGATEWAY_URL rỗng: job thử nghiệm (cố ý bị chặn) không đẩy metric, nên không để lại cảnh báo trên Grafana.
  docker compose exec -T -e PUSHGATEWAY_URL= spark-iceberg spark-submit --conf "spark.sql.catalog.nessie.ref=$ref" \
    "/opt/app/transformation/jobs/$script" "$@" 2>&1 | grep -E '"event": "(day_blocked|day_written|day_skipped)"' | cut -c1-300 >&2
  local code=${PIPESTATUS[0]}
  set -e
  echo "$code"
}
latest() { spark_sql "$1" "SELECT snapshot_id FROM $2.snapshots ORDER BY committed_at DESC LIMIT 1"; }
published() { spark_sql "$1" "SELECT concat_ws(',', collect_set(cast(snapshot_id AS STRING))) FROM nessie.ops.partition_status WHERE table_name='$FACT' AND ictrl_dt=DATE'$DAY'"; }
ref_hash() { curl -fsS "$NESSIE/trees/$1" | tr -d '\n ' | grep -o '"hash":"[0-9a-f]*"' | head -1 | cut -d'"' -f4; }
main_hash=$(ref_hash main)
cleanup() { curl -fsS -X DELETE "$NESSIE/trees/$BRANCH@$(ref_hash "$BRANCH")" >/dev/null 2>&1 && echo "  (đã xóa nhánh $BRANCH)" || echo "  CẢNH BÁO: chưa xóa được nhánh $BRANCH"; }
trap cleanup EXIT

echo "[1/6] Tạo nhánh $BRANCH từ main@${main_hash:0:12}"
curl -fsS -X POST "$NESSIE/trees?name=$BRANCH&type=BRANCH" -H 'Content-Type: application/json' \
  -d "{\"type\":\"BRANCH\",\"name\":\"main\",\"hash\":\"$main_hash\"}" >/dev/null
main_before=$(latest main "$FACT")
published_before=$(published main)
gold_rows_main=$(spark_sql main "SELECT count(*) FROM $TARGET WHERE ictrl_dt = DATE'$DAY'")

echo "[2/6] Chèn bản ghi lỗi (rcvd > sent) vào bronze.ping_result_raw trên nhánh"
spark_sql "$BRANCH" "
INSERT INTO nessie.bronze.ping_result_raw
SELECT sha2(concat_ws('|', measurement_id, probe_id, event_ts, sha2(bad, 256)), 256), measurement_id, probe_id, af, event_ts, event_time,
       stored_ts, ictrl_dt, window_start, bad, sha2(bad, 256), 'changed', fw, mver, source_object, manifest_path, 'dq-test',
       dlt_load_id, fetched_at + INTERVAL 1 DAY, probe_set_version, 'dq-test', current_timestamp()
FROM (SELECT *, regexp_replace(raw_json, '\"rcvd\":[0-9]+', '\"rcvd\":9') AS bad FROM nessie.bronze.ping_result_raw
      WHERE ictrl_dt = DATE'$DAY' AND raw_json LIKE '%\"sent\":3%' LIMIT 1)" >/dev/null

echo "[3/6] silver_build trên nhánh (kỳ vọng: exit 2, blocked, snapshot công bố giữ nguyên)"
silver_code=$(spark_job "$BRANCH" silver_build.py --dates "$DAY")
status=$(spark_sql "$BRANCH" "SELECT concat_ws(',', collect_set(status)) FROM nessie.ops.partition_status WHERE table_name='$FACT' AND ictrl_dt=DATE'$DAY'")
branch_snapshot=$(latest "$BRANCH" "$FACT")
published_after=$(published "$BRANCH")

echo "[4/6] gold_build trên nhánh cho $DAY (kỳ vọng: dựng từ snapshot Silver cũ, không rỗng)"
gold_code=$(spark_job "$BRANCH" gold_build.py --dates "$DAY")
gold_rows_branch=$(spark_sql "$BRANCH" "SELECT count(*) FROM $TARGET WHERE ictrl_dt = DATE'$DAY'")

echo "[5/6] gold_build trên nhánh cho $EMPTY_DAY (không có Silver; kỳ vọng: không ghi đè mart)"
gold_before_empty=$(latest "$BRANCH" "$TARGET")
empty_code=$(spark_job "$BRANCH" gold_build.py --dates "$EMPTY_DAY")
gold_after_empty=$(latest "$BRANCH" "$TARGET")
main_after=$(latest main "$FACT")

echo "[6/6] Kết quả"
echo "  silver_build exit                  = $silver_code (kỳ vọng 2)"
echo "  silver status trên nhánh           = $status (kỳ vọng blocked)"
echo "  fact snapshot trên nhánh           = $branch_snapshot (kỳ vọng = main trước test: $main_before)"
echo "  snapshot công bố trước / sau chặn  = $published_before / $published_after (kỳ vọng bằng nhau, khác rỗng)"
echo "  gold_build exit ($DAY)        = $gold_code (kỳ vọng 0); dòng target nhánh/main = $gold_rows_branch/$gold_rows_main"
echo "  gold_build exit ($EMPTY_DAY)        = $empty_code (kỳ vọng 0); snapshot target trước/sau = $gold_before_empty/$gold_after_empty"
echo "  fact snapshot main sau test        = $main_after"
[ "$silver_code" = 2 ] && [ "$status" = blocked ] && [ "$branch_snapshot" = "$main_before" ] \
  && [ -n "$published_before" ] && [ "$published_after" = "$published_before" ] \
  && [ "$gold_code" = 0 ] && [ "$gold_rows_branch" = "$gold_rows_main" ] && [ "$gold_rows_branch" -gt 0 ] \
  && [ "$empty_code" = 0 ] && [ "$gold_after_empty" = "$gold_before_empty" ] \
  && [ "$main_after" = "$main_before" ] \
  && echo "PASS: lỗi critical chặn công bố, bản đã công bố vẫn đọc được, Gold không bị xoá trắng, main không bị ảnh hưởng" \
  || { echo "FAIL"; exit 1; }
