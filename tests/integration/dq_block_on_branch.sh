#!/usr/bin/env bash
# Integration test: lỗi chất lượng critical phải chặn công bố Silver, và bản đã công bố vẫn đọc được.
# Chạy trên nhánh Nessie tạm `dqtest` (catalog Trino lakehouse_dqtest) nên không làm bẩn nhánh main:
#   1. Tạo nhánh dqtest từ main.
#   2. Trên nhánh: chèn vào Bronze một phiên bản mới (fetched_at muộn hơn) của một result thật, với rcvd > sent.
#   3. Chạy silver trên nhánh -> kỳ vọng exit 2, status = blocked, fact giữ snapshot cũ, và
#      ops.partition_status VẪN trỏ tới snapshot đã công bố trước đó (không bị ghi đè thành NULL).
#   4. Chạy gold trên nhánh cho DAY -> Gold dựng từ snapshot Silver cũ, mart không rỗng.
#   5. Chạy gold trên nhánh cho một ngày không có Silver -> không ghi đè mart (không có commit mới).
#   6. Kiểm tra main không đổi, rồi xóa nhánh.
# Yêu cầu: core + trino đang chạy, image ripe-atlas-transform đã build, Silver và Gold ngày DAY đã có trên main.
set -euo pipefail
cd "$(dirname "$0")/../.."
export MSYS_NO_PATHCONV=1
DAY="${1:-2026-11-20}"
EMPTY_DAY="${2:-2026-11-01}"   # ngày không có dữ liệu Silver
BRANCH=dqtest                   # cố định: catalog serving/trino/catalog/lakehouse_dqtest.properties trỏ vào nhánh này
NESSIE=http://localhost:19120/api/v2
FACT=nessie.silver.fact_ping_result

sql() {  # sql <catalog> <query>: một giá trị / dòng, không header
  docker compose exec -T trino trino --user transform --catalog "$1" --output-format TSV --execute "$2" < /dev/null 2>/dev/null | tr -d '\r'
}
job() {  # job <args...>: chạy runner trên nhánh, in sự kiện chính, trả về exit code
  set +e
  # PUSHGATEWAY_URL rỗng: job thử nghiệm (cố ý bị chặn) không đẩy metric, không để lại cảnh báo trên Grafana.
  docker compose --profile jobs run --rm -T -e LAKEHOUSE_CATALOG=lakehouse_dqtest -e PUSHGATEWAY_URL= transform "$@" < /dev/null 2>&1 \
    | grep -E '"event": "(day_blocked|day_written|day_skipped)"' | cut -c1-300 >&2
  local code=${PIPESTATUS[0]}
  set -e
  echo "$code"
}
latest() { sql "$1" "SELECT snapshot_id FROM silver.\"fact_ping_result\$snapshots\" ORDER BY committed_at DESC LIMIT 1"; }
latest_gold() { sql "$1" "SELECT snapshot_id FROM gold.\"quality_hourly_target\$snapshots\" ORDER BY committed_at DESC LIMIT 1"; }
published() { sql "$1" "SELECT array_join(array_agg(DISTINCT CAST(snapshot_id AS varchar)), ',') FROM ops.partition_status WHERE table_name='$FACT' AND ictrl_dt=DATE '$DAY'"; }
gold_rows() { sql "$1" "SELECT count(*) FROM gold.quality_hourly_target WHERE ictrl_dt = DATE '$DAY'"; }
ref_hash() { curl -fsS "$NESSIE/trees/$1" | tr -d '\n ' | grep -o '"hash":"[0-9a-f]*"' | head -1 | cut -d'"' -f4; }
drop_branch() { h=$(ref_hash "$BRANCH" 2>/dev/null) && [ -n "$h" ] && curl -fsS -X DELETE "$NESSIE/trees/$BRANCH@$h" >/dev/null; }
cleanup() { drop_branch && echo "  (đã xóa nhánh $BRANCH)" || echo "  CẢNH BÁO: chưa xóa được nhánh $BRANCH"; }

drop_branch || true   # nhánh còn sót từ lần chạy trước bị ngắt
main_hash=$(ref_hash main)
trap cleanup EXIT

echo "[1/6] Tạo nhánh $BRANCH từ main@${main_hash:0:12}"
curl -fsS -X POST "$NESSIE/trees?name=$BRANCH&type=BRANCH" -H 'Content-Type: application/json' \
  -d "{\"type\":\"BRANCH\",\"name\":\"main\",\"hash\":\"$main_hash\"}" >/dev/null
main_before=$(latest lakehouse)
published_before=$(published lakehouse)
gold_rows_main=$(gold_rows lakehouse)

echo "[2/6] Chèn bản ghi lỗi (rcvd > sent) vào bronze.ping_result_raw trên nhánh"
sql lakehouse_dqtest "
INSERT INTO bronze.ping_result_raw
SELECT lower(to_hex(sha256(to_utf8(concat_ws('|', CAST(measurement_id AS varchar), CAST(probe_id AS varchar), CAST(event_ts AS varchar),
         lower(to_hex(sha256(to_utf8(bad))))))))), measurement_id, probe_id, af, event_ts, event_time, stored_ts, ictrl_dt, window_start,
       bad, lower(to_hex(sha256(to_utf8(bad)))), 'changed', fw, mver, source_object, manifest_path, 'dq-test', dlt_load_id,
       fetched_at + INTERVAL '1' DAY, probe_set_version, 'dq-test', current_timestamp
FROM (SELECT *, regexp_replace(raw_json, '\"rcvd\":[0-9]+', '\"rcvd\":9') AS bad FROM bronze.ping_result_raw
      WHERE ictrl_dt = DATE '$DAY' AND raw_json LIKE '%\"sent\":3%' LIMIT 1)" >/dev/null

echo "[3/6] silver trên nhánh (kỳ vọng: exit 2, blocked, snapshot công bố giữ nguyên)"
silver_code=$(job silver --dates "$DAY")
status=$(sql lakehouse_dqtest "SELECT array_join(array_agg(DISTINCT status), ',') FROM ops.partition_status WHERE table_name='$FACT' AND ictrl_dt=DATE '$DAY'")
branch_snapshot=$(latest lakehouse_dqtest)
published_after=$(published lakehouse_dqtest)

echo "[4/6] gold trên nhánh cho $DAY (kỳ vọng: dựng từ snapshot Silver cũ, không rỗng)"
gold_code=$(job gold --dates "$DAY")
gold_rows_branch=$(gold_rows lakehouse_dqtest)

echo "[5/6] gold trên nhánh cho $EMPTY_DAY (không có Silver; kỳ vọng: không ghi đè mart)"
gold_before_empty=$(latest_gold lakehouse_dqtest)
empty_code=$(job gold --dates "$EMPTY_DAY")
gold_after_empty=$(latest_gold lakehouse_dqtest)
main_after=$(latest lakehouse)

echo "[6/6] Kết quả"
echo "  silver exit                        = $silver_code (kỳ vọng 2)"
echo "  silver status trên nhánh           = $status (kỳ vọng blocked)"
echo "  fact snapshot trên nhánh           = $branch_snapshot (kỳ vọng = main trước test: $main_before)"
echo "  snapshot công bố trước / sau chặn  = $published_before / $published_after (kỳ vọng bằng nhau, khác rỗng)"
echo "  gold exit ($DAY)              = $gold_code (kỳ vọng 0); dòng target nhánh/main = $gold_rows_branch/$gold_rows_main"
echo "  gold exit ($EMPTY_DAY)              = $empty_code (kỳ vọng 0); snapshot target trước/sau = $gold_before_empty/$gold_after_empty"
echo "  fact snapshot main sau test        = $main_after"
[ "$silver_code" = 2 ] && [ "$status" = blocked ] && [ "$branch_snapshot" = "$main_before" ] \
  && [ -n "$published_before" ] && [ "$published_after" = "$published_before" ] \
  && [ "$gold_code" = 0 ] && [ "$gold_rows_branch" = "$gold_rows_main" ] && [ "$gold_rows_branch" -gt 0 ] \
  && [ "$empty_code" = 0 ] && [ "$gold_after_empty" = "$gold_before_empty" ] \
  && [ "$main_after" = "$main_before" ] \
  && echo "PASS: lỗi critical chặn công bố, bản đã công bố vẫn đọc được, Gold không bị xoá trắng, main không bị ảnh hưởng" \
  || { echo "FAIL"; exit 1; }
