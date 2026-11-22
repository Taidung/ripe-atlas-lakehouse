"""DAG của pipeline RIPE Atlas Lakehouse.

ripe_atlas_hourly        5 phút sau mỗi giờ: ingest (giờ vừa xong + overlap) -> Bronze -> Silver -> Gold
ripe_atlas_daily_metadata 00:20 UTC: snapshot metadata -> Bronze -> Silver (--repair-unknown) -> Gold hôm qua + hôm nay
ripe_atlas_reconcile     02:30 UTC: tải lại cả ngày hôm qua (dữ liệu trễ > overlap) -> Bronze -> Silver -> Gold
ripe_atlas_backfill      thủ công, tham số start/end
ripe_atlas_maintenance   03:30 UTC: compaction Iceberg cho ngày đã đóng, dọn object mồ côi staging, xoay log Airflow

Mọi task ghi lakehouse dùng pool `lakehouse_writer` (1 slot): backfill và lịch chạy không bao giờ ghi đồng thời.
Mọi task gọi RIPE Atlas API dùng pool `atlas_api` (1 slot).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

sys.path.insert(0, str(Path(__file__).parent))
from ripe_atlas.operators import HourlyIngestionOperator, IngestionOperator, TransformOperator, utc_date, vn_date  # noqa: E402

DEFAULTS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}
START = datetime(2026, 11, 19)
WRITER, API = "lakehouse_writer", "atlas_api"
MACROS = {"vn_date": vn_date, "utc_date": utc_date}

with DAG(
    dag_id="ripe_atlas_hourly",
    description="Ingest giờ vừa kết thúc (+ overlap) và cập nhật Bronze/Silver/Gold",
    schedule="5 * * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["ripe-atlas", "hourly"],
    dagrun_timeout=timedelta(minutes=55),
) as hourly:
    ingest = HourlyIngestionOperator(task_id="ingest_results", arguments=[], pool=API, execution_timeout=timedelta(minutes=30))
    bronze = TransformOperator(
        task_id="bronze_results",
        command="bronze",
        arguments=["--entity", "ping_result_raw"],
        hourly_range=True,
        pool=WRITER,
        execution_timeout=timedelta(minutes=20),
        run_type="scheduled",
    )
    silver = TransformOperator(
        task_id="silver",
        command="silver",
        hourly_range=True,
        pool=WRITER,
        execution_timeout=timedelta(minutes=30),
        retries=1,
        run_type="scheduled",
    )
    gold = TransformOperator(
        task_id="gold",
        command="gold",
        hourly_range=True,
        pool=WRITER,
        execution_timeout=timedelta(minutes=30),
        retries=1,
        run_type="scheduled",
    )
    ingest >> bronze >> silver >> gold

with DAG(
    dag_id="ripe_atlas_daily_metadata",
    description="Snapshot metadata probe/measurement; sửa fact có probe chưa biết; cập nhật Gold",
    schedule="20 0 * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["ripe-atlas", "daily"],
    user_defined_macros=MACROS,
) as daily_metadata:
    meta = IngestionOperator(
        task_id="ingest_metadata", arguments=["metadata", "--run-type", "scheduled"], pool=API, execution_timeout=timedelta(minutes=20)
    )
    meta_bronze = TransformOperator(
        task_id="bronze_metadata",
        command="bronze",
        arguments=["--entity", "metadata"],
        pool=WRITER,
        execution_timeout=timedelta(minutes=20),
        run_type="scheduled",  # stage bronze_metadata chỉ có lần chạy này
    )
    repair = TransformOperator(
        task_id="silver_repair_unknown",
        command="silver",
        arguments=["--repair-unknown"],
        pool=WRITER,
        execution_timeout=timedelta(minutes=60),
        retries=1,
        run_type="repair",  # không dùng chung group với Silver hằng giờ
    )
    gold_recent = TransformOperator(
        task_id="gold_recent_days",
        command="gold",
        arguments=["--dates", "{{ vn_date(dag_run.run_after, -1) }},{{ vn_date(dag_run.run_after) }}"],
        pool=WRITER,
        execution_timeout=timedelta(minutes=45),
        retries=1,
        run_type="repair",  # không dùng chung group với Gold hằng giờ
    )
    meta >> meta_bronze >> repair >> gold_recent

with DAG(
    dag_id="ripe_atlas_reconcile",
    description="Tải lại cả ngày hôm qua (UTC) để thu dữ liệu đến muộn hơn overlap 3 giờ",
    schedule="30 2 * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["ripe-atlas", "daily"],
    user_defined_macros=MACROS,
) as reconcile:
    rng = ["--start", "{{ utc_date(dag_run.run_after, -1) }}T00:00:00Z", "--end", "{{ utc_date(dag_run.run_after) }}T00:00:00Z"]
    r_ingest = IngestionOperator(
        task_id="ingest_backfill_yesterday",
        arguments=["backfill", *rng, "--run-type", "reconcile"],
        pool=API,
        execution_timeout=timedelta(hours=2),
    )
    r_bronze = TransformOperator(
        task_id="bronze",
        command="bronze",
        arguments=["--entity", "ping_result_raw", *rng],
        pool=WRITER,
        execution_timeout=timedelta(minutes=45),
        run_type="reconcile",
    )
    r_silver = TransformOperator(
        task_id="silver",
        command="silver",
        arguments=rng,
        pool=WRITER,
        execution_timeout=timedelta(minutes=60),
        retries=1,
        run_type="reconcile",
    )
    r_gold = TransformOperator(
        task_id="gold",
        command="gold",
        arguments=rng,
        pool=WRITER,
        execution_timeout=timedelta(minutes=60),
        retries=1,
        run_type="reconcile",
    )
    r_ingest >> r_bronze >> r_silver >> r_gold

with DAG(
    dag_id="ripe_atlas_backfill",
    description="Backfill khoảng [start, end) UTC, cùng logic với lịch chạy chính; tiến độ ghi theo manifest",
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULTS, "retries": 1},
    tags=["ripe-atlas", "manual"],
    params={
        "start": Param("2026-11-19T00:00:00Z", type="string", description="ISO-8601 UTC, tròn giờ"),
        "end": Param("2026-11-19T03:00:00Z", type="string", description="ISO-8601 UTC, tròn giờ (exclusive)"),
        "probe_set": Param("dev-200-v2", type="string"),
    },
) as backfill:
    b_rng = ["--start", "{{ params.start }}", "--end", "{{ params.end }}"]
    b_ingest = IngestionOperator(
        task_id="ingest_backfill",
        arguments=["backfill", *b_rng, "--probe-set", "{{ params.probe_set }}"],
        pool=API,
        execution_timeout=timedelta(hours=6),
    )
    b_bronze = TransformOperator(
        task_id="bronze",
        command="bronze",
        arguments=["--entity", "ping_result_raw", *b_rng],
        pool=WRITER,
        execution_timeout=timedelta(hours=2),
        run_type="backfill",
    )
    b_silver = TransformOperator(
        task_id="silver", command="silver", arguments=b_rng, pool=WRITER, execution_timeout=timedelta(hours=3), run_type="backfill"
    )
    b_gold = TransformOperator(
        task_id="gold", command="gold", arguments=b_rng, pool=WRITER, execution_timeout=timedelta(hours=3), run_type="backfill"
    )
    b_ingest >> b_bronze >> b_silver >> b_gold

with DAG(
    dag_id="ripe_atlas_maintenance",
    description="Compaction Iceberg ngày đã đóng; dọn object mồ côi staging; xoay log Airflow",
    schedule="30 3 * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULTS, "retries": 1},
    tags=["ripe-atlas", "daily", "maintenance"],
) as maintenance:
    compact = TransformOperator(
        task_id="iceberg_compaction",
        command="maintenance",
        pool=WRITER,
        execution_timeout=timedelta(hours=2),
        run_type="scheduled",
    )
    orphans = IngestionOperator(
        task_id="staging_orphan_cleanup",
        arguments=["cleanup", "--orphan-min-age-hours", "48", "--apply"],
        execution_timeout=timedelta(minutes=30),
    )
    logs = BashOperator(
        task_id="rotate_airflow_task_logs", bash_command="find /opt/airflow/logs -type f -name '*.log' -mtime +14 -print -delete | wc -l"
    )
    compact >> orphans >> logs
