# orchestration/: Airflow (profile `orchestration`)

Airflow 3 standalone. Thư mục `airflow/dags` được mount vào `/opt/airflow/dags`.
Task gọi container `ingestion` và `spark-iceberg` qua Docker SDK (socket Docker được mount vào).

| File | Vai trò |
|---|---|
| `airflow/dags/ripe_atlas_dags.py` | Khai báo DAG: ingestion theo giờ → Bronze → Silver → Gold; bảo trì Iceberg |
| `airflow/dags/ripe_atlas/operators.py` | `SparkJobOperator` chạy `spark-submit /opt/app/transformation/jobs/<script>`, cùng operator chạy ingestion |
| `airflow/dags/.airflowignore` | Không để Airflow parse package `ripe_atlas/` như một DAG |

Pool `lakehouse_writer` (1 slot): mọi task ghi lakehouse chạy tuần tự, kể cả khi backfill trùng lịch chạy.
Pool `atlas_api` (1 slot): mọi task gọi RIPE Atlas API chạy tuần tự.
Cách vận hành, rerun và backfill: `docs/runbook.md`.
