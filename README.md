Start the services, then load the metadata snapshots from `staging` into the Iceberg bronze tables:

```bash
docker compose up -d
docker compose exec -T spark-iceberg spark-submit /opt/spark/jobs/staging_to_bronze.py
```

Load one completed ping hour (UTC+7). Repeating this command replaces that hour's bronze partition:

```bash
docker compose exec -T spark-iceberg spark-submit /opt/spark/jobs/staging_to_bronze.py --window-start 2026-11-18T13:00:00+07:00
```

Bronze tables are `nessie.bronze.measurements`, `nessie.bronze.probes`, and `nessie.bronze.ping_results`. Each row keeps the original JSON in `payload` and its staging path in `source_file`.

Run the Bronze unit tests in the Spark container (no S3 or Iceberg writes):

```bash
docker compose exec -T spark-iceberg spark-submit /opt/spark/tests/test_bronze.py
```

Build the Silver star schema from Bronze. Refresh dimensions after metadata, then load each completed ping hour:

```bash
docker compose exec -T spark-iceberg spark-submit /opt/spark/jobs/bronze_to_silver.py
docker compose exec -T spark-iceberg spark-submit /opt/spark/jobs/bronze_to_silver.py --window-start 2026-11-18T13:00:00+07:00
```

`dim_measurement` and `dim_probe` keep the latest metadata snapshot per ID (SCD1). `fact_ping` has one row per measurement, probe, and second; rerunning an hour replaces that hour's partition.

Run Silver unit tests without S3 or Iceberg writes:

```bash
docker compose exec -T spark-iceberg spark-submit /opt/spark/tests/test_silver.py
```
