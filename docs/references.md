# Tài liệu tham khảo

Ngày truy cập của tất cả link dưới đây: **2026-11-18**.

## RIPE Atlas

- Built-in measurements: https://atlas.ripe.net/docs/getting-started/built-in-measurements/
  - 1001/1004 là IPv4, 2001/2004 là IPv6, interval 240 giây; af chạy tùy theo khả năng của probe.
- Results API: https://atlas.ripe.net/docs/apis/rest-api-reference/measurements/measurements_results/
  - Tham số `start`, `stop`, `probe_ids`, `public_only`, `format` (`json`/`txt`).
- Results và latest: https://atlas.ripe.net/docs/apis/rest-api-manual/measurements/results-and-latest/
  - Các tham số chung (`fields`, `page`, …) không áp dụng cho endpoint results; response lớn được stream.
- Probes list: https://atlas.ripe.net/docs/apis/rest-api-reference/probes/probes_list/
- Pagination: https://atlas.ripe.net/docs/apis/rest-api-manual/core-concepts/pagination/
  - `page_size` mặc định 50, tối đa 500; đi theo `next`.
- Filtering: https://atlas.ripe.net/docs/apis/rest-api-manual/core-concepts/filtering/
  - Filter không hợp lệ bị bỏ qua im lặng.
- Rate limiting: https://atlas.ripe.net/docs/apis/rest-api-manual/core-concepts/rate-limiting/
  - Chỉ nêu HTTP 429 và khuyên back-off, không có con số cụ thể.
- Result format: https://atlas.ripe.net/docs/apis/measurement-result-format/
- Format version 5000: https://atlas.ripe.net/docs/apis/measurement-result-format/version-5000/

## Công cụ

- dlt filesystem destination (doc 1.32.0): https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem
  - jsonl là định dạng mặc định, gzip mặc định; `endpoint_url` cho S3-compatible; dlt không tự tạo bucket.
- GX Core với DataFrame (doc 1.25.0): https://docs.greatexpectations.io/docs/core/connect_to_data/dataframes/
- OpenMetadata Spark lineage: https://docs.open-metadata.org/latest/connectors/ingestion/lineage/spark-lineage
  - Dùng `io.openlineage:openlineage-spark_2.12` ≥ 1.37.0; endpoint `/api/v1/openlineage/lineage`.
- Prometheus, khi nào dùng Pushgateway: https://prometheus.io/docs/practices/pushing/
  - Chỉ cho service-level batch job; Pushgateway không tự xóa series.

## Phiên bản mới nhất

Phiên bản stable mới nhất được tra để lập compatibility matrix; **chưa pin** vào repo.

| Thành phần | Phiên bản | Nguồn |
|---|---|---|
| Trino | 484 | https://github.com/trinodb/trino/releases/tag/484 |
| Superset | 6.1.0 | https://github.com/apache/superset/releases/tag/6.1.0 |
| OpenMetadata | 2.0.8-release | https://github.com/open-metadata/OpenMetadata/releases/tag/2.0.8-release |
| Iceberg | 1.12.0 | https://iceberg.apache.org/releases/ |
| Airflow | 3.4.0 | https://github.com/apache/airflow/releases/tag/3.4.0 |
| Prometheus | 3.16.0 | https://github.com/prometheus/prometheus/releases/tag/v3.16.0 |
| Grafana | 13.3.1 | https://github.com/grafana/grafana/releases/tag/v13.3.1 |
| Pushgateway | 1.11.4 | https://github.com/prometheus/pushgateway/releases/tag/v1.11.4 |
| GX Core | 1.25.0? | Chỉ theo version selector trên trang doc; **chưa xác minh** trên PyPI (bị chặn bởi challenge page) |
