"""Ghi staging: dlt ghi object dữ liệu (jsonl.gz), fsspec ghi manifest và hash index.

Layout trong bucket staging (gốc `<root>` = s3://<bucket>/ripe_atlas):

  <root>/ping_result_raw/window_date=YYYY-MM-DD/window_hour=HH/msm_id=N/<load_id>.<file_id>.jsonl.gz
  <root>/probe_metadata_raw/ictrl_dt=YYYY-MM-DD/<load_id>.<file_id>.jsonl.gz
  <root>/measurement_metadata_raw/ictrl_dt=YYYY-MM-DD/<load_id>.<file_id>.jsonl.gz
  <root>/_manifests/<entity>/<partition>/<run_id>.json      # receipt: object nào thuộc lần chạy nào
  <root>/_index/ping_result_raw/<partition>/<run_id>.tsv.gz  # (probe_id, event_ts, raw_sha256) đã stage
  <root>/_probe_sets/<version>.json                          # bản sao tập probe đã dùng

window_date/window_hour theo giờ Asia/Ho_Chi_Minh. Đây chỉ là tổ chức thư mục staging, không phải
partition spec Iceberg (Bronze partition theo ictrl_dt, measurement_id).

Manifest được ghi SAU KHI dlt load thành công. Downstream chỉ đọc object liệt kê trong manifest
`status=complete`; object không có manifest (load thất bại giữa chừng) bị bỏ qua và dọn sau.
"""

from __future__ import annotations

import gzip
import json
import tempfile
from dataclasses import dataclass
from typing import Any

import dlt
import fsspec
from dlt.common.configuration.specs import AwsCredentials

from ingestion.settings import DATASET_NAME, StagingSettings

MANIFEST_VERSION = 1


@dataclass
class StagingStore:
    """Đích staging. `bucket_url` là gốc bucket (s3://staging hoặc thư mục local cho test)."""

    bucket_url: str
    fs: Any
    credentials: AwsCredentials | None = None

    @classmethod
    def from_settings(cls, settings: StagingSettings) -> StagingStore:
        fs = fsspec.filesystem(
            "s3",
            key=settings.access_key,
            secret=settings.secret_key,
            client_kwargs={"endpoint_url": settings.endpoint_url, "region_name": settings.region},
        )
        credentials = AwsCredentials(
            aws_access_key_id=settings.access_key,
            aws_secret_access_key=settings.secret_key,
            endpoint_url=settings.endpoint_url,
            region_name=settings.region,
        )
        return cls(f"s3://{settings.bucket}", fs, credentials)

    @classmethod
    def local(cls, directory: str) -> StagingStore:
        """Đích trên đĩa local (file:// URI), dùng cho test offline với dlt thật."""
        from pathlib import Path

        return cls(Path(directory).resolve().as_uri(), fsspec.filesystem("file"))

    @property
    def root(self) -> str:
        return f"{self.bucket_url}/{DATASET_NAME}"

    # ------------------------------------------------------------------ objects
    def _ensure_parent(self, path: str) -> None:
        # Object store không có thư mục; chỉ filesystem local cần tạo thư mục cha.
        if not self.bucket_url.startswith("s3://"):
            self.fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)

    def write_json(self, path: str, document: dict[str, Any]) -> None:
        self._ensure_parent(path)
        with self.fs.open(path, "wb") as fh:
            fh.write(json.dumps(document, indent=1, sort_keys=True, default=str).encode("utf-8"))

    def read_json(self, path: str) -> dict[str, Any]:
        with self.fs.open(path, "rb") as fh:
            return json.loads(fh.read().decode("utf-8"))

    def write_gzip_text(self, path: str, text: str) -> None:
        self._ensure_parent(path)
        with self.fs.open(path, "wb") as fh:
            fh.write(gzip.compress(text.encode("utf-8")))

    def read_gzip_text(self, path: str) -> str:
        with self.fs.open(path, "rb") as fh:
            return gzip.decompress(fh.read()).decode("utf-8")

    def glob(self, pattern: str) -> list[str]:
        protocol = "s3://" if self.bucket_url.startswith("s3://") else ""
        return sorted(protocol + p if protocol and not p.startswith(protocol) else p for p in self.fs.glob(pattern))

    def size(self, path: str) -> int:
        return int(self.fs.size(path))

    # ------------------------------------------------------------------ dlt
    def load_with_dlt(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        columns: dict[str, dict[str, Any]],
        layout: str,
        placeholders: dict[str, str],
    ) -> tuple[str, list[str]]:
        """Ghi `records` bằng dlt (jsonl.gz) vào layout cho trước; trả về (load_id, object paths).

        Mỗi lần gọi dùng pipeline tạm trong thư mục riêng: không có pending package cũ nào được
        load lại ngoài ý muốn. Tiến độ bền vững nằm ở manifest trên staging, không ở state dlt local.
        """
        destination = dlt.destinations.filesystem(
            bucket_url=self.bucket_url,
            credentials=self.credentials,
            layout=layout,
            extra_placeholders=placeholders,
        )
        resource = dlt.resource(
            records,
            name=table_name,
            write_disposition="append",
            max_table_nesting=0,
            columns=columns,
            schema_contract={"columns": "freeze"},
        )
        with tempfile.TemporaryDirectory(prefix="dlt-") as pipelines_dir:
            pipeline = dlt.pipeline(
                pipeline_name=f"atlas_{table_name}",
                pipelines_dir=pipelines_dir,
                destination=destination,
                dataset_name=DATASET_NAME,
            )
            info = pipeline.run(resource, loader_file_format="jsonl")
            info.raise_on_failed_jobs()
            load_id = info.loads_ids[-1]
        directory = layout.split("{load_id}")[0].format(table_name=table_name, **placeholders)
        objects = self.glob(f"{self.root}/{directory}{load_id}.*")
        if not objects:
            raise RuntimeError(f"dlt reported load {load_id} but no objects found under {directory}")
        return load_id, objects
