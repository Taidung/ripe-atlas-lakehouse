# Image chạy runner biến đổi (python -m lakehouse) + dbt trên Trino. Build context là thư mục gốc repo.
# Không chứa engine: dữ liệu được Trino xử lý; container này chỉ điều phối (chọn manifest, gọi dbt, ghi trạng thái công bố).
FROM python:3.11-slim@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535

RUN pip install --no-cache-dir uv==0.11.26

WORKDIR /opt/app
COPY pyproject.toml uv.lock ./
# Chỉ nhóm `transform` trong uv.lock (dbt-core, dbt-trino, trino, prometheus-client, s3fs).
RUN uv sync --frozen --no-install-project --only-group transform && rm -rf /root/.cache

COPY transformation ./transformation
COPY config/metrics.json ./config/metrics.json

# Thư mục dbt chỉ đọc: target/ và logs/ ghi ra /tmp.
ENV PATH="/opt/app/.venv/bin:$PATH" \
    PYTHONPATH=/opt/app/transformation \
    APP_DIR=/opt/app \
    PYTHONUNBUFFERED=1 \
    PYTHONUTF8=1 \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false

RUN useradd --uid 10001 --create-home runner
USER runner

ENTRYPOINT ["python", "-m", "lakehouse"]
CMD ["--help"]
