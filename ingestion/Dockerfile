# Image chạy job ingestion (dlt -> SeaweedFS staging). Build context là thư mục gốc repo.
FROM python:3.11-slim@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535

RUN pip install --no-cache-dir uv==0.11.26

WORKDIR /opt/app
COPY pyproject.toml uv.lock ./
# Cài đúng phiên bản trong uv.lock (kể cả pytest để chạy test trong container).
RUN uv sync --frozen --no-install-project && rm -rf /root/.cache

COPY ingestion ./ingestion
COPY config ./config

ENV PATH="/opt/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    RUNTIME__DLTHUB_TELEMETRY=false

ENTRYPOINT ["python", "-m", "ingestion"]
CMD ["--help"]
