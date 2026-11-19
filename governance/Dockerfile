# Ingestion metadata cho OpenMetadata 2.0.8: cùng phiên bản client với server.
FROM python:3.11-slim@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535
RUN pip install --no-cache-dir "openmetadata-ingestion[trino,superset]==2.0.8.0" "requests==2.34.2" "pyyaml==6.0.2"
WORKDIR /app
