FROM apache/superset:6.1.0@sha256:16b50bbef6648912a79e3293d418fefa743ce555d36c5af6b85d859119ed7f88

# Image chính thức không kèm driver Trino và driver Postgres cho metadata DB.
USER root
COPY requirements.txt /tmp/superset-requirements.txt
RUN . /app/.venv/bin/activate && uv pip install --no-cache -r /tmp/superset-requirements.txt
USER superset
