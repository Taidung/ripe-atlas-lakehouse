"""Cấu hình Superset cho đồ án. Secrets đọc từ biến môi trường do Compose truyền vào."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:"
    f"{os.environ['SUPERSET_DB_PASSWORD']}@superset-db:5432/superset"
)

# Cache kết quả chart ngắn (5 phút): dashboard không giữ số liệu cũ lâu sau khi Gold publish.
# SimpleCache là cache theo tiến trình; đủ cho môi trường đồ án một node.
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
