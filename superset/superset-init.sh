#!/bin/sh
# Khởi tạo Superset idempotent: chạy lại nhiều lần không lỗi, không tạo trùng.
set -eu

superset db upgrade

if ! superset fab list-users 2>/dev/null | grep -q "username:admin"; then
  superset fab create-admin \
    --username admin --firstname Admin --lastname User \
    --email admin@example.local --password "$SUPERSET_ADMIN_PASSWORD"
fi

superset init

# Kết nối Trino, catalog "nessie". Trino không bật xác thực trong môi trường đồ án;
# user "superset" chỉ để truy vết trong query history của Trino.
superset set-database-uri \
  --database_name "RIPE Atlas Lakehouse (Trino)" \
  --uri "trino://superset@trino:8080/nessie"
