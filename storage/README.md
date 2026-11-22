# storage/: lưu trữ đối tượng và catalog

| Thành phần | Service Compose | Vai trò |
|---|---|---|
| SeaweedFS (S3 API) | `seaweedfs` (một tiến trình `weed server`: master + volume + filer + S3; alias mạng `s3`), `init-buckets` | Bucket `staging` (dữ liệu thô từ ingestion) và bucket `warehouse` (file Iceberg) |
| Nessie + Postgres | `nessie`, `postgres` | Catalog Iceberg có nhánh và commit. Bronze/Silver/Gold/ops đều nằm trong catalog `nessie` |

## File

| File | Vai trò |
|---|---|
| `seaweedfs/s3.json` | Danh tính S3 và quyền: `atlas-ingest` chỉ ghi `staging`, `processing` đọc `staging` và ghi `warehouse`, `admin` quản trị. Khoá lấy từ `.env`, **không** commit khoá thật |

## Quy ước

- Không chạy `expire_snapshots`: Nessie giữ lịch sử, và Gold đọc Silver theo snapshot đã công bố (time travel).
- Muốn test mà không làm bẩn dữ liệu chính thì dùng nhánh Nessie tạm (ví dụ `tests/integration/dq_block_on_branch.sh`).
- Lệnh `docker compose … down -v` **xoá toàn bộ volume** gồm dữ liệu SeaweedFS và lịch sử Nessie.
- SeaweedFS dùng một volume `volume_data:/data` cho cả 3 phần: `m9333/` (trạng thái master), `filerldb2/` (metadata
  thư mục/bucket của filer, đường dẫn lấy từ `filer.toml` có sẵn trong image) và các file `*.dat`/`*.idx` (dữ liệu).
  **Đừng đặt `WEED_LEVELDB2_DIR`**: trỏ filer ra ngoài `/data` thì metadata không nằm trong volume, container tạo lại
  là mất và mọi bucket trông như rỗng dù file dữ liệu vẫn còn.

Chi tiết phiên bản và cổng: `docs/compatibility.md`, `README.md` §2.
