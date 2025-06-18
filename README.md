# Tối ưu Ingest bằng Airbyte OSS và Airflow

Đề tài nghiên cứu, thiết kế và triển khai hệ thống ingest dữ liệu sử dụng bộ công cụ Airbyte – Airflow

## Yêu cầu hệ thống

- Docker + Docker Compose
- Python 3.8+
- Hệ điều hành Linux/macOS/Windows (hỗ trợ Docker Desktop)
- RAM tối thiểu 4GB cho container

## Cài đặt môi trường

```bash
# Clone mã nguồn
git clone https://github.com/your-org/airbyte-airflow-ingest.git
cd airbyte-airflow-ingest

# Tạo virtual environment
python -m venv venv
source venv/bin/activate  # Hoặc: .\venv\Scripts\activate (Windows)

# Cài đặt các thư viện Python cần thiết
pip install -r requirements.txt

# Chạy toàn bộ stack (Airflow, Airbyte, Postgres nguồn & đích)
bash ./start.sh
```

## Cấu hình kết nối trong Airflow

Truy cập Airflow UI tại http://localhost:8080
Chọn Admin/Connections

### Tạo kết nối Airbyte API

- Connection ID: airbyte
- Connection Type: Airbyte
- Host: http://host.docker.internal:8000/api/public/v1/
- Client ID: 952f97d3-ee61-41ae-94d5-776cc63de7af
- Client Secret: 8vkOQ6k5NOjOsokfw9TZ6rwdOIIr57O9

### Tạo 3 kết nối PostgreSQL

| Connection ID    | Host                  | Port | Schema       |
|------------------|-----------------------|------|--------------|
| `postgres_film`  | host.docker.internal  | 5432 | film_db      |
| `postgres_ticket`| host.docker.internal  | 5433 | ticket_db    |
| `postgres_dwh`   | host.docker.internal  | 5434 | destination_db |

## Tạo Connection trong Airbyte UI

Truy cập http://localhost:8000 và cấu hình các connection sau:

| Connection Name  | Source     | Destination | Sync Mode                      | Schedule type  |
|------------------|------------|-------------|--------------------------------|--------|
| `film_schedule`  | `film_db`  | `destination_db`       | Full Refresh + Overwrite       | Manual |
| `ticket_schedule`| `ticket_db`| `destination_db`       | Full Refresh + Overwrite       | Manual |
| `film_log`       | `film_db`  | `destination_db`       | Incremental (CDC) + Append     | Manual |
| `ticket_log`     | `ticket_db`| `destination_db`       | Incremental (CDC) + Append     | Manual |

- optional fields / Debezium heartbeat query: 
```bash
INSERT INTO airbyte_heartbeat (text) VALUES ('heartbeat')
```

- Ban đầu để sync mode của connection _log là Full Refresh + Overwrite, sau lần sync đầu tiên thì đổi lại Incremental (CDC) + Append.

## Kích hoạt DAG trong Airflow

Vào Airflow UI tại: http://localhost:8080
- Mở tab DAGs
- Unpause 2 DAGs

| DAG ID                   | Mô tả                           |
|--------------------------|----------------------------------|
| `dag_hourly_refresh`     | Trigger Full Refresh mỗi giờ     |
| `dag_minutely_incremental` | Trigger CDC mỗi phút             |

## Dừng hệ thống

```bash
bash ./stop.sh
```