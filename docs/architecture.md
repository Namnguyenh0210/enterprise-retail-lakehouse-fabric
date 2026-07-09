# Architecture — Enterprise Lakehouse Pipeline

## Overview

Dự án áp dụng kiến trúc **Medallion (Bronze → Silver → Gold)** trên **Microsoft Fabric** với **Delta Lake** làm storage format, xử lý dữ liệu khách hàng từ hệ thống thương mại điện tử **Olist (Brazil)**.

---

## Kiến trúc tổng quan

```
[Source: CSV File]
        │
        ▼
┌───────────────────────────────────────────────┐
│  🥉 BRONZE LAYER  (Raw Ingestion)             │
│  • Đọc CSV thô, không transform               │
│  • Gắn audit metadata (_batch_id, _run_id...) │
│  • Idempotent append vào Delta table          │
│  Table: bronze_customers                      │
└───────────────────────┬───────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────┐
│  🥈 SILVER LAYER  (Cleansing & DQ)            │
│  • Chuẩn hóa: UPPER/TRIM, thêm region        │
│  • CDC hash (MD5) để phát hiện thay đổi       │
│  • Data Quality checks → Quarantine           │
│  • Deduplication (giữ bản ghi mới nhất)       │
│  • UPSERT via MERGE (hash-based CDC)          │
│  Table: silver_customers                      │
│  Table: quarantine_customers                  │
└───────────────────────┬───────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────┐
│  🥇 GOLD LAYER  (Dimension — SCD Type 2)      │
│  • SurrogateKey (SHA-256)                     │
│  • SCD Type 2: đóng bản ghi cũ + chèn mới    │
│  • Version / IsCurrent / EffectiveDates       │
│  • Z-ORDER tối ưu cho BI Dashboard            │
│  Table: gold_dim_customer                     │
└───────────────────────┬───────────────────────┘
                        │
                        ▼
              [Power BI Dashboard]
```

---

## Stack công nghệ

| Component        | Technology                        |
|------------------|-----------------------------------|
| Platform         | Microsoft Fabric (Lakehouse)      |
| Compute          | Apache Spark (Synapse PySpark)    |
| Storage Format   | Delta Lake (Parquet + Transaction Log) |
| Orchestration    | Fabric Data Pipeline              |
| Source Data      | Olist E-Commerce CSV (Brazil)     |
| BI Layer         | Power BI                          |

---

## Các table Delta

| Table                   | Layer      | Mô tả                                  |
|-------------------------|------------|----------------------------------------|
| `bronze_customers`      | Bronze     | Dữ liệu thô + audit metadata           |
| `silver_customers`      | Silver     | Dữ liệu sạch, chuẩn hóa, không trùng  |
| `quarantine_customers`  | Silver     | Bản ghi lỗi DQ để kỹ sư kiểm tra      |
| `gold_dim_customer`     | Gold       | Dimension table với SCD Type 2         |

---

## Luồng xử lý chi tiết

### Bronze — Raw Ingestion
1. **Fail-Fast Validation**: Kiểm tra file không rỗng, đúng delimiter, đủ cột header
2. **Idempotency**: Xóa bản ghi cũ của cùng file trước khi append (an toàn khi retry)
3. **Audit Columns**: `_pipeline_run_id`, `_batch_id`, `_source_file_name`, `_created_at`, `_ingestion_date`

### Silver — Cleansing & DQ
1. **Standardization**: `customer_city`, `customer_state` → UPPER + TRIM; thêm `customer_region`
2. **CDC Hash**: `md5(zip || city || state)` → `_record_hash` (chỉ update khi nội dung thực sự thay đổi)
3. **DQ Rules**: `customer_id NOT NULL`, `customer_unique_id NOT NULL`, `len(state) = 2`, `zip ~ ^[0-9]+$`
4. **Quarantine**: Bản ghi không qua DQ → lưu vào `quarantine_customers` với lý do lỗi
5. **Deduplication**: `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _created_at DESC) = 1`
6. **UPSERT**: MERGE vào Silver — chỉ update khi `_record_hash` thay đổi

### Gold — SCD Type 2
1. **SurrogateKey**: SHA-256 của `customer_id`
2. **Initial Load**: Version=1, IsCurrent=True, EffectiveEndDate=NULL
3. **Incremental**: Staged MERGE pattern
   - Nếu hash thay đổi → đóng bản ghi cũ (`IsCurrent=False, EffectiveEndDate=now`) + chèn mới
   - Nếu hash không đổi → no-op (không tốn write)
4. **Optimization**: `ZORDER BY (customer_region, customer_state)` để tối ưu BI queries

---

## Tính năng kỹ thuật nổi bật

- **Idempotency**: Pipeline có thể chạy lại nhiều lần mà không bị nhân bản dữ liệu
- **CDC (Change Data Capture)**: Chỉ propagate thay đổi thực sự qua các layer
- **Full History**: Gold layer duy trì toàn bộ lịch sử thay đổi qua SCD Type 2
- **Data Lineage**: Mỗi dòng dữ liệu có đủ thông tin trace về nguồn gốc
- **DQ Transparency**: Bản ghi lỗi không bị xóa âm thầm, được lưu vào Quarantine
