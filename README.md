# Enterprise Lakehouse Pipeline — Olist Customer 360

> **Medallion Architecture (Bronze → Silver → Gold) on Microsoft Fabric**  
> Xây dựng bởi: Nguyen Nam | Platform: Microsoft Fabric + Delta Lake + PySpark

---

## Tổng quan

Dự án triển khai kiến trúc **Medallion Data Lakehouse** hoàn chỉnh trên **Microsoft Fabric**, xử lý dữ liệu khách hàng từ **Olist** — nền tảng thương mại điện tử lớn nhất Brazil (~99,441 khách hàng).

Mục tiêu: Chuyển đổi dữ liệu CSV thô thành Dimension Table chuẩn Data Warehouse (SCD Type 2), sẵn sàng cho Power BI Dashboard phân tích Customer 360.

---

## Kiến trúc hệ thống

```
olist_customers_dataset.csv
           │
           ▼
  ┌─────────────────┐
  │  🥉  BRONZE     │  Raw ingestion + audit metadata (idempotent append)
  │ bronze_customers│
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐     ┌───────────────────┐
  │  🥈  SILVER     │────►│  🚧 QUARANTINE     │  DQ-failed records
  │silver_customers │     │quarantine_customers│
  └────────┬────────┘     └───────────────────┘
           │
           ▼
  ┌─────────────────┐
  │  🥇  GOLD       │  SCD Type 2 Dimension (full history)
  │gold_dim_customer│
  └────────┬────────┘
           │
           ▼
     Power BI Dashboard
```

---

## Cấu trúc dự án

```
Enterprise-Lakehouse-Pipeline/
├── notebooks/                          # Fabric notebooks — upload & chạy trực tiếp trên Fabric
│   ├── NB_01_Bronze_Ingest_Customers.py   # Layer 1: Raw ingestion
│   ├── NB_11_Silver_Clean_Customers.py    # Layer 2: Cleansing & DQ
│   └── NB_21_Gold_Dim_Customer.py         # Layer 3: SCD Type 2
│
├── src/                                # Business logic thuần (Python class, không phụ thuộc Fabric)
│   ├── bronze_ingestion.py                # BronzeIngestionProcessor
│   ├── silver_cleansing.py                # SilverCleansingProcessor
│   └── gold_dimension.py                  # GoldDimensionProcessor
│
├── pipelines/                          # Fabric Pipeline definitions
│   └── PL_INGEST_CUSTOMERS.json           # Orchestration: Bronze → Silver → Gold
│
├── sql/                                # SQL validation & analysis queries
│   └── 01_validate_bronze.sql             # Post-ingestion data quality checks
│
├── data/
│   └── raw/
│       └── olist_customers_dataset.csv    # Source data (~99k rows)
│
├── docs/                               # Project documentation
│   ├── architecture.md                    # System architecture detail
│   ├── data_dictionary.md                 # Column definitions & schema
│   └── business_requirement.md            # Business rules & requirements
│
├── requirements.txt                    # Python dependencies
├── .gitignore
└── README.md
```

---

## `notebooks/` vs `src/` — Phân biệt vai trò

Dự án tổ chức code theo **2 tầng tách biệt** để đảm bảo tính rõ ràng và tái sử dụng:

### 📓 `notebooks/` — Fabric-ready Execution Layer
Mỗi notebook là một file chạy được trực tiếp trên **Microsoft Fabric** (Synapse PySpark kernel). Nội dung mỗi notebook bao gồm:
- Cấu hình đường dẫn Lakehouse (ABFS)
- Toàn bộ class business logic (self-contained)
- Cell thực thi `.process()`
- Cell kiểm tra kết quả (sanity check)

> Upload file `.py` lên Fabric workspace → bấm **Run All** là pipeline hoạt động.

### 🐍 `src/` — Pure Business Logic Layer
Chứa **Python class thuần**, tách hoàn toàn khỏi Fabric dependencies. Mục đích:

| Mục đích | Mô tả |
|----------|-------|
| **Code review** | Đọc logic nghiệp vụ sạch, không bị lẫn boilerplate Fabric |
| **Unit testing** | Có thể test local với PySpark standalone mà không cần Fabric |
| **Tái sử dụng** | Dễ đóng gói thành Python wheel để dùng ở dự án khác |
| **Portfolio/CV** | Thể hiện khả năng thiết kế OOP clean, separation of concerns |

> ⚠️ **Lưu ý:** Trong Fabric, notebook **không import trực tiếp** từ `src/`. Hai tầng này có cùng business logic và được sync thủ công khi có thay đổi.

---

## Tính năng kỹ thuật

| Feature | Mô tả |
|---------|-------|
| **Idempotency** | Chạy lại pipeline nhiều lần, dữ liệu không bị duplicate |
| **CDC (Change Data Capture)** | MD5 hash phát hiện thay đổi, chỉ update dòng thực sự thay đổi |
| **Data Quality** | Bản ghi lỗi → Quarantine (không xóa âm thầm) |
| **SCD Type 2** | Toàn bộ lịch sử thay đổi khách hàng được lưu trữ |
| **Z-ORDER** | Tối ưu vật lý Delta files cho BI query performance |
| **Fail-Fast** | Validate file trước khi xử lý để phát hiện lỗi sớm |
| **Audit Trail** | Mỗi dòng có đủ `_run_id`, `_batch_id`, `_source_file_name` |

---

## Hướng dẫn sử dụng

### 1. Chuẩn bị Fabric Workspace

1. Tạo Lakehouse tên `Olist_Lakehouse` trên Fabric
2. Upload `data/raw/olist_customers_dataset.csv` vào `Files/raw/customers/` trong Lakehouse
3. Import 3 notebooks từ thư mục `notebooks/` vào Workspace

### 2. Cập nhật đường dẫn

Trong mỗi notebook, thay thế 2 giá trị sau:

```python
LAKEHOUSE_BASE = "abfss://<WORKSPACE_ID>@onelake.dfs.fabric.microsoft.com/<LAKEHOUSE_ID>"
```

Lấy `WORKSPACE_ID` và `LAKEHOUSE_ID` từ **Fabric Portal → Lakehouse → Settings**.

### 3. Chạy pipeline

**Cách 1 — Chạy thủ công từng notebook:**
```
NB_01_Bronze_Ingest_Customers → NB_11_Silver_Clean_Customers → NB_21_Gold_Dim_Customer
```

**Cách 2 — Import Pipeline:**
```
Import pipelines/PL_INGEST_CUSTOMERS.json vào Fabric → Run all
```

### 4. Kiểm tra kết quả

Chạy các câu SQL trong `sql/01_validate_bronze.sql` trên Lakehouse SQL endpoint.

---

## Data Quality Rules

| Rule | Cột | Điều kiện hợp lệ | Vi phạm → |
|------|-----|-----------------|------------|
| DQ-01 | `customer_id` | NOT NULL | Quarantine |
| DQ-02 | `customer_unique_id` | NOT NULL | Quarantine |
| DQ-03 | `customer_state` | LENGTH = 2 | Quarantine |
| DQ-04 | `customer_zip_code_prefix` | Chỉ chứa số `[0-9]+` | Quarantine |

---

## Nguồn dữ liệu

- **Dataset:** [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **File sử dụng:** `olist_customers_dataset.csv`
- **Số dòng:** ~99,441 khách hàng
- **Phạm vi địa lý:** 27 bang của Brazil

---

## Tech Stack

- **Platform:** Microsoft Fabric (Lakehouse)
- **Compute:** Apache Spark — Synapse PySpark
- **Storage:** Delta Lake (Parquet + Transaction Log)
- **Orchestration:** Fabric Data Pipeline
- **BI:** Power BI
- **Language:** Python 3.x

---

## Tác giả

**Nguyen Nam** — Data Engineer  
Microsoft Fabric | Delta Lake | Medallion Architecture
