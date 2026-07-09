# Data Dictionary — Olist Customer Lakehouse Pipeline

## Nguồn dữ liệu gốc

**File:** `olist_customers_dataset.csv`  
**Nguồn:** [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)  
**Số dòng:** ~99,441  
**Encoding:** UTF-8

---

## 1. Bronze Layer — `bronze_customers`

### Business Columns (raw, không transform)

| Column                    | Type   | Mô tả                                              | Ví dụ                              |
|---------------------------|--------|----------------------------------------------------|------------------------------------|
| `customer_id`             | string | ID giao dịch của khách (unique per order)          | `06b8999e2fba1a1fbc88172c00ba8bc7` |
| `customer_unique_id`      | string | ID thực sự của khách hàng (1 người nhiều đơn)      | `861eff4711a542e4b93843c6dd7febb0` |
| `customer_zip_code_prefix`| string | 5 chữ số đầu ZIP code (Brazil)                     | `14409`                            |
| `customer_city`           | string | Tên thành phố (lowercase, raw)                     | `franca`                           |
| `customer_state`          | string | Mã bang 2 ký tự (Brazil)                           | `SP`                               |

### Audit / Metadata Columns

| Column                | Type      | Mô tả                                              |
|-----------------------|-----------|----------------------------------------------------|
| `_pipeline_run_id`    | string    | UUID của lần chạy pipeline (unique per execution)  |
| `_batch_id`           | string    | Timestamp batch `YYYYMMDDHHmmss`                   |
| `_source_system`      | string    | Hệ thống nguồn: `olist_ecommerce`                  |
| `_load_type`          | string    | Kiểu nạp: `INCREMENTAL`                            |
| `_created_at`         | timestamp | Thời điểm dòng được ghi vào Bronze                |
| `_ingestion_date`     | date      | Ngày nạp dữ liệu (partition-friendly)              |
| `_source_file_path`   | string    | ABFS path đầy đủ của file CSV nguồn                |
| `_source_file_name`   | string    | Tên file (e.g. `olist_customers_dataset.csv`)      |

---

## 2. Silver Layer — `silver_customers`

Kế thừa tất cả cột từ Bronze, thêm các cột sau:

### Transformed Business Columns

| Column                    | Type   | Transform                           | Ví dụ          |
|---------------------------|--------|-------------------------------------|----------------|
| `customer_city`           | string | UPPER(TRIM(...))                    | `FRANCA`       |
| `customer_state`          | string | UPPER(TRIM(...))                    | `SP`           |
| `customer_zip_code_prefix`| string | TRIM(...)                           | `14409`        |
| `customer_region`         | string | Phân vùng địa lý theo `customer_state` | `Sudeste`   |

### Mapping `customer_region`

| Region          | Các bang (customer_state)                          |
|-----------------|----------------------------------------------------|
| `Norte`         | AC, AP, AM, PA, RO, RR, TO                         |
| `Nordeste`      | AL, BA, CE, MA, PB, PE, PI, RN, SE                 |
| `Centro-Oeste`  | DF, GO, MT, MS                                     |
| `Sudeste`       | ES, MG, RJ, SP                                     |
| `Sul`           | PR, RS, SC                                         |
| `Unknown`       | Mã bang không hợp lệ hoặc ngoài danh sách          |

### Silver Metadata Columns

| Column              | Type      | Mô tả                                                                    |
|---------------------|-----------|--------------------------------------------------------------------------|
| `_record_hash`      | string    | `MD5(customer_zip_code_prefix \|\| customer_city \|\| customer_state)`   |
| `_silver_update_ts` | timestamp | Thời điểm dòng được UPSERT vào Silver                                    |

---

## 3. Silver Layer — `quarantine_customers`

Bản ghi không vượt qua Data Quality checks.

| Column                | Mô tả                                                      |
|-----------------------|------------------------------------------------------------|
| (all Bronze columns)  | Dữ liệu gốc giữ nguyên                                     |
| `_dq_error_reason`    | Lý do lỗi: `"Lỗi DQ: ID rỗng, Mã bang != 2 ký tự, hoặc ZIP không hợp lệ"` |
| `_quarantine_ts`      | Thời điểm bị quarantine                                    |
| `_quarantine_run_id`  | UUID của lần chạy đã quarantine bản ghi này                |

### DQ Rules

| Rule                              | Điều kiện hợp lệ                      |
|-----------------------------------|---------------------------------------|
| customer_id không NULL            | `customer_id IS NOT NULL`             |
| customer_unique_id không NULL     | `customer_unique_id IS NOT NULL`      |
| Mã bang đúng 2 ký tự             | `LENGTH(customer_state) = 2`          |
| ZIP code chỉ chứa số             | `customer_zip_code_prefix ~ ^[0-9]+$` |

---

## 4. Gold Layer — `gold_dim_customer`

Dimension table cho Data Warehouse / Power BI.

### Dimension Columns

| Column                    | Type      | Mô tả                                              |
|---------------------------|-----------|----------------------------------------------------|
| `SurrogateKey`            | string    | SHA-256 của `BusinessKey_CustomerID` (DW key)      |
| `BusinessKey_CustomerID`  | string    | `customer_id` từ hệ thống nguồn                    |
| `customer_unique_id`      | string    | ID thực của khách hàng                             |
| `customer_zip_code_prefix`| string    | ZIP code (đã trim)                                 |
| `customer_city`           | string    | Thành phố (đã UPPER)                               |
| `customer_state`          | string    | Bang (đã UPPER, 2 ký tự)                           |
| `customer_region`         | string    | Khu vực địa lý                                     |

### SCD Type 2 Columns

| Column               | Type      | Mô tả                                                         |
|----------------------|-----------|---------------------------------------------------------------|
| `Version`            | integer   | Số phiên bản, bắt đầu từ 1, tăng dần mỗi lần thay đổi       |
| `IsCurrent`          | boolean   | `True` = bản ghi đang hiệu lực, `False` = đã hết hạn         |
| `EffectiveStartDate` | timestamp | Thời điểm bản ghi bắt đầu có hiệu lực                        |
| `EffectiveEndDate`   | timestamp | Thời điểm bản ghi hết hiệu lực (`NULL` nếu đang hiệu lực)    |
| `_record_hash`       | string    | MD5 hash từ Silver — dùng để phát hiện thay đổi              |
| `_audit_insert_batch`| string    | batch_id của lần chèn bản ghi này                            |
| `_audit_update_batch`| string    | batch_id của lần đóng bản ghi này (NULL nếu chưa đóng)       |
