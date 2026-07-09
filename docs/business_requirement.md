# Business Requirements — Enterprise Lakehouse Pipeline

## 1. Bối cảnh dự án

**Tên dự án:** Enterprise Lakehouse Pipeline — Olist Customer 360  
**Platform:** Microsoft Fabric (Lakehouse + Notebooks + Pipeline)  
**Domain:** E-Commerce / Customer Analytics  
**Nguồn dữ liệu:** Olist Brazilian E-Commerce Public Dataset

Olist là nền tảng thương mại điện tử lớn tại Brazil, kết nối hàng nghìn merchant với khách hàng trên toàn quốc. Dữ liệu khách hàng cần được chuẩn hóa và tổ chức đúng chuẩn để phục vụ phân tích kinh doanh và BI Dashboard.

---

## 2. Mục tiêu kinh doanh

| ID   | Yêu cầu                                                                                   | Mức độ ưu tiên |
|------|-------------------------------------------------------------------------------------------|----------------|
| BR-1 | Tập trung toàn bộ dữ liệu khách hàng vào một Lakehouse duy nhất (Single Source of Truth) | Must Have      |
| BR-2 | Chuẩn hóa địa chỉ khách hàng (city, state, region) để phân tích địa lý                  | Must Have      |
| BR-3 | Duy trì lịch sử thay đổi thông tin khách hàng (SCD Type 2) phục vụ audit & trending      | Must Have      |
| BR-4 | Phát hiện và cách ly dữ liệu lỗi thay vì bỏ qua âm thầm (Data Quality transparency)      | Must Have      |
| BR-5 | Pipeline có thể chạy lại an toàn mà không nhân bản dữ liệu (Idempotency)                 | Must Have      |
| BR-6 | Cung cấp Dimension Table sẵn sàng cho Power BI Dashboard                                 | Must Have      |
| BR-7 | Phân vùng khách hàng theo khu vực địa lý Brazil (Norte, Nordeste, Sul, Sudeste, Centro-Oeste) | Should Have |

---

## 3. Yêu cầu dữ liệu nguồn

### Input Schema

File CSV `olist_customers_dataset.csv` cần có đúng 5 cột:

| Cột                       | Bắt buộc | Mô tả                        |
|---------------------------|----------|------------------------------|
| `customer_id`             | ✅       | ID giao dịch khách hàng      |
| `customer_unique_id`      | ✅       | ID thực của khách hàng       |
| `customer_zip_code_prefix`| ✅       | Mã ZIP 5 số (Brazil)         |
| `customer_city`           | ✅       | Tên thành phố                |
| `customer_state`          | ✅       | Mã bang 2 ký tự              |

### Data Quality Rules

| Rule ID | Mô tả                             | Hành động khi vi phạm    |
|---------|-----------------------------------|--------------------------|
| DQ-01   | `customer_id` không được NULL     | Quarantine               |
| DQ-02   | `customer_unique_id` không NULL   | Quarantine               |
| DQ-03   | `customer_state` đúng 2 ký tự    | Quarantine               |
| DQ-04   | `customer_zip_code_prefix` là số  | Quarantine               |

---

## 4. Yêu cầu đầu ra (Output)

### 4.1 Bronze Table — `bronze_customers`
- Dữ liệu thô nguyên bản, không transform
- Bổ sung đầy đủ audit columns để truy vết nguồn gốc
- Hỗ trợ incremental append idempotent

### 4.2 Silver Table — `silver_customers`
- Dữ liệu đã chuẩn hóa (UPPER/TRIM)
- Cột `customer_region` phân vùng theo 5 khu vực địa lý Brazil
- Không có duplicate theo `customer_id`
- Chỉ chứa bản ghi vượt qua tất cả DQ rules

### 4.3 Quarantine Table — `quarantine_customers`
- Lưu toàn bộ bản ghi lỗi DQ
- Ghi rõ lý do lỗi trong `_dq_error_reason`
- Kỹ sư dữ liệu có thể inspect và xử lý thủ công

### 4.4 Gold Dimension — `gold_dim_customer`
- Chuẩn SCD Type 2 với `Version`, `IsCurrent`, `EffectiveStartDate`, `EffectiveEndDate`
- `SurrogateKey` (SHA-256) thay cho natural key để dùng trong Data Warehouse
- Sẵn sàng join với Fact tables trong Power BI

---

## 5. Yêu cầu phi chức năng

| Yêu cầu          | Mô tả                                                               |
|------------------|---------------------------------------------------------------------|
| **Idempotency**  | Chạy lại pipeline bất kỳ lần nào cũng cho kết quả nhất quán        |
| **Scalability**  | Xử lý tốt khi data tăng lên 10x (>1M dòng)                         |
| **Observability**| Log đầy đủ: run_id, batch_id, row counts, error details             |
| **Performance**  | Z-ORDER tối ưu physical layout cho BI queries filter theo state/region|
| **Traceability** | Mỗi dòng Gold có thể trace về file nguồn, batch, run cụ thể        |

---

## 6. Luồng chạy (Execution Flow)

```
Trigger (Manual / Scheduled)
        │
        ▼
PL_INGEST_CUSTOMERS (Fabric Pipeline)
        │
        ├─[Step 1]─► NB_01_Bronze_Ingest_Customers  (Validate + Ingest)
        │                       │ Success
        ├─[Step 2]─► NB_11_Silver_Clean_Customers   (Cleanse + DQ + UPSERT)
        │                       │ Success
        └─[Step 3]─► NB_21_Gold_Dim_Customer        (SCD Type 2 MERGE)
                                │ Success
                          Power BI refresh
```

---

## 7. Định nghĩa hoàn thành (Definition of Done)

- [x] Bronze table được nạp đúng với đủ audit columns
- [x] Silver table không có duplicate, DQ violations được quarantine
- [x] Gold table có SCD Type 2 đúng: lịch sử thay đổi được duy trì
- [x] Pipeline idempotent: chạy lại nhiều lần không tạo ra duplicate
- [x] SQL validation queries xác nhận chất lượng dữ liệu sau mỗi layer
- [x] README và docs đầy đủ để onboard thành viên mới
