-- =============================================================================
-- File        : 01_validate_bronze.sql
-- Layer       : Bronze Validation
-- Description : Kiểm tra chất lượng dữ liệu sau khi nạp vào Bronze layer.
--               Chạy sau NB_01_Bronze_Ingest_Customers để đảm bảo dữ liệu đúng.
-- Target      : bronze_customers (Delta Table)
-- =============================================================================

-- 1. Tổng số dòng đã nạp
SELECT
    COUNT(*)            AS total_rows,
    COUNT(DISTINCT _batch_id) AS total_batches,
    MIN(_ingestion_date) AS first_ingestion,
    MAX(_ingestion_date) AS latest_ingestion
FROM bronze_customers;

-- =============================================================================

-- 2. Kiểm tra NULL trên các cột business-critical
SELECT
    COUNT_IF(customer_id IS NULL)              AS null_customer_id,
    COUNT_IF(customer_unique_id IS NULL)       AS null_unique_id,
    COUNT_IF(customer_zip_code_prefix IS NULL) AS null_zip,
    COUNT_IF(customer_city IS NULL)            AS null_city,
    COUNT_IF(customer_state IS NULL)           AS null_state
FROM bronze_customers;

-- =============================================================================

-- 3. Kiểm tra trùng lặp customer_id trong cùng một batch
SELECT
    _batch_id,
    customer_id,
    COUNT(*) AS cnt
FROM bronze_customers
GROUP BY _batch_id, customer_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

-- =============================================================================

-- 4. Phân bố theo bang (customer_state) — Top 10
SELECT
    customer_state,
    COUNT(*) AS total_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM bronze_customers
GROUP BY customer_state
ORDER BY total_customers DESC
LIMIT 10;

-- =============================================================================

-- 5. Kiểm tra metadata audit (lineage)
SELECT
    _source_file_name,
    _batch_id,
    _load_type,
    _source_system,
    COUNT(*) AS rows_in_batch
FROM bronze_customers
GROUP BY _source_file_name, _batch_id, _load_type, _source_system
ORDER BY _batch_id DESC;

-- =============================================================================

-- 6. Kiểm tra ZIP code format (phải là số)
SELECT
    customer_zip_code_prefix,
    COUNT(*) AS cnt
FROM bronze_customers
WHERE customer_zip_code_prefix NOT RLIKE '^[0-9]+$'
ORDER BY cnt DESC
LIMIT 20;

-- =============================================================================

-- 7. Sample 10 dòng mới nhất
SELECT *
FROM bronze_customers
ORDER BY _created_at DESC
LIMIT 10;
