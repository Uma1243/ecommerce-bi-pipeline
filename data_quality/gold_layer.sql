-- =============================================================================
-- GOLD LAYER — Full Data Quality Pipeline
-- Project : project-80fa4072-8e79-4492-a86
-- Run order: Execute each section top to bottom, once.
--            After that, only the STORED PROCEDURE runs daily via Scheduled Query.
-- =============================================================================


-- =============================================================================
-- SECTION 1: CREATE GOLD DATASET
-- Run once in Cloud Shell:
--   bq --location=US mk --dataset project-80fa4072-8e79-4492-a86:gold
-- =============================================================================


-- =============================================================================
-- SECTION 2: CREATE gold.orders  (clean, trusted data — what dashboards read)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `project-80fa4072-8e79-4492-a86.gold.orders`
(
  -- Business columns (typed + cleaned)
  order_id         STRING       NOT NULL,
  order_date       DATE,
  ship_date        DATE,
  customer_name    STRING,
  customer_email   STRING,
  customer_state   STRING,
  product_name     STRING,       -- normalised
  category         STRING,
  quantity         INT64,
  unit_price       FLOAT64,
  discount_pct     FLOAT64,
  revenue          FLOAT64,
  store_name       STRING,       -- normalised
  shipping_method  STRING,
  payment_method   STRING,
  order_status     STRING,

  -- Audit columns (carried from raw)
  _source_file     STRING,
  _row_hash        STRING,
  _loaded_at       TIMESTAMP,
  _gold_created_at TIMESTAMP     -- when this record entered the gold layer
)
PARTITION BY order_date
CLUSTER BY store_name, category
OPTIONS (
  description = 'Cleaned, validated orders — single source of truth for dashboards'
);


-- =============================================================================
-- SECTION 3: CREATE gold.orders_quarantine  (rejected rows + reason)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `project-80fa4072-8e79-4492-a86.gold.orders_quarantine`
(
  -- Raw columns kept as-is so nothing is lost
  order_id         STRING,
  order_date       STRING,
  ship_date        STRING,
  customer_name    STRING,
  customer_email   STRING,
  customer_state   STRING,
  product_name     STRING,
  category         STRING,
  quantity         STRING,
  unit_price       STRING,
  discount_pct     STRING,
  revenue          STRING,
  store_name       STRING,
  shipping_method  STRING,
  payment_method   STRING,
  order_status     STRING,

  -- Audit
  _source_file     STRING,
  _row_hash        STRING,
  _loaded_at       TIMESTAMP,

  -- Why was this row rejected?
  _failure_reason  STRING,
  _quarantine_at   TIMESTAMP
)
PARTITION BY DATE(_quarantine_at)
OPTIONS (
  description = 'Rows rejected from gold layer — review daily for data issues'
);


-- =============================================================================
-- SECTION 4: CREATE gold.dq_run_log  (daily test results — your audit trail)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `project-80fa4072-8e79-4492-a86.gold.dq_run_log`
(
  run_date         DATE,
  check_name       STRING,
  status           STRING,   -- 'PASS' or 'FAIL'
  total_rows       INT64,
  failing_rows     INT64,
  pct_failed       FLOAT64,
  checked_at       TIMESTAMP
)
OPTIONS (
  description = 'Daily data quality check results — one row per check per run'
);


-- =============================================================================
-- SECTION 5: STORED PROCEDURE — Incremental load (only today's new rows)
-- =============================================================================
CREATE OR REPLACE PROCEDURE `project-80fa4072-8e79-4492-a86.gold.run_gold_pipeline`()
BEGIN

  -- ── STEP A: Stage only rows loaded today that aren't already in gold ────────
  -- We match on _row_hash so even if the same file is re-uploaded accidentally,
  -- we never insert duplicates into gold.
  CREATE TEMP TABLE staged AS
  WITH

  new_raw AS (
    SELECT *
    FROM `project-80fa4072-8e79-4492-a86.raw.orders` 
    WHERE
      -- Only today's load partition
      DATE(r._loaded_at) = CURRENT_DATE()
  ),

  casted AS (
    SELECT
      *,
      SAFE.PARSE_DATE('%Y-%m-%d', TRIM(order_date))  AS order_date_cast,
      SAFE.PARSE_DATE('%Y-%m-%d', TRIM(ship_date))   AS ship_date_cast,
      SAFE_CAST(TRIM(quantity)     AS INT64)          AS quantity_cast,
      SAFE_CAST(TRIM(unit_price)   AS FLOAT64)        AS unit_price_cast,
      SAFE_CAST(TRIM(discount_pct) AS FLOAT64)        AS discount_pct_cast,
      SAFE_CAST(TRIM(revenue)      AS FLOAT64)        AS revenue_cast
    FROM new_raw
  ),

  deduped AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY _loaded_at ASC
      ) AS _rn
    FROM casted
  ),

  checked AS (
    SELECT
      *,
      TRIM(CONCAT(
        IF(order_id IS NULL OR TRIM(order_id) = '',          'MISSING_ORDER_ID, ',           ''),
        IF(_rn > 1,                                          'DUPLICATE_ORDER_ID, ',         ''),
        IF(order_date_cast IS NULL,                          'INVALID_ORDER_DATE, ',         ''),
        IF(order_date_cast > CURRENT_DATE(),                 'FUTURE_ORDER_DATE, ',          ''),
        IF(ship_date_cast IS NOT NULL
           AND order_date_cast IS NOT NULL
           AND ship_date_cast < order_date_cast,             'SHIP_BEFORE_ORDER_DATE, ',     ''),
        IF(revenue_cast IS NOT NULL
           AND quantity_cast IS NOT NULL
           AND quantity_cast > 0
           AND revenue_cast <= 0,                            'ZERO_REVENUE_WITH_QTY, ',      ''),
        IF(store_name IS NULL OR TRIM(store_name) = '',      'MISSING_STORE_NAME, ',         ''),
        IF(order_date IS NULL OR TRIM(order_date) = '',      'MISSING_ORDER_DATE_STRING, ',  '')
      )) AS failure_reasons
    FROM deduped
  )

  SELECT *, (failure_reasons = '') AS is_valid
  FROM checked;


  -- ── STEP B: Insert new clean rows into gold.orders ──────────────────────────
  INSERT INTO `project-80fa4072-8e79-4492-a86.gold.orders`
  SELECT
    TRIM(order_id)                                        AS order_id,
    order_date_cast                                       AS order_date,
    ship_date_cast                                        AS ship_date,
    TRIM(customer_name)                                   AS customer_name,
    NULLIF(TRIM(customer_email), '')                      AS customer_email,
    TRIM(customer_state)                                  AS customer_state,
    INITCAP(TRIM(product_name))                           AS product_name,
    TRIM(category)                                        AS category,
    quantity_cast                                         AS quantity,
    unit_price_cast                                       AS unit_price,
    COALESCE(discount_pct_cast, 0.0)                      AS discount_pct,
    ABS(revenue_cast)                                     AS revenue,
    CASE
      WHEN LOWER(TRIM(store_name)) IN ('garden grove store','garden grove')
        THEN 'Garden Grove Store'
      WHEN LOWER(TRIM(store_name)) IN ('pacific coast store','pacific coast')
        THEN 'Pacific Coast Store'
      WHEN LOWER(TRIM(store_name)) IN ('lakeside outlet','lakeside')
        THEN 'Lakeside Outlet'
      WHEN LOWER(TRIM(store_name)) IN ('mountain view','mountain view store')
        THEN 'Mountain View Store'
      WHEN LOWER(TRIM(store_name)) IN ('prairie home','prairie home store')
        THEN 'Prairie Home Store'
      ELSE INITCAP(TRIM(store_name))
    END                                                   AS store_name,
    TRIM(shipping_method)                                 AS shipping_method,
    TRIM(payment_method)                                  AS payment_method,
    TRIM(order_status)                                    AS order_status,
    _source_file,
    _row_hash,
    _loaded_at,
    CURRENT_TIMESTAMP()                                   AS _gold_created_at
  FROM staged
  WHERE is_valid = TRUE;


  -- ── STEP C: Insert rejected rows into quarantine ────────────────────────────
  INSERT INTO `project-80fa4072-8e79-4492-a86.gold.orders_quarantine`
  SELECT
    order_id, order_date, ship_date, customer_name, customer_email,
    customer_state, product_name, category, quantity, unit_price,
    discount_pct, revenue, store_name, shipping_method, payment_method,
    order_status, _source_file, _row_hash, _loaded_at,
    REGEXP_REPLACE(failure_reasons, r',\s*$', '') AS _failure_reason,
    CURRENT_TIMESTAMP() AS _quarantine_at
  FROM staged
  WHERE is_valid = FALSE;


  -- ── STEP D: Write today's DQ results to run log ─────────────────────────────
  -- Safe to re-run — deletes today's entries before reinserting
  DELETE FROM `project-80fa4072-8e79-4492-a86.gold.dq_run_log`
  WHERE run_date = CURRENT_DATE();

  INSERT INTO `project-80fa4072-8e79-4492-a86.gold.dq_run_log`
  SELECT CURRENT_DATE(), 'no_duplicate_order_ids',
    IF(COUNT(*) = 0, 'PASS', 'FAIL'),
    (SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders` WHERE DATE(_loaded_at) = CURRENT_DATE()),
    COUNT(*), ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders` WHERE DATE(_loaded_at) = CURRENT_DATE()), 0), 2),
    CURRENT_TIMESTAMP()
  FROM `project-80fa4072-8e79-4492-a86.gold.orders_quarantine`
  WHERE _failure_reason LIKE '%DUPLICATE_ORDER_ID%' AND DATE(_quarantine_at) = CURRENT_DATE()

  UNION ALL
  SELECT CURRENT_DATE(), 'no_future_order_dates',
    IF(COUNT(*) = 0, 'PASS', 'FAIL'),
    (SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders` WHERE DATE(_loaded_at) = CURRENT_DATE()),
    COUNT(*), ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders` WHERE DATE(_loaded_at) = CURRENT_DATE()), 0), 2),
    CURRENT_TIMESTAMP()
  FROM `project-80fa4072-8e79-4492-a86.gold.orders_quarantine`
  WHERE _failure_reason LIKE '%FUTURE_ORDER_DATE%' AND DATE(_quarantine_at) = CURRENT_DATE()

  UNION ALL
  SELECT CURRENT_DATE(), 'ship_date_after_order_date',
    IF(COUNT(*) = 0, 'PASS', 'FAIL'),
    (SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders` WHERE DATE(_loaded_at) = CURRENT_DATE()),
    COUNT(*), ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders` WHERE DATE(_loaded_at) = CURRENT_DATE()), 0), 2),
    CURRENT_TIMESTAMP()
  FROM `project-80fa4072-8e79-4492-a86.gold.orders_quarantine`
  WHERE _failure_reason LIKE '%SHIP_BEFORE_ORDER_DATE%' AND DATE(_quarantine_at) = CURRENT_DATE()

  UNION ALL
  SELECT CURRENT_DATE(), 'no_zero_revenue_with_quantity',
    IF(COUNT(*) = 0, 'PASS', 'FAIL'),
    (SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders` WHERE DATE(_loaded_at) = CURRENT_DATE()),
    COUNT(*), ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders` WHERE DATE(_loaded_at) = CURRENT_DATE()), 0), 2),
    CURRENT_TIMESTAMP()
  FROM `project-80fa4072-8e79-4492-a86.gold.orders_quarantine`
  WHERE _failure_reason LIKE '%ZERO_REVENUE_WITH_QTY%' AND DATE(_quarantine_at) = CURRENT_DATE()

  UNION ALL
  SELECT CURRENT_DATE(), 'store_name_not_missing',
    IF(COUNT(*) = 0, 'PASS', 'FAIL'),
    (SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders` WHERE DATE(_loaded_at) = CURRENT_DATE()),
    COUNT(*), ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders` WHERE DATE(_loaded_at) = CURRENT_DATE()), 0), 2),
    CURRENT_TIMESTAMP()
  FROM `project-80fa4072-8e79-4492-a86.gold.orders_quarantine`
  WHERE _failure_reason LIKE '%MISSING_STORE_NAME%' AND DATE(_quarantine_at) = CURRENT_DATE()

  UNION ALL
  SELECT CURRENT_DATE(), 'gold_vs_raw_row_count_check',
    IF(
      ABS((SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.gold.orders`) -
          (SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders`)) /
      NULLIF((SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders`), 0) < 0.10,
      'PASS', 'FAIL'),
    (SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders`),
    ABS((SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.gold.orders`) -
        (SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders`)),
    ROUND(ABS((SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.gold.orders`) -
              (SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders`)) * 100.0 /
          NULLIF((SELECT COUNT(*) FROM `project-80fa4072-8e79-4492-a86.raw.orders`), 0), 2),
    CURRENT_TIMESTAMP()
  FROM (SELECT 1);


-- =============================================================================
-- SECTION 6: RUN THE PIPELINE NOW (first manual run to test)
-- =============================================================================
CALL `project-80fa4072-8e79-4492-a86.gold.run_gold_pipeline`();


-- =============================================================================
-- SECTION 7: VERIFICATION QUERIES — run after the procedure completes
-- =============================================================================

-- 7a. How many rows made it to gold vs quarantine?
SELECT
  'gold.orders'            AS destination,
  COUNT(*)                 AS row_count
FROM `project-80fa4072-8e79-4492-a86.gold.orders`
UNION ALL
SELECT
  'gold.orders_quarantine',
  COUNT(*)
FROM `project-80fa4072-8e79-4492-a86.gold.orders_quarantine`;


-- 7b. What are the quarantine failure reasons and how many rows each?
SELECT
  _failure_reason,
  COUNT(*) AS rejected_rows
FROM `project-80fa4072-8e79-4492-a86.gold.orders_quarantine`
GROUP BY _failure_reason
ORDER BY rejected_rows DESC;


-- 7c. Daily DQ run log — did all checks pass?
SELECT
  run_date,
  check_name,
  status,
  total_rows,
  failing_rows,
  pct_failed
FROM `project-80fa4072-8e79-4492-a86.gold.dq_run_log`
ORDER BY run_date DESC, check_name;


-- 7d. Confirm store name normalisation worked
SELECT
  store_name,
  COUNT(*) AS orders
FROM `project-80fa4072-8e79-4492-a86.gold.orders`
GROUP BY store_name
ORDER BY orders DESC;


-- 7e. Confirm product name normalisation worked
SELECT
  product_name,
  COUNT(*) AS orders
FROM `project-80fa4072-8e79-4492-a86.gold.orders`
GROUP BY product_name
ORDER BY orders DESC;


-- =============================================================================
-- SECTION 8: SCHEDULED QUERY SETUP
-- Do this once in BigQuery Console UI:
--
-- 1. Open BigQuery Console → your project
-- 2. Click "Scheduled queries" in the left panel
-- 3. Click "+ Create scheduled query"
-- 4. Paste this single line as the query:
--      CALL `project-80fa4072-8e79-4492-a86.gold.run_gold_pipeline`();
-- 5. Set schedule: every day at 07:00 AM UTC
-- 6. Set destination dataset: gold (just for the scheduler to save, not actual output)
-- 7. Save — done. Pipeline runs automatically every morning.
-- =============================================================================
