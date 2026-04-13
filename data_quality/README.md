# Layer 2 — Data Quality Pipeline

## What this solves
The marketing team was losing trust in dashboards due to duplicate orders, future-dated
records, missing store names, and inconsistent product naming. This pipeline catches all
issues automatically every day before bad data reaches any dashboard.

---

## Architecture

```
BigQuery raw.orders  (today's new rows only)
      ↓
Stored Procedure: gold.run_gold_pipeline()
      ↓
┌─────────────────────────────────────────┐
│  VALIDATE — 8 DQ checks per row         │
│  CLEAN    — normalise names, cast types │
│  ROUTE    — clean vs rejected           │
└─────────────────────────────────────────┘
      ↓                    ↓
gold.orders          gold.orders_quarantine
(clean, typed)       (rejected + failure reason)
      ↓
gold.dq_run_log
(daily PASS/FAIL per check)
```

---

## Tables Created

### gold.orders
Dashboard-ready clean data. Fully typed, normalised store/product names, partitioned by
`order_date`, clustered by `store_name` and `category` for fast analytical queries.

### gold.orders_quarantine
Every rejected row is stored here with a `_failure_reason` column explaining exactly
which check(s) it failed. Nothing is ever deleted — full audit trail.

### gold.dq_run_log
One row per DQ check per day. Teams can query this table to see data health trends over time.

```sql
SELECT run_date, check_name, status, failing_rows, pct_failed
FROM `PROJECT_ID.gold.dq_run_log`
ORDER BY run_date DESC, check_name;
```

---

## The 8 Data Quality Checks

| # | Check | Action |
|---|---|---|
| 1 | Missing `order_id` | Quarantine |
| 2 | Duplicate `order_id` in same batch | Quarantine |
| 3 | `order_date` unparseable | Quarantine |
| 4 | `order_date` in the future | Quarantine |
| 5 | `ship_date` before `order_date` | Quarantine |
| 6 | Revenue = 0 with quantity > 0 | Quarantine |
| 7 | Missing `store_name` | Quarantine |
| 8 | Missing `order_date` string | Quarantine |

## What gets cleaned automatically (not quarantined)

| Issue | Fix applied |
|---|---|
| `"patio chair"`, `"PATIO CHAIR"` | `INITCAP()` → `"Patio Chair"` |
| `"Pacific Coast"` vs `"Pacific Coast Store"` | Canonical lookup via `CASE WHEN` |
| `"garden grove store"` → `"Garden Grove Store"` | Canonical lookup |
| Empty string → NULL | `NULLIF(TRIM(col), '')` |
| Negative revenue | `ABS(revenue)` |
| Missing discount | Defaults to `0.0` |

---

## Incremental Logic

The stored procedure only processes **today's new rows**:

```sql
WHERE DATE(_loaded_at) = CURRENT_DATE()
```

It uses `order_id` as the primary key. Since the source system guarantees unique order IDs,
a `MERGE` is not needed — clean rows are inserted, duplicates within the same batch are
caught by the `ROW_NUMBER()` dedup window and quarantined.

---

## Setup Steps

### Step 1 — Create gold dataset
```bash
bq --location=US mk --dataset PROJECT_ID:gold
```

### Step 2 — Run gold_layer.sql in BigQuery Console
Open BigQuery Console → SQL Editor → paste contents of `gold_layer.sql` → Run each
section in order (Sections 2, 3, 4, 5, 6).

### Step 3 — Set up Scheduled Query (runs pipeline daily at 7AM)
1. BigQuery Console → Scheduled Queries → Create Scheduled Query
2. Query:
```sql
CALL `PROJECT_ID.gold.run_gold_pipeline`();
```
3. Schedule: Every day at 07:00 UTC
4. Save

### Step 4 — Verify results
```sql
-- Row split between gold and quarantine
SELECT 'gold.orders' AS dest, COUNT(*) AS rows FROM `PROJECT_ID.gold.orders`
UNION ALL
SELECT 'quarantine', COUNT(*) FROM `PROJECT_ID.gold.orders_quarantine`;

-- DQ check results
SELECT * FROM `PROJECT_ID.gold.dq_run_log` ORDER BY run_date DESC;

-- Quarantine breakdown by failure reason
SELECT _failure_reason, COUNT(*) AS rejected_rows
FROM `PROJECT_ID.gold.orders_quarantine`
GROUP BY 1 ORDER BY 2 DESC;
```

---

## Production & Scaling Notes
See `docs/production_scaling.md` → Section: Data Quality Layer
