"""
Cloud Function: gcs_to_bigquery_raw
Trigger : Cloud Storage (OBJECT_FINALIZE) on your raw-csv bucket
Runtime : Python 3.11
Purpose : Whenever a CSV lands in GCS, load it into BigQuery raw.orders
          as-is (all strings) with audit columns appended.
"""

import functions_framework
import logging
import hashlib
import json
from datetime import datetime, timezone

from google.cloud import bigquery, storage

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── Config (override via environment variables in GCP) ────────────────────────
PROJECT_ID  = "your-project-id"          # ← replace
DATASET_ID  = "raw_dataset"
TABLE_ID    = "orders"
LOCATION    = "US"

# BigQuery schema — all source columns as STRING so no load ever fails
# Audit columns (_loaded_at, _source_file, _row_hash) are appended by the function
RAW_SCHEMA = [
    bigquery.SchemaField("order_id",        "STRING"),
    bigquery.SchemaField("order_date",      "STRING"),
    bigquery.SchemaField("ship_date",       "STRING"),
    bigquery.SchemaField("customer_name",   "STRING"),
    bigquery.SchemaField("customer_email",  "STRING"),
    bigquery.SchemaField("customer_state",  "STRING"),
    bigquery.SchemaField("product_name",    "STRING"),
    bigquery.SchemaField("category",        "STRING"),
    bigquery.SchemaField("quantity",        "STRING"),
    bigquery.SchemaField("unit_price",      "STRING"),
    bigquery.SchemaField("discount_pct",    "STRING"),
    bigquery.SchemaField("revenue",         "STRING"),
    bigquery.SchemaField("store_name",      "STRING"),
    bigquery.SchemaField("shipping_method", "STRING"),
    bigquery.SchemaField("payment_method",  "STRING"),
    bigquery.SchemaField("order_status",    "STRING"),
    # ── Audit columns ─────────────────────────────────────────────────────
    bigquery.SchemaField("_loaded_at",      "TIMESTAMP"),
    bigquery.SchemaField("_source_file",    "STRING"),
    bigquery.SchemaField("_row_hash",       "STRING"),
]


@functions_framework.cloud_event
def gcs_to_bigquery_raw(cloud_event):
    """
    Entry point — triggered by Cloud Storage OBJECT_FINALIZE event.
    cloud_event.data contains: bucket, name, contentType, size, etc.
    """
    data        = cloud_event.data
    bucket_name = data["bucket"]
    file_name   = data["name"]
    content_type = data.get("contentType", "")

    log.info(f"Event received | bucket={bucket_name} | file={file_name} | type={content_type}")

    # ── Guard: only process CSV files ─────────────────────────────────────
    if not file_name.endswith(".csv"):
        log.info("Not a CSV — skipping.")
        return

    # ── Read CSV from GCS ──────────────────────────────────────────────────
    gcs_client = storage.Client()
    bucket     = gcs_client.bucket(bucket_name)
    blob       = bucket.blob(file_name)
    raw_bytes  = blob.download_as_bytes()
    csv_text   = raw_bytes.decode("utf-8-sig")  # strip BOM if present

    lines  = csv_text.strip().splitlines()
    header = [h.strip().lower() for h in lines[0].split(",")]

    log.info(f"CSV loaded | rows={len(lines)-1} | columns={header}")

    # ── Parse rows + append audit fields ──────────────────────────────────
    loaded_at   = datetime.now(timezone.utc).isoformat()
    source_file = f"gs://{bucket_name}/{file_name}"
    rows_to_insert = []

    for line in lines[1:]:
        if not line.strip():
            continue  # skip blank lines

        values = _split_csv_line(line)

        # Pad or truncate to match header length (handles trailing commas / missing fields)
        while len(values) < len(header):
            values.append(None)
        values = values[: len(header)]

        row = dict(zip(header, values))

        # Replace empty strings with None (BigQuery treats "" as a value, not NULL)
        row = {k: (v if v not in ("", "NA", "N/A", "null", "NULL") else None)
               for k, v in row.items()}

        # ── Compute row hash for dedup detection downstream ────────────────
        hash_source = json.dumps(row, sort_keys=True, default=str)
        row_hash    = hashlib.md5(hash_source.encode()).hexdigest()

        row["_loaded_at"]   = loaded_at
        row["_source_file"] = source_file
        row["_row_hash"]    = row_hash

        rows_to_insert.append(row)

    if not rows_to_insert:
        log.warning("No data rows found in file — nothing to insert.")
        return

    # ── Ensure dataset + table exist ───────────────────────────────────────
    bq_client  = bigquery.Client(project=PROJECT_ID)
    dataset_ref = bq_client.dataset(DATASET_ID)

    _ensure_dataset(bq_client, dataset_ref)
    table_ref = dataset_ref.table(TABLE_ID)
    _ensure_table(bq_client, table_ref)

    # ── Stream insert into BigQuery ────────────────────────────────────────
    # Using insert_rows_json so we don't need a temp GCS file.
    # For production with >100K rows, swap to a load job from GCS instead.
    BATCH_SIZE = 500
    total_errors = []

    for start in range(0, len(rows_to_insert), BATCH_SIZE):
        batch  = rows_to_insert[start : start + BATCH_SIZE]
        errors = bq_client.insert_rows_json(table_ref, batch)
        if errors:
            log.error(f"BQ insert errors (batch starting {start}): {errors}")
            total_errors.extend(errors)

    if total_errors:
        raise RuntimeError(f"BigQuery insert had {len(total_errors)} errors. Check logs.")

    log.info(
        f"✅ Done | inserted={len(rows_to_insert)} rows "
        f"into {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    )


# ── Helpers ────────────────────────────────────────────────────────────────────

def _split_csv_line(line: str) -> list:
    """
    Minimal CSV parser that handles quoted fields with commas inside.
    Python's csv module is safer for production, but this is clear for a demo.
    """
    import csv, io
    reader = csv.reader(io.StringIO(line))
    for row in reader:
        return row
    return []


def _ensure_dataset(client: bigquery.Client, dataset_ref):
    """Create dataset if it does not exist."""
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        client.create_dataset(dataset, exists_ok=True)
        log.info(f"Created dataset: {dataset_ref.dataset_id}")


def _ensure_table(client: bigquery.Client, table_ref):
    """Create table with RAW_SCHEMA if it does not exist."""
    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=RAW_SCHEMA)
        # Partition by _loaded_at so queries on 'today's load' are cheap
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="_loaded_at",
        )
        table.description = "Raw orders — exact copy of source CSV with audit columns"
        client.create_table(table, exists_ok=True)
        log.info(f"Created table: {table_ref.table_id}")
