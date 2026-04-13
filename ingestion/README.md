# Layer 1 — Automated Ingestion Pipeline

## What this solves
Previously someone manually downloaded a CSV every morning and uploaded it to BigQuery.
This pipeline automates the entire process — a CSV dropped into GCS triggers a Cloud Function
that loads the data into BigQuery within seconds, with no human involvement.

---

## Architecture

```
orders_raw.csv
      ↓  (manual upload or future: Gmail API)
GCS Bucket  gs://ecommerce-raw-orders-hassan/
      ↓  (OBJECT_FINALIZE event via Eventarc)
Cloud Function  gcs-to-bigquery-raw
      ↓
BigQuery  raw.orders
```

---

## BigQuery Schema Design

All source columns are loaded as **STRING** intentionally.

> Why strings in raw? Because raw is your audit layer. A bad date like "2025-99-99"
> should never break a load job. Type casting happens in the transformation layer (gold),
> not on ingest. This guarantees 100% of rows always land in raw.

Three audit columns are appended by the Cloud Function to every row:

| Column | Type | Purpose |
|---|---|---|
| `_loaded_at` | TIMESTAMP | When this row was loaded — used for incremental processing |
| `_source_file` | STRING | Full GCS path of the source file — full lineage |
| `_row_hash` | MD5 STRING | Hash of all field values — detect accidental re-uploads |

Table is **partitioned by DATE(_loaded_at)** so daily incremental queries only
scan today's partition, not 3 years of history.

---

## Setup Steps

### Step 1 — Enable APIs
```bash
gcloud services enable cloudfunctions.googleapis.com cloudbuild.googleapis.com \
  bigquery.googleapis.com storage.googleapis.com run.googleapis.com eventarc.googleapis.com
```

### Step 2 — Create GCS bucket
```bash
gsutil mb -l US gs://ecommerce-raw-orders-hassan
```

### Step 3 — Create BigQuery dataset
```bash
bq --location=US mk --dataset PROJECT_ID:raw
```

### Step 4 — Grant IAM permissions
```bash
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:service-PROJECT_NUMBER@gcp-sa-eventarc.iam.gserviceaccount.com" \
  --role="roles/eventarc.eventReceiver"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:service-PROJECT_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountTokenCreator"
```

### Step 5 — Deploy Cloud Function
```bash
cd cloud_function/

gcloud functions deploy gcs-to-bigquery-raw \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=. \
  --entry-point=gcs_to_bigquery_raw \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=ecommerce-raw-orders-hassan" \
  --memory=512MB \
  --timeout=300s
```

### Step 6 — Upload CSV to trigger pipeline
```bash
gsutil cp orders_raw.csv gs://ecommerce-raw-orders-hassan/2025-04-11/orders_raw.csv
```

### Step 7 — Verify data landed
```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as total FROM \`PROJECT_ID.raw.orders\`"
```

---

## Production & Scaling Notes
See `docs/production_scaling.md` → Section: Ingestion Layer
