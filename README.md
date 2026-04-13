# E-Commerce BI Pipeline — GCP Data & AI Engineer Assessment

A fully automated data pipeline built on Google Cloud Platform that solves three business problems:

1. **Automated Ingestion** — CSV orders land in GCS, Cloud Function loads them into BigQuery automatically
2. **Data Quality** — BigQuery Stored Procedure validates, cleans and quarantines bad data daily
3. **BI Chatbot** — Gemini API chatbot lets non-technical users query BigQuery in plain English

---

## Architecture

```
CSV Upload (GCS)
      ↓
Cloud Function (event-driven)
      ↓
BigQuery raw.orders  ←── all strings, audit columns, partitioned
      ↓
Stored Procedure (Scheduled Query 7AM daily)
      ↓
BigQuery gold.orders         ← clean, typed, dashboard-ready
BigQuery gold.orders_quarantine ← rejected rows with failure reason
BigQuery gold.dq_run_log     ← daily DQ check results
      ↓
Gemini API → SQL → BigQuery → Plain English Answer
```

---


## GCP Services Used

| Service | Purpose | Free Tier Used |
|---|---|---|
| Cloud Storage | Raw CSV landing zone | ✅ |
| Cloud Functions Gen2 | Event-driven CSV ingestion | ✅ |
| BigQuery | Data warehouse (raw + gold) | ✅ Sandbox |
| Gemini API | Natural language to SQL | ✅ Free quota |
| Cloud Scheduler | Daily pipeline trigger | ✅ |
| Eventarc | GCS → Cloud Function trigger | ✅ |

---

## Quick Start

### 1. Generate test data
```bash
cd data
python generate_orders.py
```

### 2. Deploy ingestion pipeline
See `ingestion/README.md`

### 3. Run data quality pipeline
See `data_quality/README.md`

