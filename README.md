# Ranker Service (GKE-ready)

A FastAPI-based ranker service that assembles diversified feeds from product candidates stored in Postgres/BigQuery and assets in GCS. The ML model is intentionally mocked (pass) at this stage; the service uses simple fallbacks.

## Endpoints
- GET `/healthz` – health check
- GET `/get_diverse_feed?user_id=...&device=...&n=50` – returns diversified feed

## Requirements
- Python 3.11
- Postgres (Cloud SQL recommended)
- Firestore (for user shown-set history)
- Optional: BigQuery and GCS access

## Local Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export POSTGRES_DSN="host=localhost port=5432 user=postgres password=postgres dbname=postgres"
# Optional BigQuery/GCS
export BQ_PROJECT=your-project
export BQ_DATASET=your_dataset
export BQ_TABLE_PRODUCTS=products
export GCS_BUCKET_PRODUCTS=looksy_shopify_parsed
# Firestore uses Application Default Credentials (ADC)
# Ensure `gcloud auth application-default login` or service account on GKE
uvicorn app.main:app --host 0.0.0.0 --port 8500
```

## Docker
```bash
docker build -t gcr.io/PROJECT_ID/ranker:latest .
docker run -p 8500:8500 \
  -e POSTGRES_DSN="$POSTGRES_DSN" \
  -e BQ_PROJECT=$BQ_PROJECT \
  -e BQ_DATASET=$BQ_DATASET \
  -e BQ_TABLE_PRODUCTS=$BQ_TABLE_PRODUCTS \
  -e GCS_BUCKET_PRODUCTS=$GCS_BUCKET_PRODUCTS \
  gcr.io/PROJECT_ID/ranker:latest
```

## Kubernetes (GKE)
- Update `k8s/deployment.yaml` image to your registry
- Use Workload Identity or mount credentials for Firestore/BigQuery
- Create secret for `postgres_dsn` and config for BQ dataset/project
```bash
kubectl apply -f k8s/deployment.yaml
```

## Notes
- The model function `predict()` in `app/ranker/model.py` is a stub (pass).
- Firestore stores shown history under `user_feed_history/{user_id}/shown/{prod_id}`.
- Candidate sources and freshness/features are placeholders; replace with your feature store and scoring.
