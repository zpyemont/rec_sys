# Ranker Service (GKE-ready)

A FastAPI-based ranker service that assembles diversified feeds from product candidates stored in Postgres/BigQuery and assets in GCS. The ML model is intentionally mocked (pass) at this stage; the service uses simple fallbacks.

## Endpoints
- GET `/healthz` – health check
- GET `/get_diverse_feed?user_id=...&device=...&n=50` – returns diversified feed

## Requirements
- Python 3.11
- Postgres (Cloud SQL recommended)
- Redis (for user shown-set)
- Optional: BigQuery and GCS access

## Local Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export REDIS_URL=redis://localhost:6379/0
export POSTGRES_DSN="host=localhost port=5432 user=postgres password=postgres dbname=postgres"
# Optional BigQuery/GCS
export BQ_PROJECT=your-project
export BQ_DATASET=your_dataset
export BQ_TABLE_PRODUCTS=products
export GCS_BUCKET_PRODUCTS=looksy_shopify_parsed
uvicorn app.main:app --host 0.0.0.0 --port 8500
```

## Docker
```bash
docker build -t gcr.io/PROJECT_ID/ranker:latest .
docker run -p 8500:8500 \
  -e REDIS_URL=$REDIS_URL \
  -e POSTGRES_DSN="$POSTGRES_DSN" \
  -e BQ_PROJECT=$BQ_PROJECT \
  -e BQ_DATASET=$BQ_DATASET \
  -e BQ_TABLE_PRODUCTS=$BQ_TABLE_PRODUCTS \
  -e GCS_BUCKET_PRODUCTS=$GCS_BUCKET_PRODUCTS \
  gcr.io/PROJECT_ID/ranker:latest
```

## Kubernetes (GKE)
- Update `k8s/deployment.yaml` image to your registry
- Create secrets `ranker-secrets` with keys: `redis_url`, `postgres_dsn`, `bq_project`, `bq_dataset`
```bash
kubectl apply -f k8s/deployment.yaml
```

## Notes
- The model function `predict()` in `app/ranker/model.py` is a stub (pass).
- Candidate sources and freshness/features are placeholders; replace with your feature store and scoring.
