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

### Database Connection
This service connects to the PostgreSQL database created by the ingestion_pipeline:
- **Database Name:** `product` (NOT `postgres`)
- **Cloud SQL Instance:** `looksyuk:europe-west1:products`
- **Primary Table:** `products`

For local development, ensure you have access to the database or set up Cloud SQL Proxy:
```bash
# Option 1: Cloud SQL Proxy (recommended for cloud database)
cloud_sql_proxy -instances=looksyuk:europe-west1:products=tcp:5432

# Option 2: Direct connection (if database is local)
# Ensure PostgreSQL is running with the 'product' database
```

### Environment Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# PostgreSQL connection (update with your credentials)
export POSTGRES_DSN="host=localhost port=5432 user=postgres password=YOUR_PASSWORD dbname=product"

# Optional BigQuery/GCS (for fallback metadata queries)
export BQ_PROJECT=looksyuk
export BQ_DATASET=shopify
export BQ_TABLE_PRODUCTS=products
export GCS_BUCKET_PRODUCTS=looksy-shopify-parsed

# Firestore uses Application Default Credentials (ADC)
# Ensure `gcloud auth application-default login` or service account on GKE
gcloud auth application-default login

# Start the service
uvicorn app.main:app --host 0.0.0.0 --port 8500
```

## Docker
```bash
docker build -t gcr.io/looksyuk/ranker:latest .
docker run -p 8500:8500 \
  -e POSTGRES_DSN="host=localhost port=5432 user=postgres password=YOUR_PASSWORD dbname=product" \
  -e BQ_PROJECT=looksyuk \
  -e BQ_DATASET=shopify \
  -e BQ_TABLE_PRODUCTS=products \
  -e GCS_BUCKET_PRODUCTS=looksy-shopify-parsed \
  gcr.io/looksyuk/ranker:latest
```

## Kubernetes (GKE)
- Update `k8s/deployment.yaml` image to your registry
- Use Workload Identity or mount credentials for Firestore/BigQuery
- Create secret for `postgres_dsn` and config for BQ dataset/project
```bash
kubectl apply -f k8s/deployment.yaml
```

## Database Schema

### PostgreSQL `products` Table
This service queries the `products` table created by the ingestion_pipeline. The schema is:

```sql
CREATE TABLE products (
    product_id VARCHAR PRIMARY KEY,
    domain VARCHAR,
    url VARCHAR,
    title VARCHAR,
    description TEXT,
    category VARCHAR,
    gender VARCHAR,
    images TEXT[],              -- PostgreSQL array of image URLs
    brand VARCHAR,
    vendor VARCHAR,
    sku VARCHAR,
    gtin VARCHAR,
    mpn VARCHAR,
    price NUMERIC,
    currency VARCHAR,
    availability VARCHAR,
    parsed_at TIMESTAMP,        -- When product was parsed/scraped
    selectors_hash VARCHAR,
    used_gemini BOOLEAN,
    updated_at TIMESTAMP,       -- Auto-updated on upsert
    created_at TIMESTAMP
);
```

### Query Methods Available
The [app/connectors/postgres.py](app/connectors/postgres.py) provides these methods:

1. **`get_recent_products(hours, limit)`** - Products parsed in the last N hours
   - Uses `parsed_at` column
   - Returns list of `product_id` strings

2. **`get_popular_products(limit)`** - Most recently updated products (popularity placeholder)
   - Orders by `updated_at DESC, created_at DESC`
   - Returns list of `product_id` strings

3. **`get_by_brand_or_vendor(category, limit)`** - Products filtered by brand/vendor
   - Matches `brand` or `vendor` columns (case-insensitive)
   - Returns list of `product_id` strings

4. **`get_product_metadata_for_ids(prod_ids)`** - Batch fetch product details
   - Returns: `prod_id`, `title`, `price`, `image_url` (first image from array)
   - Used to hydrate final feed response

### BigQuery Fallback
If PostgreSQL returns empty results, the service falls back to BigQuery:
- **Dataset:** `shopify`
- **Table:** `products`
- Same schema as PostgreSQL (with `images` as `STRING REPEATED`)

## Notes
- The model function `predict()` in `app/ranker/model.py` is a stub (pass).
- Firestore stores shown history under `user_feed_history/{user_id}/shown/{prod_id}`.
- Candidate sources and freshness/features are placeholders; replace with your feature store and scoring.
- **Database connection:** Ensure `dbname=product` (not `postgres`) when connecting to the ingestion_pipeline database.
