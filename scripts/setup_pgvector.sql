-- Setup pgvector extension and embedding column for products table
-- Run this script on your PostgreSQL database

-- Enable pgvector extension (requires superuser or rds_superuser on RDS)
CREATE EXTENSION IF NOT EXISTS vector;

-- Add column for CLIP image embeddings (512-dim)
-- This will store the pre-computed CLIP embeddings from your ingestion pipeline
ALTER TABLE products ADD COLUMN IF NOT EXISTS image_embedding vector(512);

-- Create HNSW index for fast approximate nearest neighbor search
-- Parameters:
--   m = 16: Number of connections per layer (higher = better recall, more memory)
--   ef_construction = 64: Size of dynamic candidate list during construction
--
-- For ~100K items, this provides:
--   - Query latency: <10ms
--   - Recall: ~95%+
--   - Memory: ~100MB for index
CREATE INDEX IF NOT EXISTS idx_products_embedding ON products
USING hnsw (image_embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- Note: If you're on an older PostgreSQL/pgvector version without HNSW, use IVFFlat:
-- CREATE INDEX idx_products_embedding ON products
-- USING ivfflat (image_embedding vector_cosine_ops)
-- WITH (lists = 100);  -- sqrt(num_rows) is a good starting point

-- Verify setup
SELECT
    extname,
    extversion
FROM pg_extension
WHERE extname = 'vector';

-- Check if column was added
SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_name = 'products'
  AND column_name = 'image_embedding';
