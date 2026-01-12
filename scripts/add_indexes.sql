-- Performance indexes for candidate retrieval queries
-- Run this script on your PostgreSQL database
-- Uses CONCURRENTLY to avoid locking the table during index creation

-- For freshness queries: get_fresh_candidates()
-- Query: SELECT product_id FROM products WHERE is_active = true AND parsed_at >= NOW() - INTERVAL '48 hours' ORDER BY parsed_at DESC
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_parsed_at_active
ON products(parsed_at DESC)
WHERE is_active = true;

-- For trending queries: get_trending_candidates()
-- Query: SELECT product_id FROM products WHERE is_active = true AND like_count > 5 AND parsed_at >= NOW() - INTERVAL '48 hours' ORDER BY like_count DESC
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_trending
ON products(like_count DESC, parsed_at DESC)
WHERE is_active = true AND like_count > 5;

-- For category-based queries (existing heuristic fallback)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_subcategory_active
ON products(subcategory)
WHERE is_active = true;

-- For brand diversity window functions
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_brand_parsed
ON products(brand, parsed_at DESC)
WHERE is_active = true;

-- For random sampling: pre-compute a random bucket to avoid ORDER BY RANDOM()
-- First, add the column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'products' AND column_name = 'random_bucket'
    ) THEN
        ALTER TABLE products ADD COLUMN random_bucket INT DEFAULT (RANDOM() * 100)::INT;
    END IF;
END $$;

-- Index for random sampling
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_random_bucket
ON products(random_bucket)
WHERE is_active = true AND like_count >= 3;

-- Update existing rows with random buckets (one-time operation)
UPDATE products
SET random_bucket = (RANDOM() * 100)::INT
WHERE random_bucket IS NULL;

-- Analyze tables to update query planner statistics
ANALYZE products;
