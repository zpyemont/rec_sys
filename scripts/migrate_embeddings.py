"""
Migrate CLIP image embeddings from BigQuery to PostgreSQL with pgvector.

This script:
1. Reads product embeddings from BigQuery
2. Batch inserts them into PostgreSQL products.image_embedding column
3. Handles errors gracefully and provides progress updates

Usage:
    python scripts/migrate_embeddings.py

Environment variables required:
    - POSTGRES_DSN or (PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE)
    - GOOGLE_APPLICATION_CREDENTIALS (for BigQuery)
"""

import os
import sys
import logging
from typing import List, Tuple, Optional
import numpy as np

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
import psycopg2
from psycopg2.extras import execute_batch

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_postgres_dsn() -> str:
    """Build PostgreSQL DSN from environment variables."""
    if dsn := os.getenv('POSTGRES_DSN'):
        return dsn

    host = os.getenv('PG_HOST', 'localhost')
    port = os.getenv('PG_PORT', '5432')
    user = os.getenv('PG_USER', 'postgres')
    password = os.getenv('PG_PASSWORD', '')
    database = os.getenv('PG_DATABASE', 'product')

    return f"host={host} port={port} user={user} password={password} dbname={database}"


def get_bq_table() -> str:
    """Get BigQuery table path from environment or use default."""
    dataset = os.getenv('BQ_DATASET', 'your_dataset')
    table = os.getenv('BQ_TABLE', 'products')
    return f"{dataset}.{table}"


def fetch_embeddings_from_bq(
    bq_client: bigquery.Client,
    bq_table: str,
    batch_size: int = 10000,
    offset: int = 0
) -> List[Tuple[str, List[float]]]:
    """
    Fetch product embeddings from BigQuery in batches.

    Returns list of (product_id, embedding) tuples.
    """
    query = f"""
        SELECT
            product_id,
            image_embedding
        FROM `{bq_table}`
        WHERE image_embedding IS NOT NULL
          AND ARRAY_LENGTH(image_embedding) = 512
        ORDER BY product_id
        LIMIT {batch_size}
        OFFSET {offset}
    """

    results = []
    for row in bq_client.query(query):
        product_id = row['product_id']
        embedding = list(row['image_embedding'])
        results.append((product_id, embedding))

    return results


def insert_embeddings_to_pg(
    pg_conn,
    embeddings: List[Tuple[str, List[float]]],
    batch_size: int = 1000
) -> int:
    """
    Insert embeddings into PostgreSQL using batch updates.

    Returns number of rows updated.
    """
    if not embeddings:
        return 0

    update_sql = """
        UPDATE products
        SET image_embedding = %s::vector
        WHERE product_id = %s
    """

    # Prepare data: swap order to (embedding, product_id) for SQL params
    data = [(emb, pid) for pid, emb in embeddings]

    with pg_conn.cursor() as cur:
        execute_batch(cur, update_sql, data, page_size=batch_size)

    pg_conn.commit()
    return len(data)


def count_bq_embeddings(bq_client: bigquery.Client, bq_table: str) -> int:
    """Count total embeddings in BigQuery."""
    query = f"""
        SELECT COUNT(*) as count
        FROM `{bq_table}`
        WHERE image_embedding IS NOT NULL
          AND ARRAY_LENGTH(image_embedding) = 512
    """
    for row in bq_client.query(query):
        return row['count']
    return 0


def count_pg_embeddings(pg_conn) -> int:
    """Count embeddings already in PostgreSQL."""
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM products WHERE image_embedding IS NOT NULL")
        return cur.fetchone()[0]


def migrate_embeddings(
    batch_size: int = 10000,
    dry_run: bool = False
) -> None:
    """
    Main migration function.

    Args:
        batch_size: Number of rows to process per batch
        dry_run: If True, only count and report without writing
    """
    logger.info("Starting embedding migration from BigQuery to PostgreSQL")

    # Initialize clients
    bq_client = bigquery.Client()
    bq_table = get_bq_table()
    pg_dsn = get_postgres_dsn()

    logger.info(f"BigQuery table: {bq_table}")
    logger.info(f"PostgreSQL DSN: {pg_dsn[:50]}...")

    # Count source data
    total_bq = count_bq_embeddings(bq_client, bq_table)
    logger.info(f"Found {total_bq:,} embeddings in BigQuery")

    if dry_run:
        logger.info("Dry run mode - no data will be written")
        return

    if total_bq == 0:
        logger.warning("No embeddings found in BigQuery. Exiting.")
        return

    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(pg_dsn)

    # Count existing embeddings
    existing_pg = count_pg_embeddings(pg_conn)
    logger.info(f"Found {existing_pg:,} existing embeddings in PostgreSQL")

    # Process in batches
    offset = 0
    total_migrated = 0

    while True:
        logger.info(f"Fetching batch at offset {offset:,}...")

        embeddings = fetch_embeddings_from_bq(bq_client, bq_table, batch_size, offset)

        if not embeddings:
            logger.info("No more embeddings to fetch. Migration complete.")
            break

        logger.info(f"Fetched {len(embeddings):,} embeddings, inserting into PostgreSQL...")

        try:
            inserted = insert_embeddings_to_pg(pg_conn, embeddings)
            total_migrated += inserted
            logger.info(f"Inserted {inserted:,} embeddings. Total: {total_migrated:,}/{total_bq:,}")
        except Exception as e:
            logger.error(f"Error inserting batch at offset {offset}: {e}")
            pg_conn.rollback()
            raise

        offset += batch_size

        # Progress
        progress = (offset / total_bq) * 100
        logger.info(f"Progress: {progress:.1f}%")

    # Final verification
    final_pg = count_pg_embeddings(pg_conn)
    logger.info(f"Migration complete. PostgreSQL now has {final_pg:,} embeddings")

    pg_conn.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Migrate embeddings from BigQuery to PostgreSQL')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for processing')
    parser.add_argument('--dry-run', action='store_true', help='Count only, do not write')

    args = parser.parse_args()

    migrate_embeddings(batch_size=args.batch_size, dry_run=args.dry_run)
