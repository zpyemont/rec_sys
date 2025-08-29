from functools import lru_cache
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class BucketRatios(BaseModel):
    personal: float = 0.75
    category: float = 0.15
    fresh: float = 0.10


class Settings(BaseSettings):
    service_port: int = 8500

    # Redis
    redis_url: str | None = None

    # Postgres
    postgres_dsn: str | None = None
    pg_host: str | None = None
    pg_port: int | None = None
    pg_user: str | None = None
    pg_password: str | None = None
    pg_database: str | None = None

    # BigQuery
    bq_project: str | None = None
    bq_dataset: str | None = None
    bq_table_products: str | None = "products"

    # GCS
    gcs_bucket_products: str | None = "looksy_shopify_parsed"

    # Feed
    feed_default_size: int = 50

    bucket_ratios: BucketRatios = BucketRatios()

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
