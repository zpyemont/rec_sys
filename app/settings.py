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

    # Infinite scroll & personalization settings
    shown_set_ttl_days: int = 30           # Reset shown items after 30 days
    tier_1_pool_size: int = 5000           # New users (0-100 shown)
    tier_2_pool_size: int = 15000          # Engaged users (100-500 shown)
    tier_3_pool_size: int = 50000          # Power users (500-2000 shown)

    # Category adjacency for exploration (related categories)
    # Uses hierarchical subcategories for precise recommendations
    # Format: subcategory -> [related_subcategories]
    category_adjacency: dict = {
        # === CLOTHING - TOPS ===
        'T-Shirts': ['Tank Tops', 'Sweatshirts', 'Hoodies', 'Blouses'],
        'Blouses': ['Shirts', 'T-Shirts', 'Tank Tops', 'Sweaters'],
        'Shirts': ['Blouses', 'Polos', 'T-Shirts', 'Sweaters'],
        'Tank Tops': ['T-Shirts', 'Sports Bras', 'Workout Tops'],
        'Sweaters': ['Cardigans', 'Turtlenecks', 'Hoodies', 'Sweatshirts'],
        'Cardigans': ['Sweaters', 'Blazers', 'Hoodies', 'Jackets'],
        'Hoodies': ['Sweatshirts', 'Sweaters', 'T-Shirts', 'Jackets'],
        'Sweatshirts': ['Hoodies', 'T-Shirts', 'Sweaters', 'Tank Tops'],
        'Polos': ['Shirts', 'T-Shirts', 'Sweaters', 'Tank Tops'],
        'Turtlenecks': ['Sweaters', 'Cardigans', 'Shirts', 'Blouses'],

        # === CLOTHING - BOTTOMS ===
        'Jeans': ['Pants', 'Shorts', 'Leggings', 'Joggers'],
        'Pants': ['Jeans', 'Shorts', 'Skirts', 'Joggers'],
        'Leggings': ['Joggers', 'Sweatpants', 'Shorts', 'Workout Bottoms'],
        'Shorts': ['Jeans', 'Pants', 'Skirts', 'Athletic Shorts'],
        'Skirts': ['Dresses', 'Shorts', 'Pants', 'Jeans'],
        'Joggers': ['Sweatpants', 'Leggings', 'Pants', 'Athletic Shorts'],
        'Sweatpants': ['Joggers', 'Leggings', 'Lounge Pants', 'Shorts'],

        # === CLOTHING - DRESSES & ONE-PIECES ===
        'Dresses': ['Jumpsuits', 'Skirts', 'Rompers', 'Cocktail Dresses'],
        'Jumpsuits': ['Dresses', 'Rompers', 'Overalls', 'Pants'],
        'Rompers': ['Dresses', 'Jumpsuits', 'Shorts', 'Overalls'],
        'Overalls': ['Jumpsuits', 'Jeans', 'Pants', 'Rompers'],

        # === CLOTHING - OUTERWEAR ===
        'Jackets': ['Coats', 'Blazers', 'Vests', 'Hoodies'],
        'Coats': ['Jackets', 'Parkas', 'Blazers', 'Vests'],
        'Blazers': ['Jackets', 'Cardigans', 'Coats', 'Suits'],
        'Vests': ['Jackets', 'Coats', 'Cardigans', 'Blazers'],
        'Parkas': ['Coats', 'Jackets', 'Vests', 'Hoodies'],

        # === CLOTHING - ACTIVEWEAR ===
        'Sports Bras': ['Workout Tops', 'Tank Tops', 'Athletic Shorts', 'Leggings'],
        'Workout Tops': ['Sports Bras', 'Tank Tops', 'T-Shirts', 'Workout Bottoms'],
        'Workout Bottoms': ['Leggings', 'Athletic Shorts', 'Joggers', 'Shorts'],
        'Tracksuits': ['Hoodies', 'Sweatpants', 'Joggers', 'Athletic Shorts'],
        'Athletic Shorts': ['Workout Bottoms', 'Shorts', 'Leggings', 'Joggers'],

        # === CLOTHING - LOUNGEWEAR & SLEEPWEAR ===
        'Pajama Sets': ['Sleep Shirts', 'Robes', 'Lounge Pants', 'Lounge Tops'],
        'Sleep Shirts': ['Pajama Sets', 'Robes', 'Tank Tops', 'T-Shirts'],
        'Robes': ['Pajama Sets', 'Sleep Shirts', 'Lounge Tops', 'Cardigans'],
        'Lounge Pants': ['Sweatpants', 'Joggers', 'Pajama Sets', 'Leggings'],
        'Lounge Tops': ['Sweatshirts', 'Hoodies', 'T-Shirts', 'Robes'],

        # === CLOTHING - SWIMWEAR & BEACHWEAR ===
        'Bikinis': ['One-Pieces', 'Cover-Ups', 'Rash Guards', 'Shorts'],
        'One-Pieces': ['Bikinis', 'Rash Guards', 'Cover-Ups', 'Dresses'],
        'Cover-Ups': ['Bikinis', 'One-Pieces', 'Dresses', 'Robes'],
        'Rash Guards': ['Bikinis', 'One-Pieces', 'Workout Tops', 'Athletic Shorts'],

        # === CLOTHING - FORMALWEAR ===
        'Evening Gowns': ['Cocktail Dresses', 'Dresses', 'Blazers', 'Heels'],
        'Cocktail Dresses': ['Evening Gowns', 'Dresses', 'Heels', 'Pumps'],
        'Suits': ['Blazers', 'Pants', 'Skirts', 'Tuxedos'],
        'Tuxedos': ['Suits', 'Shirts', 'Formal Shoes', 'Blazers'],

        # === FOOTWEAR - SNEAKERS ===
        'Lifestyle Sneakers': ['Running Shoes', 'Training Shoes', 'Skateboard Shoes', 'Slides'],
        'Running Shoes': ['Training Shoes', 'Lifestyle Sneakers', 'Athletic Shorts', 'Leggings'],
        'Training Shoes': ['Running Shoes', 'Lifestyle Sneakers', 'Basketball Shoes', 'Workout Bottoms'],
        'Basketball Shoes': ['Training Shoes', 'Lifestyle Sneakers', 'Athletic Shorts', 'Joggers'],
        'Skateboard Shoes': ['Lifestyle Sneakers', 'Slides', 'Shorts', 'Jeans'],

        # === FOOTWEAR - BOOTS ===
        'Ankle Boots': ['Knee-High Boots', 'Desert Boots', 'Jeans', 'Leggings'],
        'Knee-High Boots': ['Over-the-Knee Boots', 'Ankle Boots', 'Skirts', 'Dresses'],
        'Over-the-Knee Boots': ['Knee-High Boots', 'Ankle Boots', 'Skirts', 'Dresses'],
        'Hiking Boots': ['Work Boots', 'Desert Boots', 'Jeans', 'Pants'],
        'Work Boots': ['Hiking Boots', 'Desert Boots', 'Jeans', 'Pants'],
        'Desert Boots': ['Ankle Boots', 'Work Boots', 'Jeans', 'Pants'],

        # === FOOTWEAR - SANDALS & SLIDES ===
        'Flat Sandals': ['Heeled Sandals', 'Slides', 'Flip-Flops', 'Shorts'],
        'Heeled Sandals': ['Flat Sandals', 'Pumps', 'Wedges', 'Dresses'],
        'Slides': ['Flat Sandals', 'Flip-Flops', 'House Slippers', 'Shorts'],
        'Flip-Flops': ['Slides', 'Flat Sandals', 'Shorts', 'Cover-Ups'],

        # === FOOTWEAR - HEELS ===
        'Pumps': ['Stilettos', 'Block Heels', 'Kitten Heels', 'Dresses'],
        'Stilettos': ['Pumps', 'Heeled Sandals', 'Block Heels', 'Evening Gowns'],
        'Block Heels': ['Pumps', 'Wedges', 'Ankle Boots', 'Skirts'],
        'Wedges': ['Block Heels', 'Heeled Sandals', 'Pumps', 'Dresses'],
        'Kitten Heels': ['Pumps', 'Ballet Flats', 'Mules', 'Skirts'],

        # === FOOTWEAR - FLATS & LOAFERS ===
        'Ballet Flats': ['Loafers', 'Mules', 'Kitten Heels', 'Skirts'],
        'Loafers': ['Oxfords', 'Ballet Flats', 'Boat Shoes', 'Pants'],
        'Mules': ['Ballet Flats', 'Slides', 'Kitten Heels', 'Jeans'],
        'Oxfords': ['Loafers', 'Boat Shoes', 'Ankle Boots', 'Pants'],
        'Boat Shoes': ['Loafers', 'Oxfords', 'Shorts', 'Jeans'],

        # === FOOTWEAR - SLIPPERS ===
        'House Slippers': ['Slide Slippers', 'Robes', 'Lounge Pants', 'Pajama Sets'],
        'Slide Slippers': ['House Slippers', 'Slides', 'Robes', 'Lounge Tops'],

        # === ACCESSORIES - BAGS ===
        'Handbags': ['Crossbody Bags', 'Clutches', 'Backpacks', 'Wallets'],
        'Crossbody Bags': ['Handbags', 'Belt Bags', 'Backpacks', 'Wallets'],
        'Clutches': ['Handbags', 'Wallets', 'Evening Gowns', 'Heels'],
        'Backpacks': ['Crossbody Bags', 'Handbags', 'Belt Bags', 'Sneakers'],
        'Belt Bags': ['Crossbody Bags', 'Backpacks', 'Wallets', 'Jeans'],
        'Wallets': ['Handbags', 'Clutches', 'Belt Bags', 'Crossbody Bags'],

        # === ACCESSORIES - JEWELRY ===
        'Necklaces': ['Earrings', 'Bracelets', 'Rings', 'Dresses'],
        'Earrings': ['Necklaces', 'Bracelets', 'Rings', 'Blouses'],
        'Bracelets': ['Rings', 'Necklaces', 'Earrings', 'Watches'],
        'Rings': ['Bracelets', 'Necklaces', 'Earrings', 'Anklets'],
        'Anklets': ['Rings', 'Bracelets', 'Sandals', 'Heels'],

        # === ACCESSORIES - HATS & HEADWEAR ===
        'Hats': ['Headbands', 'Sunglasses', 'Scarves', 'Jackets'],
        'Headbands': ['Hair Clips', 'Hats', 'Earrings', 'Dresses'],
        'Hair Clips': ['Headbands', 'Earrings', 'Necklaces', 'T-Shirts'],

        # === ACCESSORIES - SCARVES & WRAPS ===
        'Scarves': ['Shawls', 'Hats', 'Gloves', 'Jackets'],
        'Shawls': ['Scarves', 'Robes', 'Evening Gowns', 'Cardigans'],

        # === ACCESSORIES - MISC ===
        'Belts': ['Jeans', 'Pants', 'Skirts', 'Dresses'],
        'Sunglasses': ['Hats', 'Scarves', 'Cover-Ups', 'Sandals'],
        'Watches': ['Bracelets', 'Rings', 'Shirts', 'Suits'],
        'Gloves': ['Scarves', 'Hats', 'Coats', 'Jackets'],

        # === BACKWARD COMPATIBILITY (old category names) ===
        # Keep lowercase versions for existing data
        'dresses': ['Dresses', 'Jumpsuits', 'Skirts', 'Rompers'],
        'tops': ['T-Shirts', 'Blouses', 'Shirts', 'Sweaters'],
        'jeans': ['Jeans', 'Pants', 'Shorts', 'Skirts'],
        'sneakers': ['Lifestyle Sneakers', 'Running Shoes', 'Training Shoes', 'Slides'],
        'jackets': ['Jackets', 'Coats', 'Blazers', 'Hoodies'],
        'skirts': ['Skirts', 'Dresses', 'Shorts', 'Pants'],
        'sweaters': ['Sweaters', 'Hoodies', 'Cardigans', 'Sweatshirts'],
        'shirts': ['Shirts', 'Polos', 'T-Shirts', 'Blouses'],
        'pants': ['Pants', 'Jeans', 'Shorts', 'Joggers'],
        't-shirts': ['T-Shirts', 'Tank Tops', 'Shirts', 'Polos'],
        'bags': ['Handbags', 'Crossbody Bags', 'Backpacks', 'Clutches'],
        'jewelry': ['Necklaces', 'Earrings', 'Bracelets', 'Rings'],
    }

    # Kafka (Confluent Cloud)
    kafka_bootstrap_servers: str = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
    kafka_api_key: str = "DGCM2ZPZ5T2ZUKFE"
    kafka_api_secret: str = ""  # Set via environment variable
    kafka_enabled: bool = False  # Feature flag to enable Kafka publishing

    # Monolith TensorFlow Serving
    monolith_host: str = "localhost"
    monolith_port: int = 8500
    monolith_model_name: str = "fashion_ranking"
    monolith_timeout: float = 5.0
    monolith_enabled: bool = False  # Feature flag to enable Monolith integration

    # Worker ID for request ID generation (for distributed deployments)
    worker_id: int = 1

    # Stock filtering - filter out products with these availability values
    filter_out_of_stock: bool = True  # Feature flag to enable stock filtering
    excluded_availability_values: list[str] = [
        "out of stock",
        "sold out",
        "unavailable",
        "out_of_stock",
        "soldout"
    ]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
