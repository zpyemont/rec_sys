from typing import Optional

# Tech debt: this mapping should become a DB column added at ingestion time.
# When new subcategories appear in the catalog, add them here.
VALID_SLOTS = ("top", "bottom", "dress", "outerwear", "shoes", "bag", "accessory", "activewear")

SLOT_MAP: dict[str, set[str]] = {
    "top": {
        "T-Shirts", "Blouses", "Shirts", "Tank Tops", "Sweaters", "Cardigans",
        "Hoodies", "Sweatshirts", "Polos", "Turtlenecks", "Lounge Tops",
        "Sleep Shirts", "Workout Tops", "Rash Guards", "Cover-Ups",
        # lowercase backwards-compat
        "tops", "t-shirts", "sweaters", "shirts",
    },
    "bottom": {
        "Jeans", "Pants", "Leggings", "Shorts", "Skirts", "Joggers",
        "Sweatpants", "Athletic Shorts", "Workout Bottoms", "Lounge Pants",
        "Pajama Sets", "Overalls",
        # lowercase
        "jeans", "skirts", "pants",
    },
    "dress": {
        "Dresses", "Jumpsuits", "Rompers", "Evening Gowns", "Cocktail Dresses",
        "One-Pieces",
        # lowercase
        "dresses",
    },
    "outerwear": {
        "Jackets", "Coats", "Blazers", "Vests", "Parkas", "Suits", "Tuxedos",
        "Shawls", "Robes",
        # lowercase
        "jackets",
    },
    "shoes": {
        "Lifestyle Sneakers", "Running Shoes", "Training Shoes", "Basketball Shoes",
        "Skateboard Shoes", "Ankle Boots", "Knee-High Boots", "Over-the-Knee Boots",
        "Hiking Boots", "Work Boots", "Desert Boots", "Flat Sandals", "Heeled Sandals",
        "Slides", "Flip-Flops", "Pumps", "Stilettos", "Block Heels", "Wedges",
        "Kitten Heels", "Ballet Flats", "Loafers", "Mules", "Oxfords", "Boat Shoes",
        "House Slippers", "Slide Slippers", "Formal Shoes",
        # lowercase
        "sneakers",
    },
    "bag": {
        "Handbags", "Crossbody Bags", "Clutches", "Backpacks", "Belt Bags", "Wallets",
        # lowercase
        "bags",
    },
    "accessory": {
        "Necklaces", "Earrings", "Bracelets", "Rings", "Anklets", "Watches",
        "Hats", "Headbands", "Hair Clips", "Scarves", "Belts", "Sunglasses", "Gloves",
        # lowercase
        "jewelry",
    },
    "activewear": {
        "Sports Bras", "Bikinis", "Tracksuits",
    },
}

# Inverted index for O(1) lookup
_SUBCAT_TO_SLOT: dict[str, str] = {
    subcat.lower(): slot
    for slot, subcats in SLOT_MAP.items()
    for subcat in subcats
}


def subcategory_to_slot(subcategory: str) -> Optional[str]:
    """Map a product subcategory string to a styling slot. Returns None if unmapped."""
    return _SUBCAT_TO_SLOT.get(subcategory.lower())
