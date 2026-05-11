import pytest
from app.styling.slot_mapper import subcategory_to_slot, SLOT_MAP, VALID_SLOTS

class TestSubcategoryToSlot:
    def test_dress_maps_to_dress(self):
        assert subcategory_to_slot("Dresses") == "dress"

    def test_jeans_maps_to_bottom(self):
        assert subcategory_to_slot("Jeans") == "bottom"

    def test_ankle_boots_maps_to_shoes(self):
        assert subcategory_to_slot("Ankle Boots") == "shoes"

    def test_handbag_maps_to_bag(self):
        assert subcategory_to_slot("Handbags") == "bag"

    def test_blazer_maps_to_outerwear(self):
        assert subcategory_to_slot("Blazers") == "outerwear"

    def test_tshirt_maps_to_top(self):
        assert subcategory_to_slot("T-Shirts") == "top"

    def test_earrings_maps_to_accessory(self):
        assert subcategory_to_slot("Earrings") == "accessory"

    def test_sports_bra_maps_to_activewear(self):
        assert subcategory_to_slot("Sports Bras") == "activewear"

    def test_unknown_returns_none(self):
        assert subcategory_to_slot("Unicorn Pants") is None

    def test_case_insensitive(self):
        assert subcategory_to_slot("dresses") == "dress"
        assert subcategory_to_slot("JEANS") == "bottom"

    def test_all_settings_subcategories_map_to_something(self):
        """Every key in settings.category_adjacency should map to a slot."""
        from app.settings import get_settings
        settings = get_settings()
        unmapped = []
        for subcat in settings.category_adjacency:
            if subcategory_to_slot(subcat) is None:
                unmapped.append(subcat)
        assert unmapped == [], f"Unmapped subcategories: {unmapped}"

    def test_valid_slots_constant(self):
        assert set(VALID_SLOTS) == {"top", "bottom", "dress", "outerwear", "shoes", "bag", "accessory", "activewear"}

    def test_slot_map_covers_all_slots(self):
        assert set(SLOT_MAP.keys()) == set(VALID_SLOTS)
