from app.styling.palette import extract_colours, colour_overlap

class TestExtractColours:
    def test_extracts_black(self):
        assert "black" in extract_colours("Classic black midi dress")

    def test_extracts_champagne(self):
        assert "champagne" in extract_colours("Champagne slip dress with satin finish")

    def test_extracts_ivory(self):
        assert "ivory" in extract_colours("Ivory linen blazer")

    def test_case_insensitive(self):
        assert "navy" in extract_colours("NAVY BLUE silk blouse")

    def test_extracts_multiple(self):
        result = extract_colours("Black and white striped midi skirt")
        assert "black" in result
        assert "white" in result

    def test_empty_string(self):
        assert extract_colours("") == []

    def test_no_colours(self):
        assert extract_colours("Slim fit relaxed waistband") == []

    def test_deduplicates(self):
        result = extract_colours("navy navy navy")
        assert result.count("navy") == 1


class TestColourOverlap:
    def test_identical_palettes(self):
        assert colour_overlap(["black", "white"], ["black", "white"]) == 1.0

    def test_no_overlap(self):
        assert colour_overlap(["black"], ["white"]) == 0.0

    def test_partial_overlap(self):
        score = colour_overlap(["black", "white"], ["black", "red"])
        assert 0.0 < score < 1.0

    def test_empty_lists(self):
        assert colour_overlap([], []) == 0.0

    def test_one_empty(self):
        assert colour_overlap(["black"], []) == 0.0
