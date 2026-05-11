import re

# Curated colour wordlist for fashion context.
# Tech debt: replace with embedding-based colour extraction at ingestion time.
COLOUR_TOKENS = {
    "black", "white", "grey", "gray", "navy", "blue", "red", "pink", "rose",
    "burgundy", "wine", "maroon", "green", "olive", "sage", "khaki", "beige",
    "camel", "tan", "brown", "chocolate", "rust", "terracotta", "orange",
    "yellow", "mustard", "gold", "silver", "cream", "ivory", "ecru", "champagne",
    "blush", "nude", "coral", "lilac", "purple", "violet", "lavender", "teal",
    "emerald", "mint", "cobalt", "indigo", "copper", "bronze", "mocha", "taupe",
    "charcoal", "off-white", "stone", "sand",
}

_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(c) for c in sorted(COLOUR_TOKENS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)


def extract_colours(text: str) -> list[str]:
    """Return deduplicated colour tokens found in text, order-preserving."""
    seen: set[str] = set()
    result: list[str] = []
    for m in _PATTERN.finditer(text):
        token = m.group(0).lower()
        if token not in seen:
            seen.add(token)
            result.append(token)
    return result


def colour_overlap(a: list[str], b: list[str]) -> float:
    """Jaccard similarity between two colour token lists."""
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 0.0
    union = sa | sb
    return len(sa & sb) / len(union)
