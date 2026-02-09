"""
Genre normalization engine.

Normalizes formatting duplicates in genre/tag strings (from Last.fm) into
canonical forms. Pure Python — no database dependency.

The normalization pipeline:
1. Lowercase and strip whitespace
2. Normalize unicode (smart quotes, accents)
3. Collapse separators (& → and, / → handled contextually)
4. Apply explicit alias overrides (rnb → r&b, etc.)
5. Normalize hyphenation (post-punk stays hyphenated, post punk → post-punk)
"""

import re
import unicodedata

# Prefixes that should keep their hyphen when followed by a genre word.
# "post punk" → "post-punk", "neo soul" → "neo-soul", etc.
HYPHEN_PREFIXES: set[str] = {
    "acid",
    "afro",
    "alt",
    "anti",
    "art",
    "avant",
    "dark",
    "dream",
    "electro",
    "euro",
    "folk",
    "garage",
    "hard",
    "hyper",
    "indie",
    "jazz",
    "math",
    "neo",
    "noise",
    "nu",
    "post",
    "power",
    "pre",
    "proto",
    "psycho",
    "slow",
    "space",
    "speed",
    "stoner",
    "synth",
    "trip",
}


# Explicit alias map: variant → canonical form.
# Applied AFTER lowercasing and whitespace normalization, BEFORE hyphenation.
ALIAS_MAP: dict[str, str] = {
    # R&B / RnB variants
    "rnb": "r&b",
    "r and b": "r&b",
    "r n b": "r&b",
    "rhythm and blues": "r&b",
    "rhythmandblues": "r&b",
    # Rock and roll variants
    "rock n roll": "rock and roll",
    "rock & roll": "rock and roll",
    "rock n' roll": "rock and roll",
    "rock'n'roll": "rock and roll",
    "rocknroll": "rock and roll",
    "rock roll": "rock and roll",
    # Hip hop variants
    "hip hop": "hip-hop",
    "hiphop": "hip-hop",
    "hip-hop/rap": "hip-hop",
    "hip hop/rap": "hip-hop",
    # Trip hop variants
    "trip hop": "trip-hop",
    "triphop": "trip-hop",
    # Lo-fi variants
    "lo fi": "lo-fi",
    "lofi": "lo-fi",
    "low fi": "lo-fi",
    "low-fi": "lo-fi",
    # Electronic variants
    "electronica": "electronic",
    # D&B / DnB variants
    "drum and bass": "drum & bass",
    "drum n bass": "drum & bass",
    "drum'n'bass": "drum & bass",
    "drumnbass": "drum & bass",
    "dnb": "drum & bass",
    "d&b": "drum & bass",
    "d and b": "drum & bass",
    # Shoegaze
    "shoe gaze": "shoegaze",
    "shoe-gaze": "shoegaze",
    # Synth pop
    "synth pop": "synth-pop",
    "synthpop": "synth-pop",
    # Post punk
    "post punk": "post-punk",
    "postpunk": "post-punk",
    # Post rock
    "post rock": "post-rock",
    "postrock": "post-rock",
    # New wave
    "newwave": "new wave",
    "new-wave": "new wave",
    # Pop punk
    "pop punk": "pop-punk",
    "poppunk": "pop-punk",
    # Dream pop
    "dream pop": "dream-pop",
    "dreampop": "dream-pop",
    # Brit pop
    "brit pop": "britpop",
    "brit-pop": "britpop",
    # Math rock
    "math rock": "math-rock",
    "mathrock": "math-rock",
    # Noise rock
    "noise rock": "noise-rock",
    "noiserock": "noise-rock",
    # Space rock
    "space rock": "space-rock",
    "spacerock": "space-rock",
    # Stoner rock
    "stoner rock": "stoner-rock",
    "stonerrock": "stoner-rock",
    # Nu metal
    "nu metal": "nu-metal",
    "numetal": "nu-metal",
    "nu-metal": "nu-metal",
    "nü metal": "nu-metal",
    "nü-metal": "nu-metal",
    # Art rock
    "art rock": "art-rock",
    "artrock": "art-rock",
    # Indie rock
    "indie rock": "indie-rock",
    "indierock": "indie-rock",
    # Indie pop
    "indie pop": "indie-pop",
    "indiepop": "indie-pop",
    # Alt country
    "alt country": "alt-country",
    "altcountry": "alt-country",
    "alternative country": "alt-country",
    # Power pop
    "power pop": "power-pop",
    "powerpop": "power-pop",
    # Garage rock
    "garage rock": "garage-rock",
    "garagerock": "garage-rock",
    # Hard rock
    "hard rock": "hard-rock",
    "hardrock": "hard-rock",
    # Acid house
    "acid house": "acid-house",
    "acidhouse": "acid-house",
    # Acid jazz
    "acid jazz": "acid-jazz",
    "acidjazz": "acid-jazz",
    # Electro house
    "electro house": "electro-house",
    "electrohouse": "electro-house",
    # Dark wave
    "dark wave": "darkwave",
    "dark-wave": "darkwave",
    # Cold wave
    "cold wave": "coldwave",
    "cold-wave": "coldwave",
    # Slow core
    "slow core": "slowcore",
    "slow-core": "slowcore",
    # Speed metal
    "speed metal": "speed-metal",
    "speedmetal": "speed-metal",
    # Power metal
    "power metal": "power-metal",
    "powermetal": "power-metal",
    # Thrash metal
    "thrash metal": "thrash-metal",
    "thrashmetal": "thrash-metal",
    # Death metal
    "death metal": "death-metal",
    "deathmetal": "death-metal",
    # Black metal
    "black metal": "black-metal",
    "blackmetal": "black-metal",
    # Doom metal
    "doom metal": "doom-metal",
    "doommetal": "doom-metal",
    # Heavy metal
    "heavy metal": "heavy-metal",
    "heavymetal": "heavy-metal",
    # Folk rock
    "folk rock": "folk-rock",
    "folkrock": "folk-rock",
    # Folk punk
    "folk punk": "folk-punk",
    "folkpunk": "folk-punk",
    # Psychedelic rock
    "psychedelic rock": "psychedelic-rock",
    "psychedelicrock": "psychedelic-rock",
    # Progressive rock
    "progressive rock": "progressive-rock",
    "progressiverock": "progressive-rock",
    "prog rock": "progressive-rock",
    "prog-rock": "progressive-rock",
    "progrock": "progressive-rock",
    # Progressive metal
    "progressive metal": "progressive-metal",
    "progressivemetal": "progressive-metal",
    "prog metal": "progressive-metal",
    "prog-metal": "progressive-metal",
    "progmetal": "progressive-metal",
    # Decade tags
    "00s": "2000s",
    "10s": "2010s",
    "20s": "2020s",
    "the 80s": "80s",
    "the 90s": "90s",
    "the 70s": "70s",
    "the 60s": "60s",
    # Common misspellings / abbreviations
    "uk": "british",
    # Singer-songwriter variants
    "singer songwriter": "singer-songwriter",
    "singersongwriter": "singer-songwriter",
}


def _normalize_unicode(text: str) -> str:
    """Normalize unicode characters to ASCII equivalents where possible.

    Handles smart quotes, accented characters, etc.
    """
    # Replace smart quotes and special apostrophes
    text = text.replace("\u2018", "'").replace("\u2019", "'")
    text = text.replace("\u201c", '"').replace("\u201d", '"')
    # Normalize unicode to NFKD (decompose) then strip combining chars
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(c for c in normalized if not unicodedata.combining(c))


def _normalize_separators(text: str) -> str:
    """Normalize common separators in genre strings.

    Converts various separator styles to a consistent form.
    """
    # Collapse multiple spaces
    text = re.sub(r"\s+", " ", text)
    # Normalize "and" variations used as connectors within genre names,
    # but don't touch "&" in known compound names (handled by ALIAS_MAP)
    return text


def _apply_hyphen_rules(text: str) -> str:
    """Apply hyphenation rules for compound genre names.

    Words like "post punk" become "post-punk" based on HYPHEN_PREFIXES.
    Already-hyphenated forms are preserved.
    """
    words = text.split()
    if len(words) < 2:
        return text

    result = []
    i = 0
    while i < len(words):
        word = words[i]
        # Check if this word is a known prefix and there's a next word
        if i + 1 < len(words) and word in HYPHEN_PREFIXES:
            # Join with hyphen
            result.append(f"{word}-{words[i + 1]}")
            i += 2
        else:
            result.append(word)
            i += 1

    return " ".join(result)


def normalize_genre(raw: str) -> str:
    """Normalize a raw genre string to its canonical form.

    Pipeline:
    1. Strip and lowercase
    2. Normalize unicode
    3. Normalize separators
    4. Check alias map (exact match on cleaned string)
    5. Apply hyphenation rules

    Args:
        raw: Raw genre string (e.g. "Post Punk", "  Rock & Roll ", "rnb")

    Returns:
        Canonical normalized form (e.g. "post-punk", "rock and roll", "r&b")
    """
    if not raw or not raw.strip():
        return ""

    # Step 1: Strip and lowercase
    text = raw.strip().lower()

    # Step 2: Normalize unicode
    text = _normalize_unicode(text)

    # Step 3: Normalize separators (collapse whitespace)
    text = _normalize_separators(text)

    # Step 4: Check alias map (before hyphenation so "post punk" → "post-punk" alias wins)
    if text in ALIAS_MAP:
        return ALIAS_MAP[text]

    # Step 5: Apply hyphenation rules
    text = _apply_hyphen_rules(text)

    # Step 6: Check alias map again (after hyphenation, e.g. "new-wave" → "new wave")
    if text in ALIAS_MAP:
        return ALIAS_MAP[text]

    return text


def build_normalization_map(raw_genres: list[str]) -> dict[str, str]:
    """Build a mapping from raw genre strings to their canonical forms.

    Args:
        raw_genres: List of raw genre strings from the database

    Returns:
        Dict mapping each raw genre to its normalized canonical form.
        Identity mappings (raw == canonical) are included.
    """
    mapping: dict[str, str] = {}
    for raw in raw_genres:
        canonical = normalize_genre(raw)
        if canonical:  # Skip empty results
            mapping[raw] = canonical
    return mapping


def find_duplicate_clusters(raw_genres: list[str]) -> dict[str, list[str]]:
    """Find groups of raw genres that normalize to the same canonical form.

    Useful for reviewing normalization results before applying to the database.
    Only returns clusters with 2+ variants.

    Args:
        raw_genres: List of raw genre strings from the database

    Returns:
        Dict mapping canonical genre to list of raw variants that map to it.
        Only includes entries where len(variants) >= 2.
    """
    # Group raw genres by their canonical form
    canonical_to_raws: dict[str, list[str]] = {}
    for raw in raw_genres:
        canonical = normalize_genre(raw)
        if not canonical:
            continue
        if canonical not in canonical_to_raws:
            canonical_to_raws[canonical] = []
        canonical_to_raws[canonical].append(raw)

    # Filter to only clusters with duplicates
    return {
        canonical: variants
        for canonical, variants in sorted(canonical_to_raws.items())
        if len(variants) >= 2
    }
