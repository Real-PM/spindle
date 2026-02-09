"""
Unit tests for genre normalization engine.

No database required — tests pure Python normalization logic.
"""


from analysis.genre_normalize import (
    build_normalization_map,
    find_duplicate_clusters,
    normalize_genre,
)


class TestNormalizeGenre:
    """Test the normalize_genre() function."""

    def test_lowercases(self):
        assert normalize_genre("Rock") == "rock"
        assert normalize_genre("ELECTRONIC") == "electronic"
        assert normalize_genre("Jazz") == "jazz"

    def test_strips_whitespace(self):
        assert normalize_genre("  rock  ") == "rock"
        assert normalize_genre("\tpop\n") == "pop"

    def test_collapses_internal_whitespace(self):
        assert normalize_genre("classic  rock") == "classic rock"

    def test_empty_and_blank(self):
        assert normalize_genre("") == ""
        assert normalize_genre("   ") == ""

    def test_unicode_normalization(self):
        # Smart quotes
        assert normalize_genre("rock\u2019n\u2019roll") == "rock and roll"
        # The ALIAS_MAP catches "rock'n'roll" after unicode normalization

    def test_rnb_variants(self):
        assert normalize_genre("rnb") == "r&b"
        assert normalize_genre("RnB") == "r&b"
        assert normalize_genre("R and B") == "r&b"
        assert normalize_genre("Rhythm and Blues") == "r&b"

    def test_rock_and_roll_variants(self):
        assert normalize_genre("Rock & Roll") == "rock and roll"
        assert normalize_genre("Rock N Roll") == "rock and roll"
        assert normalize_genre("Rock n' Roll") == "rock and roll"
        assert normalize_genre("rock'n'roll") == "rock and roll"
        assert normalize_genre("rocknroll") == "rock and roll"

    def test_hip_hop_variants(self):
        assert normalize_genre("Hip Hop") == "hip-hop"
        assert normalize_genre("hip hop") == "hip-hop"
        assert normalize_genre("HipHop") == "hip-hop"
        assert normalize_genre("Hip-Hop") == "hip-hop"

    def test_trip_hop_variants(self):
        assert normalize_genre("Trip Hop") == "trip-hop"
        assert normalize_genre("trip hop") == "trip-hop"
        assert normalize_genre("triphop") == "trip-hop"

    def test_lofi_variants(self):
        assert normalize_genre("lo fi") == "lo-fi"
        assert normalize_genre("lofi") == "lo-fi"
        assert normalize_genre("Lo-Fi") == "lo-fi"
        assert normalize_genre("low fi") == "lo-fi"

    def test_drum_and_bass_variants(self):
        assert normalize_genre("Drum and Bass") == "drum & bass"
        assert normalize_genre("drum n bass") == "drum & bass"
        assert normalize_genre("DnB") == "drum & bass"
        assert normalize_genre("D&B") == "drum & bass"

    def test_post_punk_hyphenation(self):
        assert normalize_genre("Post Punk") == "post-punk"
        assert normalize_genre("post punk") == "post-punk"
        assert normalize_genre("Post-Punk") == "post-punk"
        assert normalize_genre("postpunk") == "post-punk"

    def test_synth_pop_hyphenation(self):
        assert normalize_genre("Synth Pop") == "synth-pop"
        assert normalize_genre("synthpop") == "synth-pop"
        assert normalize_genre("Synth-Pop") == "synth-pop"

    def test_dream_pop(self):
        assert normalize_genre("Dream Pop") == "dream-pop"
        assert normalize_genre("dreampop") == "dream-pop"

    def test_new_wave(self):
        assert normalize_genre("New Wave") == "new wave"
        assert normalize_genre("newwave") == "new wave"
        assert normalize_genre("new-wave") == "new wave"

    def test_britpop(self):
        assert normalize_genre("Brit Pop") == "britpop"
        assert normalize_genre("brit-pop") == "britpop"

    def test_progressive_rock_variants(self):
        assert normalize_genre("Progressive Rock") == "progressive-rock"
        assert normalize_genre("Prog Rock") == "progressive-rock"
        assert normalize_genre("prog-rock") == "progressive-rock"

    def test_metal_subgenres(self):
        assert normalize_genre("Death Metal") == "death-metal"
        assert normalize_genre("Black Metal") == "black-metal"
        assert normalize_genre("Doom Metal") == "doom-metal"
        assert normalize_genre("Nu Metal") == "nu-metal"
        assert normalize_genre("nü metal") == "nu-metal"

    def test_decade_normalization(self):
        assert normalize_genre("00s") == "2000s"
        assert normalize_genre("10s") == "2010s"
        assert normalize_genre("the 80s") == "80s"
        assert normalize_genre("the 90s") == "90s"
        # Already-canonical decades pass through
        assert normalize_genre("80s") == "80s"
        assert normalize_genre("90s") == "90s"

    def test_singer_songwriter(self):
        assert normalize_genre("Singer Songwriter") == "singer-songwriter"
        assert normalize_genre("singer songwriter") == "singer-songwriter"
        assert normalize_genre("Singer-Songwriter") == "singer-songwriter"

    def test_passthrough_for_unknown_genres(self):
        """Genres not in ALIAS_MAP or HYPHEN_PREFIXES just get lowercased."""
        assert normalize_genre("Ambient") == "ambient"
        assert normalize_genre("Baroque") == "baroque"
        assert normalize_genre("Female Vocalists") == "female vocalists"

    def test_hyphen_prefix_joining(self):
        """Words in HYPHEN_PREFIXES get joined with next word via hyphen."""
        assert normalize_genre("acid jazz") == "acid-jazz"
        assert normalize_genre("neo soul") == "neo-soul"
        assert normalize_genre("proto punk") == "proto-punk"
        assert normalize_genre("space rock") == "space-rock"

    def test_already_canonical(self):
        """Already-normalized genres should pass through unchanged."""
        assert normalize_genre("rock") == "rock"
        assert normalize_genre("electronic") == "electronic"
        assert normalize_genre("post-punk") == "post-punk"
        assert normalize_genre("r&b") == "r&b"


class TestBuildNormalizationMap:
    """Test build_normalization_map()."""

    def test_basic_mapping(self):
        raw = ["Rock", "rock", "ROCK", "Post Punk", "post-punk"]
        result = build_normalization_map(raw)
        assert result["Rock"] == "rock"
        assert result["rock"] == "rock"
        assert result["ROCK"] == "rock"
        assert result["Post Punk"] == "post-punk"
        assert result["post-punk"] == "post-punk"

    def test_empty_list(self):
        assert build_normalization_map([]) == {}

    def test_skips_empty_strings(self):
        result = build_normalization_map(["rock", "", "  "])
        assert len(result) == 1
        assert result["rock"] == "rock"


class TestFindDuplicateClusters:
    """Test find_duplicate_clusters()."""

    def test_finds_duplicates(self):
        raw = ["Rock", "rock", "ROCK", "Pop", "Jazz"]
        clusters = find_duplicate_clusters(raw)
        assert "rock" in clusters
        assert set(clusters["rock"]) == {"Rock", "rock", "ROCK"}
        # Pop and Jazz have no duplicates
        assert "pop" not in clusters
        assert "jazz" not in clusters

    def test_alias_clusters(self):
        raw = ["Hip Hop", "HipHop", "hip-hop", "hiphop"]
        clusters = find_duplicate_clusters(raw)
        assert "hip-hop" in clusters
        assert len(clusters["hip-hop"]) == 4

    def test_no_duplicates(self):
        raw = ["rock", "pop", "jazz"]
        clusters = find_duplicate_clusters(raw)
        assert clusters == {}

    def test_empty_input(self):
        assert find_duplicate_clusters([]) == {}
