"""test_dedup.py - Tests TDD pour le module dedup."""
import pytest
from dedup import (
    normalize_text, signature_tokens, jaccard,
    parse_french_date, is_future_date, MOIS_FR,
)


class TestNormalizeText:
    def test_lowercase_no_accents(self):
        assert normalize_text("Coupe du Monde") == "coupe du monde"

    def test_strip_accents(self):
        assert normalize_text("Champ. de France 2026") == "champ de france 2026"

    def test_punctuation_to_space(self):
        assert normalize_text("Tour-de-Ski") == "tour de ski"

    def test_empty(self):
        assert normalize_text("") == ""

    def test_collapse_spaces(self):
        assert normalize_text("a   b") == "a b"


class TestSignatureTokens:
    def test_filters_stopwords(self):
        toks = signature_tokens("Coupe du Monde de Ski")
        assert "ski" not in toks  # ski est stopword
        assert "coupe" in toks
        assert "monde" in toks

    def test_filters_short(self):
        toks = signature_tokens("le ok")
        assert toks == set()

    def test_handles_accents(self):
        toks = signature_tokens("Championnats régionaux")
        assert "championnats" in toks
        assert "regionaux" in toks


class TestJaccard:
    def test_identical(self):
        assert jaccard({"a", "b"}, {"a", "b"}) == 1.0

    def test_disjoint(self):
        assert jaccard({"a"}, {"b"}) == 0.0

    def test_partial(self):
        assert jaccard({"a", "b"}, {"a", "c"}) == pytest.approx(1 / 3)

    def test_both_empty(self):
        assert jaccard(set(), set()) == 1.0

    def test_one_empty(self):
        assert jaccard({"a"}, set()) == 0.0
