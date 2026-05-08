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
