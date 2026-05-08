"""dedup.py - Helpers de déduplication d'événements."""
import re
import unicodedata
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)

STOPWORDS = set()  # à remplir
MOIS_FR = {}  # à remplir


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def signature_tokens(title: str) -> set:
    ...


def jaccard(a: set, b: set) -> float:
    ...


def parse_french_date(text: str):
    ...


def is_future_date(date_str: str, tolerate_today: bool = True) -> bool:
    ...


def find_duplicate(sb, candidate_row: dict, similarity_threshold: float = 0.5):
    ...


def merge_into_existing(sb, existing: dict, new_row: dict) -> bool:
    ...


def upsert_event(sb, row: dict) -> str:
    ...
