"""dedup.py - Helpers de déduplication d'événements."""
import re
import unicodedata
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)

STOPWORDS = {
    "de", "du", "des", "le", "la", "les", "un", "une", "et", "à", "au", "aux",
    "en", "sur", "pour", "par", "dans", "ski", "nordique", "stade", "tuffes",
    "prémanon", "premanon", "france", "jura", "edition", "édition",
    "competition", "compétition", "course", "open", "challenge", "tour",
    "épreuve", "epreuve",
}

MOIS_FR = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11,
    "décembre": 12, "decembre": 12,
}

RE_DATE_FR_RANGE = re.compile(
    r"(?:du\s+)?(\d{1,2})\s+(?:au\s+(\d{1,2})\s+)?"
    r"(janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre)"
    r"\s+(\d{4})",
    re.IGNORECASE,
)
RE_DATE_NUM = re.compile(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b")


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def signature_tokens(title: str) -> set:
    norm = normalize_text(title)
    tokens = norm.split()
    return {t for t in tokens if len(t) >= 3 and t not in STOPWORDS}


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def parse_french_date(text: str):
    """Returns (date_start, date_end) tuple, both YYYY-MM-DD or (None, None)."""
    if not text:
        return None, None
    text_lower = text.lower()
    m = RE_DATE_FR_RANGE.search(text_lower)
    if m:
        d1, d2, mois_name, annee = m.group(1), m.group(2), m.group(3), m.group(4)
        if mois_name not in MOIS_FR:
            return None, None
        mois = MOIS_FR[mois_name]
        date_start = f"{annee}-{mois:02d}-{int(d1):02d}"
        date_end = f"{annee}-{mois:02d}-{int(d2):02d}" if d2 else None
        return date_start, date_end
    m = RE_DATE_NUM.search(text_lower)
    if m:
        d, mois, annee = m.group(1), m.group(2), m.group(3)
        try:
            return f"{annee}-{int(mois):02d}-{int(d):02d}", None
        except ValueError:
            return None, None
    return None, None


def is_future_date(date_str: str, tolerate_today: bool = True) -> bool:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return False
    today = datetime.now().date()
    return d >= today if tolerate_today else d > today


def find_duplicate(sb, candidate_row: dict, similarity_threshold: float = 0.5):
    try:
        d = datetime.strptime(candidate_row["date_start"], "%Y-%m-%d").date()
    except (ValueError, KeyError, TypeError):
        return None
    d_min = (d - timedelta(days=1)).isoformat()
    d_max = (d + timedelta(days=1)).isoformat()
    try:
        result = sb.table("events").select(
            "id, title, date_start, date_end, source_name, source_url, "
            "notes, has_catering, public_access, sport, additional_sources, status"
        ).gte("date_start", d_min).lte("date_start", d_max).execute()
    except Exception as exc:
        log.warning("dedup: erreur lecture events : %s", exc)
        return None
    if not result.data:
        return None
    candidate_tokens = signature_tokens(candidate_row.get("title", ""))
    if not candidate_tokens:
        return None
    best_match, best_score = None, 0.0
    for existing in result.data:
        existing_tokens = signature_tokens(existing.get("title", ""))
        score = jaccard(candidate_tokens, existing_tokens)
        if score > best_score:
            best_score = score
            best_match = existing
    return best_match if best_score >= similarity_threshold else None


def merge_into_existing(sb, existing: dict, new_row: dict) -> bool:
    updates = {}
    new_source_name = new_row.get("source_name")
    new_source_url = new_row.get("source_url")
    existing_main_source = existing.get("source_name")
    existing_extras = existing.get("additional_sources") or []
    already_listed = (
        new_source_name == existing_main_source
        or any(s.get("source_name") == new_source_name for s in existing_extras)
    )
    if new_source_name and not already_listed:
        updates["additional_sources"] = existing_extras + [
            {"source_name": new_source_name, "source_url": new_source_url}
        ]
    for field in ("notes", "has_catering", "public_access", "date_end"):
        if existing.get(field) in (None, "") and new_row.get(field) not in (None, ""):
            updates[field] = new_row[field]
    if not updates:
        return False
    try:
        sb.table("events").update(updates).eq("id", existing["id"]).execute()
        log.info("   ⤴ enrichi event #%s avec %d champs depuis '%s'",
                 existing["id"], len(updates), new_source_name)
        return True
    except Exception as exc:
        log.error("   ✗ enrichissement échoué pour event #%s : %s", existing["id"], exc)
        return False


def upsert_event(sb, row: dict) -> str:
    """Returns 'added', 'merged', 'unchanged' or 'error'."""
    try:
        existing = find_duplicate(sb, row)
        if existing:
            changed = merge_into_existing(sb, existing, row)
            return "merged" if changed else "unchanged"
        sb.table("events").insert(row).execute()
        return "added"
    except Exception as exc:
        log.error("   ✗ upsert event ÉCHOUÉ pour '%s' : %s",
                  row.get("title", "?")[:60], exc)
        return "error"
