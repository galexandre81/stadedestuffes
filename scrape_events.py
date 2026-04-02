"""
scrape_events.py
Scraper d'événements pour la table `events` (Supabase).
Source  : calendrier officiel FFS — https://ffs.fr/calendrier/
Cible   : compétitions à Prémanon / Les Tuffes (toutes disciplines nordiques)

Structure HTML confirmée (avril 2026) :
  <div class="items-el">
    <div class="el-date cbo-date">
      <span class="date-day">04-05</span><br/>Jan.<br/>2025
    </div>
    <div class="el-title">
      <h3 class="title-text">SAMSE BIATHLON NATIONAL TOUR 3 (LES TUFFES)</h3>
      <div class="title-type">FFS-BIATH-NA</div>
    </div>
    <div class="el-details">
      <div class="details-coords">...</div>
    </div>
  </div>
"""

import os
import re
import logging
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Supabase ───────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {"User-Agent": "stadedestuffes-bot/1.0"}
TIMEOUT = 15

CALENDRIER_BASE_URL = "https://ffs.fr/calendrier/"

# ── Mots-clés lieu ─────────────────────────────────────────────────────────────
KEYWORDS_LIEU = [
    "tuffes", "prémanon", "premanon", "cnsnmm", "stade nordique",
]

# ── Disciplines FFS nordiques (IDs vérifiés sur ffs.fr/calendrier/) ────────────
FFS_DISCIPLINES = [
    (2,  "Biathlon"),
    (3,  "Combiné nordique"),
    (4,  "Ski de fond"),
    (7,  "Saut à ski"),
]

# ── Mois abrégés français ──────────────────────────────────────────────────────
MOIS_ABBR = {
    "jan": 1, "fév": 2, "fev": 2, "mar": 3, "avr": 4,
    "mai": 5, "juin": 6, "juil": 7,
    "aou": 8, "aoû": 8, "août": 8,
    "sep": 9, "oct": 10, "nov": 11,
    "déc": 12, "dec": 12,
}

# ── Détection de sport depuis le texte ────────────────────────────────────────
SPORT_MAP = [
    ("biath",          "Biathlon"),
    ("saut",           "Saut à ski"),
    ("tremplin",       "Saut à ski"),
    ("combiné",        "Combiné nordique"),
    ("combine",        "Combiné nordique"),
    ("nordic combined","Combiné nordique"),
    ("fond",           "Ski de fond"),
    ("cross-country",  "Ski de fond"),
    ("skiathlon",      "Ski de fond"),
    ("sprint",         "Ski de fond"),
    ("ffs-fond",       "Ski de fond"),
    ("ffs-biath",      "Biathlon"),
    ("ffs-saut",       "Saut à ski"),
    ("ffs-cn",         "Combiné nordique"),
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def is_lieu_tuffes(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in KEYWORDS_LIEU)


def detect_sport(text: str, default_sport: str) -> str:
    lower = text.lower()
    for kw, sport in SPORT_MAP:
        if kw in lower:
            return sport
    return default_sport


def parse_ffs_date(date_div) -> tuple[str | None, str | None]:
    """
    Parse le bloc date d'un div.el-date du calendrier FFS.

    Structure HTML :
      <span class="date-day">04-05</span><br/>Jan.<br/>2025

    Gère :
      - jour unique   : "12"   → date_start = date_end = YYYY-MM-12
      - plage de jours: "04-05" → date_start = YYYY-MM-04, date_end = YYYY-MM-05

    Retourne (date_start, date_end) en format YYYY-MM-DD.
    date_end est None si événement sur un seul jour.
    """
    if not date_div:
        return None, None

    full_text = date_div.get_text(separator=" ", strip=True)

    # Année (20XX)
    year_m = re.search(r'20\d{2}', full_text)
    if not year_m:
        return None, None
    year = int(year_m.group(0))

    # Mois (cherche une abréviation française dans le texte)
    month = None
    lower = full_text.lower()
    for key, num in MOIS_ABBR.items():
        if key in lower:
            month = num
            break
    if not month:
        return None, None

    # Jours depuis span.date-day
    day_span = date_div.select_one("span.date-day")
    if not day_span:
        return None, None
    day_text = day_span.get_text(strip=True)  # ex : "04", "04-05", "17-19"

    # Plage de jours ?
    range_m = re.match(r'^(\d{1,2})-(\d{1,2})$', day_text)
    if range_m:
        day_start = int(range_m.group(1))
        day_end   = int(range_m.group(2))
    else:
        try:
            day_start = int(day_text)
            day_end   = day_start
        except ValueError:
            return None, None

    try:
        date_start = f"{year}-{month:02d}-{day_start:02d}"
        date_end   = f"{year}-{month:02d}-{day_end:02d}"
        # Validation
        datetime.strptime(date_start, "%Y-%m-%d")
        datetime.strptime(date_end, "%Y-%m-%d")
    except ValueError:
        return None, None

    return date_start, (date_end if date_end != date_start else None)


def upsert_event(row: dict) -> bool:
    """Insère l'événement s'il n'existe pas déjà (déduplication sur title + date_start)."""
    try:
        existing = (
            sb.table("events")
            .select("id")
            .eq("title", row["title"])
            .eq("date_start", row["date_start"] or "")
            .limit(1)
            .execute()
        )
        if existing.data:
            return False
        sb.table("events").insert(row).execute()
        return True
    except Exception as exc:
        log.error("   ✗ upsert ÉCHOUÉ pour '%s' : %s", row["title"][:60], exc)
        return False


def season_start_date() -> str:
    """
    Retourne la date de début de saison au format DD%2FMM%2FYYYY.
    La saison démarre en septembre ; si on est avant septembre,
    on remonte à septembre de l'année précédente.
    """
    now = datetime.now(timezone.utc)
    year = now.year if now.month >= 9 else now.year - 1
    return f"01%2F09%2F{year}"


# ── Scraper principal ──────────────────────────────────────────────────────────

def scrape_ffs_calendrier() -> int:
    """
    Scrape le calendrier FFS pour toutes les disciplines nordiques.
    Parcourt toutes les pages de résultats.
    N'insère que les événements mentionnant Prémanon / Les Tuffes.
    """
    inserted_total = 0
    date_du = season_start_date()

    for disc_id, default_sport in FFS_DISCIPLINES:
        inserted_disc = 0
        page = 1

        while True:
            url = (
                f"{CALENDRIER_BASE_URL}"
                f"?discipline={disc_id}"
                f"&date_du={date_du}"
                f"&filters=1"
                f"&page_number={page}"
            )
            try:
                resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.content, "html.parser")
            except Exception as exc:
                log.warning("   FFS disc=%d page=%d inaccessible : %s", disc_id, page, exc)
                break

            items = soup.select("div.items-el")
            if not items:
                break  # Plus de résultats

            for item in items:
                # Titre
                title_el = item.select_one("h3.title-text")
                if not title_el:
                    continue
                title = title_el.get_text(separator=" ", strip=True)
                if not title:
                    continue

                # Texte complet de l'item (titre + coordonnées + tableau)
                full_text = item.get_text(separator=" ", strip=True)

                # Filtre : doit mentionner Tuffes/Prémanon dans le titre ou les détails
                if not is_lieu_tuffes(title) and not is_lieu_tuffes(full_text):
                    continue

                # Date
                date_div = item.select_one(".el-date")
                date_start, date_end = parse_ffs_date(date_div)
                if not date_start:
                    log.debug("   date non parsée pour : %s", title[:60])
                    continue

                # Type d'épreuves (ex : "FFS-BIATH-NA")
                type_el = item.select_one(".title-type")
                type_txt = type_el.get_text(separator=" ", strip=True) if type_el else ""

                sport = detect_sport(f"{title} {type_txt}", default_sport)

                row = {
                    "title":         title[:255],
                    "sport":         sport,
                    "date_start":    date_start,
                    "date_end":      date_end,
                    "public_access": None,
                    "has_catering":  None,
                    "notes":         type_txt[:500] if type_txt else None,
                    "source_name":   "FFS calendrier",
                    "source_url":    CALENDRIER_BASE_URL,
                    "status":        "published",
                    "source_type":   "scraped",
                }

                if upsert_event(row):
                    log.info(
                        "   + [%s] %s  (%s%s)",
                        sport,
                        title[:70],
                        date_start,
                        f" → {date_end}" if date_end else "",
                    )
                    inserted_disc += 1

            # Pagination : continue tant qu'il y a une page suivante
            if not soup.select_one("a.next.page-numbers"):
                break
            page += 1
            time.sleep(0.5)

        log.info("FFS calendrier [%s] : %d événement(s) ajouté(s)", default_sport, inserted_disc)
        inserted_total += inserted_disc
        time.sleep(1)

    return inserted_total


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    log.info("=== scrape_events.py démarré ===")
    total = scrape_ffs_calendrier()
    log.info("=== Terminé : %d événement(s) ajouté(s) ===", total)


if __name__ == "__main__":
    main()
