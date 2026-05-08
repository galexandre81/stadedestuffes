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

import feedparser
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

from dedup import (
    upsert_event as dedup_upsert_event,
    parse_french_date,
    is_future_date,
    MOIS_FR,
)

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


# ── Helpers nouveaux scrapers ──────────────────────────────────────────────────

def is_at_tuffes(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in (
        "tuffes", "prémanon", "premanon", "cnsnmm", "jason lamy",
        "stade nordique"
    ))


def _classify_status(outcome: str, added_counter: int, merged_counter: int, error_counter: int) -> tuple[int, int, int]:
    """Helper pour incrémenter compteurs depuis un résultat dedup_upsert_event."""
    if outcome == "added":
        added_counter += 1
    elif outcome == "merged":
        merged_counter += 1
    elif outcome == "error":
        error_counter += 1
    return added_counter, merged_counter, error_counter


# ── CLUB_EVENT_PAGES ──────────────────────────────────────────────────────────
CLUB_EVENT_PAGES = [
    {"name": "Haut Jura Ski",  "url": "https://www.hautjuraski.fr/evenements", "fallback_url": "https://www.hautjuraski.fr/agenda"},
    {"name": "Haut-Jura Léman", "url": "https://www.hautjuraleman.com/events"},
    {"name": "SC du Grandvaux", "url": "https://www.scdugrandvaux.fr/"},
    {"name": "SC Bois d'Amont", "url": "https://www.scboisdamont.com/", "fallback_url": "https://skiclubboisdamont.clubffs.fr/"},
]


# ── 1. CNSNMM (officiel École Nationale Ski Montagne) ─────────────────────────
def scrape_cnsnmm() -> tuple[int, int, int]:
    """Scrape l'agenda du CNSNMM (officiel École Nationale Ski Montagne)."""
    url = "https://www.ensm.sports.gouv.fr/stade-nordique-des-tuffes-cnsnmm/"
    added = merged = errors = 0
    try:
        resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
    except Exception as exc:
        log.warning("   CNSNMM inaccessible : %s", exc)
        return 0, 0, 1

    seen_titles: set[str] = set()
    for a in soup.find_all("a"):
        text = a.get_text(separator=" ", strip=True)
        if not text or len(text) < 10 or len(text) > 250:
            continue
        date_start, date_end = parse_french_date(text)
        if not date_start:
            continue
        if not is_future_date(date_start):
            continue

        # Titre = texte minus la date (heuristique : enlever du préfixe la première date trouvée)
        title = text
        # Trouver la date dans le texte et la retirer
        import re as _re
        date_pattern = _re.compile(
            r"(?:du\s+)?(\d{1,2})\s+(?:au\s+(\d{1,2})\s+)?"
            r"(janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre)"
            r"\s+(\d{4})",
            _re.IGNORECASE,
        )
        title = date_pattern.sub("", title).strip(" -:,;")
        # Fallback si vide
        if not title or len(title) < 5:
            title = text[:120]

        if title in seen_titles:
            continue
        seen_titles.add(title)

        href = a.get("href") or url
        if href.startswith("/"):
            href = "https://www.ensm.sports.gouv.fr" + href
        elif not href.startswith("http"):
            href = url

        row = {
            "title": title[:255],
            "sport": "Nordique",
            "date_start": date_start,
            "date_end": date_end,
            "public_access": True,
            "has_catering": None,
            "notes": "Source officielle CNSNMM",
            "source_name": "CNSNMM",
            "source_url": href,
            "status": "published",
            "source_type": "scraped",
        }
        outcome = dedup_upsert_event(sb, row)
        added, merged, errors = _classify_status(outcome, added, merged, errors)
        if outcome == "added":
            log.info("   + [CNSNMM] %s (%s)", title[:70], date_start)

    return added, merged, errors


# ── 2. Cyclo Haut Jura ────────────────────────────────────────────────────────
def scrape_cyclo_haut_jura() -> tuple[int, int, int]:
    """Scrape la page Cyclo Haut Jura : événement unique détecté via une date FR."""
    url = "https://www.cyclohautjura.com/"
    try:
        resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
    except Exception as exc:
        log.warning("   Cyclo Haut Jura inaccessible : %s", exc)
        return 0, 0, 1

    page_text = soup.get_text(separator=" ", strip=True)
    date_start, date_end = parse_french_date(page_text)
    if not date_start:
        log.info("   Cyclo Haut Jura : aucune date détectée")
        return 0, 0, 0
    if not is_future_date(date_start):
        log.info("   Cyclo Haut Jura : date passée (%s)", date_start)
        return 0, 0, 0

    year = date_start[:4]
    title = f"Cyclo Haut Jura {year}"
    notes = ("Cyclo Haut Jura — organisée par Jura Ski Events. "
             "Voir aussi le Samse Tour (https://www.samsetour.com/).")

    row = {
        "title": title[:255],
        "sport": "Cyclisme",
        "date_start": date_start,
        "date_end": date_end,
        "public_access": True,
        "has_catering": True,
        "notes": notes,
        "source_name": "Cyclo Haut Jura",
        "source_url": url,
        "status": "published",
        "source_type": "scraped",
    }
    outcome = dedup_upsert_event(sb, row)
    added, merged, errors = _classify_status(outcome, 0, 0, 0)
    if outcome == "added":
        log.info("   + [Cyclo] %s (%s)", title, date_start)
    return added, merged, errors


# ── 3. Transju' (RSS) ─────────────────────────────────────────────────────────
def scrape_transju() -> tuple[int, int, int]:
    """Scrape le feed Transju' pour repérer la Transju'Jeunes (au stade des Tuffes)."""
    feed_url = "https://www.latransju.com/feed/"
    added = merged = errors = 0
    try:
        resp = requests.get(feed_url, timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as exc:
        log.warning("   Transju feed inaccessible : %s", exc)
        return 0, 0, 1

    if not feed.entries:
        log.info("   Transju : feed vide")
        return 0, 0, 0

    seen: set[str] = set()
    for entry in feed.entries:
        title = getattr(entry, "title", "").strip()
        summary_raw = getattr(entry, "summary", "") or ""
        summary = BeautifulSoup(summary_raw, "html.parser").get_text(" ", strip=True)
        full = f"{title} {summary}".lower()

        # Doit être une Transju'Jeunes ET mentionner les Tuffes/Prémanon
        if not any(kw in full for kw in ("transju'jeunes", "transjujeunes", "transju jeunes")):
            continue
        if not is_at_tuffes(full):
            continue

        date_start, date_end = parse_french_date(full)
        if not date_start:
            continue
        if not is_future_date(date_start):
            continue

        year = date_start[:4]
        norm_title = f"Transju'Jeunes {year}"
        if norm_title in seen:
            continue
        seen.add(norm_title)

        url = getattr(entry, "link", feed_url)
        row = {
            "title": norm_title[:255],
            "sport": "Ski de fond",
            "date_start": date_start,
            "date_end": date_end,
            "public_access": True,
            "has_catering": None,
            "notes": (summary[:400] + "…") if len(summary) > 400 else summary,
            "source_name": "Transju'",
            "source_url": url,
            "status": "published",
            "source_type": "scraped",
        }
        outcome = dedup_upsert_event(sb, row)
        added, merged, errors = _classify_status(outcome, added, merged, errors)
        if outcome == "added":
            log.info("   + [Transju] %s (%s)", norm_title, date_start)

    return added, merged, errors


# ── 4. Jura Tourism ───────────────────────────────────────────────────────────
def scrape_jura_tourism() -> tuple[int, int, int]:
    """Scrape l'agenda jura-tourism.com pour repérer les événements au stade."""
    base = "https://www.jura-tourism.com"
    list_url = f"{base}/agenda/"
    added = merged = errors = 0
    try:
        resp = requests.get(list_url, timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
    except Exception as exc:
        log.warning("   Jura Tourisme inaccessible : %s", exc)
        return 0, 0, 1

    detail_links: list[str] = []
    seen_links: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/agenda/" not in href:
            continue
        if href.endswith("/agenda/") or href.endswith("/agenda"):
            continue
        # URL absolue
        if href.startswith("/"):
            href = base + href
        elif not href.startswith("http"):
            continue
        if href in seen_links:
            continue
        seen_links.add(href)
        detail_links.append(href)
        if len(detail_links) >= 60:
            break

    log.info("   Jura Tourisme : %d liens d'agenda à inspecter", len(detail_links))

    for url in detail_links:
        try:
            r = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
            r.raise_for_status()
            detail = BeautifulSoup(r.content, "html.parser")
        except Exception as exc:
            log.debug("   Jura Tourisme detail KO : %s — %s", url[:60], exc)
            errors += 1
            continue

        detail_text = detail.get_text(separator=" ", strip=True)[:5000]
        if not is_at_tuffes(detail_text):
            time.sleep(0.4)
            continue

        h1 = detail.find("h1")
        if not h1:
            time.sleep(0.4)
            continue
        title = h1.get_text(separator=" ", strip=True)
        if not title or len(title) < 5:
            time.sleep(0.4)
            continue

        date_start, date_end = parse_french_date(detail_text)
        if not date_start:
            time.sleep(0.4)
            continue
        if not is_future_date(date_start):
            time.sleep(0.4)
            continue

        row = {
            "title": title[:255],
            "sport": "Nordique",
            "date_start": date_start,
            "date_end": date_end,
            "public_access": True,
            "has_catering": None,
            "notes": "Source : Jura Tourisme",
            "source_name": "Jura Tourisme",
            "source_url": url,
            "status": "published",
            "source_type": "scraped",
        }
        outcome = dedup_upsert_event(sb, row)
        added, merged, errors = _classify_status(outcome, added, merged, errors)
        if outcome == "added":
            log.info("   + [JuraTourisme] %s (%s)", title[:70], date_start)
        time.sleep(0.4)

    return added, merged, errors


# ── 5. Clubs locaux (statut 'pending' pour validation admin) ──────────────────
def _looks_like_event_link(text: str, href: str) -> bool:
    """Heuristique : lien d'événement = href contient /event ou /agenda,
    OU titre contient un mois français, OU mot-clé de compétition."""
    if not text or not href:
        return False
    href_l = href.lower()
    text_l = text.lower()
    if "/event" in href_l or "/agenda" in href_l:
        return True
    for mois in MOIS_FR.keys():
        if mois in text_l:
            return True
    for kw in ("compétition", "competition", "course", "challenge", "trophée",
               "trophee", "championnat", "open", "samse", "biathlon", "fond",
               "nordique", "saut"):
        if kw in text_l:
            return True
    return False


def scrape_club_events(club: dict) -> tuple[int, int, int]:
    """Scrape la page événements d'un club : 25 candidats max, status='pending'."""
    added = merged = errors = 0
    name = club["name"]

    # Tenter URL primaire puis fallback
    urls = [club["url"]]
    if club.get("fallback_url"):
        urls.append(club["fallback_url"])

    soup = None
    used_url = None
    for u in urls:
        try:
            resp = requests.get(u, timeout=TIMEOUT, headers=HEADERS)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
            used_url = u
            break
        except Exception as exc:
            log.debug("   %s url %s KO : %s", name, u, exc)
            continue

    if not soup:
        log.warning("   %s : toutes les URLs ont échoué", name)
        return 0, 0, 1

    candidates: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    for a in soup.find_all("a", href=True):
        text = a.get_text(separator=" ", strip=True)
        if not text or len(text) < 15 or len(text) > 200:
            continue
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        # URL absolue
        if href.startswith("/"):
            from urllib.parse import urlparse
            parsed = urlparse(used_url)
            href = f"{parsed.scheme}://{parsed.netloc}{href}"
        elif not href.startswith("http"):
            continue
        if href in seen_urls:
            continue
        if not _looks_like_event_link(text, href):
            continue
        seen_urls.add(href)
        candidates.append((text, href))
        if len(candidates) >= 25:
            break

    log.info("   %s : %d candidats événements", name, len(candidates))

    for title, url in candidates:
        # Fetch détail
        try:
            r = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
            r.raise_for_status()
            detail = BeautifulSoup(r.content, "html.parser")
        except Exception as exc:
            log.debug("   %s detail KO : %s — %s", name, url[:60], exc)
            errors += 1
            continue

        detail_text = detail.get_text(separator=" ", strip=True)[:5000]
        if not is_at_tuffes(detail_text + " " + title):
            time.sleep(0.4)
            continue

        date_start, date_end = parse_french_date(detail_text + " " + title)
        if not date_start:
            time.sleep(0.4)
            continue
        if not is_future_date(date_start):
            time.sleep(0.4)
            continue

        row = {
            "title": title[:255],
            "sport": "Nordique",
            "date_start": date_start,
            "date_end": date_end,
            "public_access": None,
            "has_catering": None,
            "notes": f"Source club : {name} (à valider)",
            "source_name": name,
            "source_url": url,
            "status": "pending",
            "source_type": "scraped",
        }
        outcome = dedup_upsert_event(sb, row)
        added, merged, errors = _classify_status(outcome, added, merged, errors)
        if outcome == "added":
            log.info("   + [%s/pending] %s (%s)", name, title[:70], date_start)
        time.sleep(0.4)

    return added, merged, errors


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    log.info("=== scrape_events.py démarré ===")
    total_added = 0
    total_merged = 0
    total_errors = 0

    # 1) FFS (comportement existant inchangé)
    ffs_inserted = scrape_ffs_calendrier()
    total_added += ffs_inserted

    # 2) Nouveaux scrapers
    log.info("→ Scraping CNSNMM agenda (officiel)")
    a, m, e = scrape_cnsnmm()
    total_added += a; total_merged += m; total_errors += e
    log.info("   CNSNMM : %d ajoutés, %d fusionnés, %d erreurs", a, m, e)
    time.sleep(2)

    log.info("→ Scraping Cyclo Haut Jura")
    a, m, e = scrape_cyclo_haut_jura()
    total_added += a; total_merged += m; total_errors += e
    log.info("   Cyclo : %d ajoutés, %d fusionnés, %d erreurs", a, m, e)
    time.sleep(2)

    log.info("→ Scraping Transju'")
    a, m, e = scrape_transju()
    total_added += a; total_merged += m; total_errors += e
    log.info("   Transju : %d ajoutés, %d fusionnés, %d erreurs", a, m, e)
    time.sleep(2)

    log.info("→ Scraping Jura Tourisme")
    a, m, e = scrape_jura_tourism()
    total_added += a; total_merged += m; total_errors += e
    log.info("   JuraTourisme : %d ajoutés, %d fusionnés, %d erreurs", a, m, e)
    time.sleep(2)

    log.info("→ Scraping clubs locaux (auto-pending)")
    for club in CLUB_EVENT_PAGES:
        log.info("   • %s", club["name"])
        a, m, e = scrape_club_events(club)
        total_added += a; total_merged += m; total_errors += e
        time.sleep(2)

    log.info("=== Terminé : %d ajoutés, %d fusionnés, %d erreurs ===",
             total_added, total_merged, total_errors)


if __name__ == "__main__":
    main()
