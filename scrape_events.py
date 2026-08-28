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

import calendar
import os
import re
import html
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

# ── Sources complémentaires (saison 2026-2027 et suivantes) ───────────────────
# Agenda national des cyclosportives — départs du Cyclo Haut-Jura aux Tuffes.
VELO_CYCLOSPORT_URL = "https://www.velo-cyclosport.com/agenda/index.php"
# Calendrier national FFS relayé par Sports Infos / Ski-Nordique.net
# (étapes SAMSE National Tour disputées à Prémanon).
SKI_NORDIQUE_CALENDAR_URL = (
    "https://www.ski-nordique.net/biathlon-decouvrez-le-calendrier-des-epreuves-"
    "nationales-pour-la-saison-2026-2027.6749329-72348.html"
)
# Pages événement de La Transju' (départs Transju'Jeunes au stade des Tuffes).
TRANSJU_EVENT_PAGES = [
    {"url": "https://www.latransju.com/la-transjeunes/", "sport": "Ski de fond",
     "label": "La Transju'Jeunes"},
    {"url": "https://www.latransju.com/la-transjutrails/", "sport": "Trail",
     "label": "La Transju'Trails"},
]
# Organisation locale de la Coupe du monde FIS (Tour de Ski 2027).
WORLD_CUP_ROUSSES_URLS = [
    "https://www.worldcupstationdesrousses.fr/",
    "https://www.worldcupstationdesrousses.fr/tour-de-ski-2027-les-rousses/",
]

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


def parse_ffs_table_dates(item) -> tuple[str | None, str | None]:
    """
    Extrait les dates depuis le TABLEAU de détail de l'item FFS.

    Chaque item du calendrier liste ses épreuves avec une date machine
    au format JJ/MM/AA (ex : « 05/07/26 »). C'est bien plus fiable que
    l'en-tête abrégé en français (« Jui. », « Aoû. »…) qui, lui, dépend
    d'un dictionnaire d'abréviations forcément incomplet — c'est ce qui
    faisait perdre les événements d'été comme le SAMSE SUMMER TOUR.

    Retourne (date_start, date_end) = min et max des dates trouvées.
    date_end vaut None pour un événement d'un seul jour.
    """
    text = item.get_text(" ", strip=True)
    dates: set = set()
    for d, m, y in re.findall(r"\b(\d{2})/(\d{2})/(\d{2,4})\b", text):
        year = int(y)
        if year < 100:
            year += 2000
        try:
            dates.add(datetime(year, int(m), int(d)).date())
        except ValueError:
            continue
    if not dates:
        return None, None
    ordered = sorted(dates)
    date_start = ordered[0].strftime("%Y-%m-%d")
    date_end = ordered[-1].strftime("%Y-%m-%d")
    return date_start, (date_end if date_end != date_start else None)


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
        merged_disc = 0
        error_disc = 0
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

                # Date : d'abord le tableau de détail (JJ/MM/AA, fiable),
                # sinon repli sur l'en-tête abrégé (« 05 Jui. 2026 »).
                date_start, date_end = parse_ffs_table_dates(item)
                if not date_start:
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

                # La source FFS passe par la meme deduplication que les autres
                # sources : similarite de titre, chevauchement d'intervalles et
                # garde-fou sur le sport. L'ancienne egalite stricte
                # (title, date_start) creait un doublon par jour de competition
                # (Championnats de France, SAMSE National Tour).
                outcome = upsert_extended(row)
                if outcome == "added":
                    log.info(
                        "   + [%s] %s  (%s%s)",
                        sport,
                        title[:70],
                        date_start,
                        f" → {date_end}" if date_end else "",
                    )
                    inserted_disc += 1
                elif outcome == "merged":
                    log.info("   ⤴ [%s] %s  (fusionné dans un événement existant)",
                             sport, title[:70])
                    merged_disc += 1
                elif outcome == "error":
                    error_disc += 1

            # Pagination : continue tant qu'il y a une page suivante
            if not soup.select_one("a.next.page-numbers"):
                break
            page += 1
            time.sleep(0.5)

        log.info("FFS calendrier [%s] : %d ajouté(s), %d fusionné(s), %d erreur(s)",
                 default_sport, inserted_disc, merged_disc, error_disc)
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
            # La page ENSM mêle actualités (« Portail Montagne ») et événements
            # au stade (sportifs OU non). On ne peut pas trancher le lieu de
            # façon fiable → statut 'pending' pour validation admin.
            "notes": "Source officielle CNSNMM (à valider : est-ce bien au stade ?)",
            "source_name": "CNSNMM",
            "source_url": href,
            "status": "pending",
            "source_type": "scraped",
        }
        outcome = dedup_upsert_event(sb, row)
        added, merged, errors = _classify_status(outcome, added, merged, errors)
        if outcome == "added":
            log.info("   + [CNSNMM/pending] %s (%s)", title[:70], date_start)

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
    # L'ancienne URL /agenda/ renvoie désormais 404 ; le nouvel agenda est ici :
    list_url = f"{base}/en-ce-moment/tout-lagenda-du-jura/"
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
        # Fiche d'événement : /agenda/<slug> (ancien) ou /fiche… (apidae)
        if not ("/agenda/" in href or "/fiche" in href):
            continue
        # Ignorer les pages d'index / catégories (pas des événements)
        if href.rstrip("/").endswith(("agenda", "tout-lagenda-du-jura",
                                       "evenements-sportifs", "evenements-culturels")):
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


# ── 4bis. Prémanon — agenda officiel de la commune (The Events Calendar) ──────
def scrape_premanon_events() -> tuple[int, int, int]:
    """
    Scrape l'agenda officiel de la commune de Prémanon (premanon.com),
    propulsé par WordPress « The Events Calendar » qui expose une API REST
    structurée avec un champ `venue`. On ne garde que les événements DONT
    le lieu, le titre ou la description mentionnent le stade des Tuffes /
    CNSNMM (et non tous les événements du village comme la bibliothèque).

    Source fiable et datée (JJ machine), toutes saisons.
    """
    base = "https://premanon.com/wp-json/tribe/events/v1/events"
    STADE_KW = ("tuffes", "cnsnmm", "stade nordique")
    added = merged = errors = 0
    page = 1

    while page <= 8:
        url = f"{base}?per_page=50&page={page}&start_date=2025-06-01"
        try:
            resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.warning("   Prémanon agenda inaccessible (page %d) : %s", page, exc)
            errors += 1
            break

        events = data.get("events", [])
        if not events:
            break

        for e in events:
            venue = ((e.get("venue") or {}).get("venue") or "")
            title = html.unescape(e.get("title", "") or "").strip()
            desc = BeautifulSoup(e.get("description", "") or "", "html.parser").get_text(" ", strip=True)
            blob = f"{title} {venue} {desc}".lower()

            # Filtre : uniquement les événements au stade / CNSNMM
            if not any(kw in blob for kw in STADE_KW):
                continue

            date_start = (e.get("start_date") or "")[:10]
            date_end = (e.get("end_date") or "")[:10]
            if not date_start:
                continue
            if not is_future_date(date_start):
                continue
            if date_end == date_start or not date_end:
                date_end = None

            sport = detect_sport(f"{title} {desc}", "Nordique")

            row = {
                "title":         title[:255],
                "sport":         sport,
                "date_start":    date_start,
                "date_end":      date_end,
                "public_access": True,
                "has_catering":  None,
                "notes":         (desc[:400] + "…") if len(desc) > 400 else (desc or None),
                "source_name":   "Prémanon (commune)",
                "source_url":    e.get("url") or "https://premanon.com/evenements/",
                "status":        "published",
                "source_type":   "scraped",
            }
            outcome = dedup_upsert_event(sb, row)
            added, merged, errors = _classify_status(outcome, added, merged, errors)
            if outcome == "added":
                log.info("   + [Prémanon] %s (%s)", title[:70], date_start)

        if len(events) < 50:
            break
        page += 1
        time.sleep(0.5)

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


def scrape_listing_blocks(name: str, soup, url: str, status: str = "pending",
                          default_sport: str = "Nordique") -> tuple[int, int, int]:
    """
    Balaie les blocs texte d'une page de listing (tableau de saison, agenda) et
    retient ceux qui citent le stade des Tuffes avec une date exploitable.
    Utilisé quand la page n'offre pas de lien détail par épreuve.
    """
    added = merged = errors = 0
    seen: set[str] = set()

    for block in soup.find_all(["tr", "li", "p", "h3", "h4"]):
        text = block.get_text(separator=" ", strip=True)
        if not text or len(text) < 10 or len(text) > 300:
            continue
        if not is_at_tuffes(text):
            continue
        date_start, date_end = parse_date_loose(text)
        if not date_start or not is_future_date(date_start):
            continue

        # Titre : on retire la date pour ne pas la répéter dans le libellé.
        title = re.sub(r"\s{2,}", " ", RE_DATE_RANGE_DASH.sub("", text)).strip(" -–—•|·")
        title = RE_DATE_SINGLE.sub("", title).strip(" -–—•|·") or text[:90]

        key = f"{title[:60]}|{date_start}"
        if key in seen:
            continue
        seen.add(key)

        row = {
            "title": title[:255],
            "sport": detect_sport(text, default_sport),
            "date_start": date_start,
            "date_end": date_end,
            "public_access": None,
            "notes": f"Source : {name} — {text[:250]}",
            "source_name": name,
            "source_url": url,
            "status": status,
            "source_type": "scraped",
        }
        outcome = upsert_extended(row)
        added, merged, errors = _classify_status(outcome, added, merged, errors)
        if outcome == "added":
            log.info("   + [%s/%s] %s (%s)", name, status, title[:70], date_start)

    return added, merged, errors


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

    # Beaucoup de clubs publient un simple tableau de saison, sans page détail
    # par course (c'est le cas du calendrier Haut-Jura Ski) : on balaie alors le
    # texte de la page pour ne pas passer à côté des épreuves aux Tuffes.
    a, m, e = scrape_listing_blocks(name, soup, used_url)
    added += a; merged += m; errors += e

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

        date_start, date_end = parse_date_loose(detail_text + " " + title)
        if not date_start:
            time.sleep(0.4)
            continue
        if not is_future_date(date_start):
            time.sleep(0.4)
            continue

        row = {
            "title": title[:255],
            "sport": detect_sport(title + " " + detail_text, "Nordique"),
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


# ── Colonnes étendues (level / organizer / is_highlight) ──────────────────────
# Ajoutées par migration_events_agenda.sql. Tant que la migration n'est pas
# passée, PostgREST refuse l'insert : on sonde une fois puis on retire ces clés.
EXTENDED_COLUMNS = ("level", "organizer", "is_highlight", "date_tbd")
_extended_supported: bool | None = None


def extended_columns_supported() -> bool:
    """Teste une seule fois si la table `events` a les colonnes étendues."""
    global _extended_supported
    if _extended_supported is None:
        try:
            sb.table("events").select(",".join(EXTENDED_COLUMNS)).limit(1).execute()
            _extended_supported = True
        except Exception as exc:
            log.warning(
                "Colonnes %s absentes de la table events (%s) — "
                "applique migration_events_agenda.sql pour les activer.",
                ", ".join(EXTENDED_COLUMNS), exc,
            )
            _extended_supported = False
    return _extended_supported


def prepare_row(row: dict) -> dict:
    """Retire les colonnes étendues si le schéma Supabase ne les a pas encore."""
    if extended_columns_supported():
        return row
    return {k: v for k, v in row.items() if k not in EXTENDED_COLUMNS}


def upsert_extended(row: dict) -> str:
    """Upsert dédupliqué, en tenant compte des colonnes étendues disponibles."""
    extras = EXTENDED_COLUMNS if extended_columns_supported() else ()
    return dedup_upsert_event(sb, prepare_row(row), extra_fields=extras)


# ── Dates : compléments au parseur français de dedup.py ───────────────────────
RE_DATE_RANGE_DASH = re.compile(
    r"\b(\d{1,2})\s*(?:-|–|—|/|\bau\b|\bet\b)\s*(\d{1,2})\s+"
    r"(janv|févr|fevr|mars|avri|mai|juin|juil|août|aout|sept|octo|nove|déce|dece)[a-zé.]*"
    r"(?:\s+(\d{4}))?",
    re.IGNORECASE,
)
RE_DATE_SINGLE = re.compile(
    r"\b(\d{1,2})\s+"
    r"(janv|févr|fevr|mars|avri|mai|juin|juil|août|aout|sept|octo|nove|déce|dece)[a-zé.]*"
    r"(?:\s+(\d{4}))?",
    re.IGNORECASE,
)
MOIS_PREFIX = {
    "janv": 1, "févr": 2, "fevr": 2, "mars": 3, "avri": 4, "mai": 5,
    "juin": 6, "juil": 7, "août": 8, "aout": 8, "sept": 9, "octo": 10,
    "nove": 11, "déce": 12, "dece": 12,
}


def season_year_for_month(month: int, reference: datetime | None = None) -> int:
    """Année de la saison en cours pour un mois donné (saison sept → août)."""
    ref = reference or datetime.now(timezone.utc)
    start_year = ref.year if ref.month >= 9 else ref.year - 1
    return start_year if month >= 9 else start_year + 1


def _future_year(month: int, day: int, reference: datetime | None = None) -> int:
    """
    Année à retenir pour une date sans millésime : celle de la saison en cours,
    décalée d'un an si elle tomberait déjà dans le passé (les calendriers
    publiés annoncent des épreuves à venir).
    """
    ref = reference or datetime.now(timezone.utc)
    year = season_year_for_month(month, ref)
    try:
        candidate = datetime(year, month, day, tzinfo=timezone.utc)
    except ValueError:
        return year
    return year + 1 if candidate.date() < ref.date() else year


def parse_date_loose(text: str, reference: datetime | None = None):
    """
    Parse une date française avec ou sans année, en plage ou non.

    Couvre « du 19 au 20 décembre 2026 », « 19-20 déc. », « 5 juillet 2026 ».
    L'année manquante est déduite de la saison en cours.
    Retourne (date_start, date_end) — date_end vaut None sur un jour unique.
    """
    if not text:
        return None, None
    lowered = text.lower()

    # 1) Plage « 19-20 décembre [2026] » / « 27 et 28 février » : testée avant le
    #    parseur strict, qui ne verrait que le second jour de la plage.
    m = RE_DATE_RANGE_DASH.search(lowered)
    if m:
        d1, d2, mois_txt, annee = m.group(1), m.group(2), m.group(3), m.group(4)
        mois = MOIS_PREFIX.get(mois_txt[:4].lower())
        if mois:
            year = int(annee) if annee else _future_year(mois, int(d1), reference)
            return f"{year}-{mois:02d}-{int(d1):02d}", f"{year}-{mois:02d}-{int(d2):02d}"

    # 2) Parseur strict partagé (« du X au Y mois AAAA », formats numériques).
    start, end = parse_french_date(text)
    if start:
        return start, end

    # 3) Jour unique, année éventuellement absente.
    m = RE_DATE_SINGLE.search(lowered)
    if m:
        d1, mois_txt, annee = m.group(1), m.group(2), m.group(3)
        mois = MOIS_PREFIX.get(mois_txt[:4].lower())
        if mois:
            year = int(annee) if annee else _future_year(mois, int(d1), reference)
            return f"{year}-{mois:02d}-{int(d1):02d}", None

    return None, None


# ── Événements confirmés à la main (saison 2026-2027) ─────────────────────────
# Vérifiés auprès des sources officielles listées dans `source_url`. Ils sont
# repoussés à chaque run : le dedup les fusionne avec la version scrapée dès
# qu'une source publique les annonce, donc aucun doublon.
# Le même jeu de données alimente le front (`redesign/events-seed.js`).
CONFIRMED_SEASON = "2026-2027"

CONFIRMED_EVENTS = [
    {
        "title": "Sprint Classique Les Tuffes",
        "sport": "Ski de fond",
        "date_start": "2026-10-24",
        "date_end": None,
        "level": "Régional",
        "organizer": "Haut-Jura Ski",
        "public_access": True,
        "notes": ("Sprint classique ouvert des U11 aux Seniors, annoncé au calendrier "
                  "de saison de Haut-Jura Ski. Format et modalités communiqués "
                  "ultérieurement par le club."),
        "source_name": "Haut-Jura Ski — saison 2026/2027",
        "source_url": "https://www.hautjuraski.fr/evenements",
    },
    {
        "title": "Cyclo Haut-Jura",
        "sport": "Cyclisme",
        "date_start": "2026-07-05",
        "date_end": None,
        "level": "Régional",
        "organizer": "Jura Ski Events",
        "public_access": True,
        "has_catering": True,
        "notes": ("Départs et arrivées depuis l'esplanade du stade des Tuffes, sur des "
                  "parcours de 65 km (940 m D+) et 100 km (1620 m D+)."),
        "source_name": "Vélo-Cyclosport — agenda cyclosportives",
        "source_url": "https://www.velo-cyclosport.com/agenda/index.php?month=7",
    },
    {
        "title": "SAMSE National Tour Biathlon — Étape 2",
        "sport": "Biathlon",
        "date_start": "2026-12-19",
        "date_end": "2026-12-20",
        "level": "National",
        "organizer": "FFS & Ski Club du Grandvaux",
        "public_access": True,
        "has_catering": True,
        "notes": ("Première étape sur neige de la saison pour les U17, avec également "
                  "les U19, U21 et Seniors. Épreuves : individuel, sprint et mass-start."),
        "source_name": "Ski-Nordique.net — calendrier national FFS",
        "source_url": SKI_NORDIQUE_CALENDAR_URL,
    },
    {
        "title": "FIS Tour de Ski 2027 — Coupe du monde",
        "sport": "Ski de fond",
        "date_start": "2027-01-01",
        "date_end": "2027-01-03",
        "level": "International",
        "organizer": "FIS & Jura Ski Events",
        "is_highlight": True,
        "public_access": True,
        "has_catering": True,
        "notes": ("Étape inaugurale du Tour de Ski FIS en France, sur trois jours : "
                  "sprint classique 1,3 km le 1er janvier, mass-start classique 20 km le "
                  "2, poursuite libre 15 km le 3 — femmes et hommes. Village partenaires, "
                  "navettes et pack VIP."),
        "source_name": "World Cup Station des Rousses",
        "source_url": "https://www.worldcupstationdesrousses.fr/tour-de-ski-2027-les-rousses/",
    },
    {
        "title": "SAMSE National Tour Biathlon — Étape 6",
        "sport": "Biathlon",
        "date_start": "2027-02-27",
        "date_end": "2027-02-28",
        "level": "National",
        "organizer": "FFS & ESSS Montbenoît",
        "public_access": True,
        "has_catering": True,
        "notes": ("Sixième étape du circuit national (U19 à Seniors), avec la participation "
                  "exceptionnelle de 70 biathlètes suisses. Épreuves : sprint, individuel "
                  "et mass-start."),
        "source_name": "Ski-Nordique.net — calendrier national FFS",
        "source_url": SKI_NORDIQUE_CALENDAR_URL,
    },
    {
        # La page officielle annonce « janvier 2027 » et le départ au stade des
        # Tuffes, sans jour précis : date_tbd affiche « date à confirmer » plutôt
        # qu'un jour inventé. date_start = fin de mois pour rester « à venir »
        # pendant tout janvier.
        "title": "La Transju'Jeunes",
        "sport": "Ski de fond",
        "date_start": "2027-01-31",
        "date_end": None,
        "date_tbd": True,
        "level": "Régional",
        "organizer": "La Transju' / Trans'Organisation",
        "public_access": True,
        "notes": ("Course jeunes au départ du stade nordique des Tuffes, environ "
                  "2 000 participants annoncés. Mois et lieu confirmés, jour exact "
                  "encore non publié par l'organisation."),
        "source_name": "La Transju' — Transju'Jeunes",
        "source_url": "https://www.latransju.com/la-transjeunes/",
    },
    {
        # Dates officielles confirmées, mais le parcours 2027 n'est pas publié :
        # le passage au stade reste à revalider → file d'attente admin.
        "title": "La Transju'Trails",
        "sport": "Trail",
        "date_start": "2027-06-05",
        "date_end": "2027-06-06",
        "level": "Régional",
        "organizer": "La Transju' / Trans'Organisation",
        "public_access": True,
        "status": "pending",
        "notes": ("Dates officielles confirmées. Le stade des Tuffes est cité comme "
                  "point de passage par des sources tierces, mais le parcours 2027 "
                  "officiel n'est pas encore publié — à revalider avant publication."),
        "source_name": "La Transju' — Transju'Trails 2027",
        "source_url": "https://www.latransju.com/la-transjutrails/",
    },
]


def seed_confirmed_events() -> tuple[int, int, int]:
    """Pousse (ou enrichit) les événements confirmés à la main."""
    added = merged = errors = 0
    for event in CONFIRMED_EVENTS:
        row = dict(event)
        row.setdefault("status", "published")
        row.setdefault("source_type", "manual")
        outcome = upsert_extended(row)
        if outcome == "error":
            # Un schéma qui n'accepte pas source_type='manual' (contrainte CHECK)
            # ne doit pas faire perdre une épreuve confirmée : on réessaie sans.
            fallback = {k: v for k, v in row.items() if k != "source_type"}
            log.info("   ↻ nouvel essai sans source_type pour '%s'", row["title"][:60])
            outcome = upsert_extended(fallback)
        added, merged, errors = _classify_status(outcome, added, merged, errors)
        if outcome == "added":
            log.info("   + [confirmé] %s (%s)", row["title"], row["date_start"])
    return added, merged, errors


# ── 7. Vélo-Cyclosport (agenda national cyclosportives) ───────────────────────
def scrape_velo_cyclosport() -> tuple[int, int, int]:
    """
    Parcourt l'agenda mensuel de Vélo-Cyclosport et retient les épreuves du
    Haut-Jura (Cyclo Haut-Jura et consorts, dont les départs se font au stade).
    """
    added = merged = errors = 0
    now = datetime.now(timezone.utc)
    months = [((now.month - 1 + offset) % 12) + 1 for offset in range(12)]
    seen: set[str] = set()

    for month in months:
        url = f"{VELO_CYCLOSPORT_URL}?month={month}"
        try:
            resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
        except Exception as exc:
            log.warning("   Vélo-Cyclosport mois %s inaccessible : %s", month, exc)
            errors += 1
            continue

        # La page est un listing : on balaie les blocs susceptibles de porter
        # une épreuve (lignes de tableau, items de liste, blocs génériques).
        for block in soup.find_all(["tr", "li", "article", "div"]):
            text = block.get_text(separator=" ", strip=True)
            if not text or len(text) > 400:
                continue
            if not any(kw in text.lower() for kw in ("haut-jura", "haut jura", "tuffes", "prémanon", "premanon")):
                continue
            date_start, date_end = parse_date_loose(text)
            if not date_start or not is_future_date(date_start):
                continue
            year = date_start[:4]
            title = f"Cyclo Haut-Jura {year}" if "jura" in text.lower() else text[:90]
            key = f"{title}|{date_start}"
            if key in seen:
                continue
            seen.add(key)

            row = {
                "title": title[:255],
                "sport": "Cyclisme",
                "date_start": date_start,
                "date_end": date_end,
                "level": "Régional",
                "organizer": "Jura Ski Events",
                "public_access": True,
                "notes": text[:400],
                "source_name": "Vélo-Cyclosport — agenda cyclosportives",
                "source_url": url,
                "status": "published" if is_at_tuffes(text) else "pending",
                "source_type": "scraped",
            }
            outcome = upsert_extended(row)
            added, merged, errors = _classify_status(outcome, added, merged, errors)
            if outcome == "added":
                log.info("   + [Vélo-Cyclosport] %s (%s)", title[:70], date_start)
        time.sleep(1)

    return added, merged, errors


# ── 8. Ski-Nordique.net (calendrier national FFS biathlon / fond) ─────────────
RE_ETAPE = re.compile(r"étape\s*n?°?\s*(\d{1,2})", re.IGNORECASE)


def scrape_ski_nordique_calendrier() -> tuple[int, int, int]:
    """
    Lit l'article « calendrier des épreuves nationales » de Ski-Nordique.net et
    retient les étapes disputées à Prémanon / aux Tuffes.
    """
    added = merged = errors = 0
    try:
        resp = requests.get(SKI_NORDIQUE_CALENDAR_URL, timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
    except Exception as exc:
        log.warning("   Ski-Nordique.net inaccessible : %s", exc)
        return 0, 0, 1

    seen: set[str] = set()
    for block in soup.find_all(["tr", "li", "p", "h2", "h3"]):
        text = block.get_text(separator=" ", strip=True)
        if not text or len(text) > 400 or not is_at_tuffes(text):
            continue
        date_start, date_end = parse_date_loose(text)
        if not date_start or not is_future_date(date_start):
            continue

        sport = detect_sport(text, "Biathlon")
        etape = RE_ETAPE.search(text)
        if etape and "samse" in text.lower():
            title = f"SAMSE National Tour {sport} — Étape {int(etape.group(1))}"
        elif etape:
            title = f"National Tour {sport} — Étape {int(etape.group(1))}"
        else:
            title = text[:90]

        key = f"{title}|{date_start}"
        if key in seen:
            continue
        seen.add(key)

        row = {
            "title": title[:255],
            "sport": sport,
            "date_start": date_start,
            "date_end": date_end,
            "level": "National",
            "public_access": True,
            "notes": text[:400],
            "source_name": "Ski-Nordique.net — calendrier national FFS",
            "source_url": SKI_NORDIQUE_CALENDAR_URL,
            "status": "published",
            "source_type": "scraped",
        }
        outcome = upsert_extended(row)
        added, merged, errors = _classify_status(outcome, added, merged, errors)
        if outcome == "added":
            log.info("   + [Ski-Nordique] %s (%s)", title[:70], date_start)

    return added, merged, errors


# ── 9. World Cup Station des Rousses (Tour de Ski / Coupe du monde FIS) ───────
def scrape_world_cup_rousses() -> tuple[int, int, int]:
    """
    Site officiel de l'organisation locale de la Coupe du monde FIS.
    Les épreuves y sont annoncées bien avant d'apparaître au calendrier FFS.
    """
    added = merged = errors = 0
    seen: set[str] = set()

    for url in WORLD_CUP_ROUSSES_URLS:
        try:
            resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
        except Exception as exc:
            log.warning("   World Cup Rousses inaccessible (%s) : %s", url, exc)
            errors += 1
            continue

        for block in soup.find_all(["h1", "h2", "h3", "li", "p", "tr"]):
            text = block.get_text(separator=" ", strip=True)
            if not text or len(text) > 400:
                continue
            lowered = text.lower()
            if not any(kw in lowered for kw in ("tour de ski", "coupe du monde", "world cup")):
                continue
            date_start, date_end = parse_date_loose(text)
            if not date_start or not is_future_date(date_start):
                continue

            year = date_start[:4]
            title = f"FIS Tour de Ski {year} — Coupe du monde" if "tour de ski" in lowered \
                else f"Coupe du monde FIS {year} — {detect_sport(text, 'Ski de fond')}"
            key = f"{title}|{date_start}"
            if key in seen:
                continue
            seen.add(key)

            row = {
                "title": title[:255],
                "sport": detect_sport(text, "Ski de fond"),
                "date_start": date_start,
                "date_end": date_end,
                "level": "International",
                "organizer": "FIS & Jura Ski Events",
                "is_highlight": True,
                "public_access": True,
                "notes": text[:400],
                "source_name": "World Cup Station des Rousses",
                "source_url": url,
                "status": "published",
                "source_type": "scraped",
            }
            outcome = upsert_extended(row)
            added, merged, errors = _classify_status(outcome, added, merged, errors)
            if outcome == "added":
                log.info("   + [World Cup Rousses] %s (%s)", title[:70], date_start)
        time.sleep(1)

    return added, merged, errors


# ── 10. Pages événement La Transju' (Transju'Jeunes, Transju'Trails) ─────────
RE_MOIS_SEUL = re.compile(
    r"\b(janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|"
    r"septembre|octobre|novembre|décembre|decembre)\s+(20\d{2})\b",
    re.IGNORECASE,
)


def scrape_transju_event_pages() -> tuple[int, int, int]:
    """
    Lit les pages événement de La Transju'. Le feed RSS (scrape_transju) ne
    couvre que les actualités : ces pages portent les dates de l'édition.

    Quand la page n'annonce qu'un mois (« Janvier 2027 »), l'épreuve est
    enregistrée avec date_tbd — le site affiche « date à confirmer » plutôt
    qu'un jour inventé.
    """
    added = merged = errors = 0

    for page in TRANSJU_EVENT_PAGES:
        url = page["url"]
        try:
            resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
        except Exception as exc:
            log.warning("   Transju page inaccessible (%s) : %s", url, exc)
            errors += 1
            continue

        page_text = soup.get_text(separator=" ", strip=True)
        if not is_at_tuffes(page_text):
            log.info("   %s : pas de mention du stade des Tuffes", page["label"])
            continue

        date_start, date_end = parse_date_loose(page_text)
        date_tbd = False
        if not date_start:
            # Pas de jour publié : on retient le mois annoncé, dernier jour du
            # mois pour que l'épreuve reste « à venir » jusqu'à sa tenue.
            m = RE_MOIS_SEUL.search(page_text.lower())
            if not m:
                log.info("   %s : aucune date exploitable", page["label"])
                continue
            mois = MOIS_FR.get(m.group(1).lower())
            year = int(m.group(2))
            if not mois:
                continue
            last_day = calendar.monthrange(year, mois)[1]
            date_start, date_tbd = f"{year}-{mois:02d}-{last_day:02d}", True

        if not is_future_date(date_start):
            log.info("   %s : édition passée (%s)", page["label"], date_start)
            continue

        row = {
            "title": page["label"],
            "sport": page["sport"],
            "date_start": date_start,
            "date_end": date_end,
            "date_tbd": date_tbd,
            "level": "Régional",
            "organizer": "La Transju' / Trans'Organisation",
            "public_access": True,
            "notes": page_text[:400],
            "source_name": f"La Transju' — {page['label']}",
            "source_url": url,
            # Le parcours peut changer d'une édition à l'autre : les épreuves
            # dont le passage au stade n'est pas explicite restent à valider.
            "status": "published" if page["sport"] != "Trail" else "pending",
            "source_type": "scraped",
        }
        outcome = upsert_extended(row)
        added, merged, errors = _classify_status(outcome, added, merged, errors)
        if outcome == "added":
            log.info("   + [Transju] %s (%s%s)", page["label"], date_start,
                     " — date à confirmer" if date_tbd else "")
        time.sleep(1)

    return added, merged, errors


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    log.info("=== scrape_events.py démarré ===")
    total_added = 0
    total_merged = 0
    total_errors = 0

    # 0) Événements confirmés à la main : poussés en premier pour que les
    #    scrapers viennent les enrichir (sources additionnelles) plutôt que
    #    créer un doublon moins complet.
    log.info("→ Événements confirmés (saison %s)", CONFIRMED_SEASON)
    a, m, e = seed_confirmed_events()
    total_added += a; total_merged += m; total_errors += e
    log.info("   Confirmés : %d ajoutés, %d fusionnés, %d erreurs", a, m, e)

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

    log.info("→ Scraping agenda commune de Prémanon")
    a, m, e = scrape_premanon_events()
    total_added += a; total_merged += m; total_errors += e
    log.info("   Prémanon : %d ajoutés, %d fusionnés, %d erreurs", a, m, e)
    time.sleep(2)

    log.info("→ Scraping Vélo-Cyclosport (agenda cyclosportives)")
    a, m, e = scrape_velo_cyclosport()
    total_added += a; total_merged += m; total_errors += e
    log.info("   Vélo-Cyclosport : %d ajoutés, %d fusionnés, %d erreurs", a, m, e)
    time.sleep(2)

    log.info("→ Scraping Ski-Nordique.net (calendrier national FFS)")
    a, m, e = scrape_ski_nordique_calendrier()
    total_added += a; total_merged += m; total_errors += e
    log.info("   Ski-Nordique : %d ajoutés, %d fusionnés, %d erreurs", a, m, e)
    time.sleep(2)

    log.info("→ Scraping World Cup Station des Rousses (Coupe du monde FIS)")
    a, m, e = scrape_world_cup_rousses()
    total_added += a; total_merged += m; total_errors += e
    log.info("   World Cup Rousses : %d ajoutés, %d fusionnés, %d erreurs", a, m, e)
    time.sleep(2)

    log.info("→ Scraping pages événement La Transju'")
    a, m, e = scrape_transju_event_pages()
    total_added += a; total_merged += m; total_errors += e
    log.info("   Transju pages : %d ajoutés, %d fusionnés, %d erreurs", a, m, e)
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
