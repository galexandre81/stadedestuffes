"""
scrape_events.py
Scraper d'événements pour la table `events` (Supabase).
Stratégie : détecter les compétitions à Prémanon/Les Tuffes via :
  1. Articles FFS (API WordPress REST) mentionnant prémanon/tuffes
  2. Articles NordicMag (RSS) mentionnant prémanon/tuffes
  3. Articles des clubs locaux (CSR Pontarlier, Saugeathlon) pour compétitions régionales

Les événements mentionnant Prémanon/Tuffes sont publiés directement (status='published').
La validation (status='pending') est réservée aux soumissions manuelles du formulaire public.
"""

import os
import re
import logging
import time
from datetime import datetime, timezone, timedelta

import feedparser
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
TIMEOUT = 12

# ── Mots-clés lieu ─────────────────────────────────────────────────────────────
KEYWORDS_LIEU = [
    "tuffes", "prémanon", "premanon", "cnsnmm", "stade nordique",
]

KEYWORDS_COMPET = [
    "compétition", "competition", "championnat", "coupe", "cup",
    "épreuve", "course", "concours", "grand prix", "gp ",
    "nationale", "national", "régionale", "régional",
]

# ── Mapping titre → discipline ─────────────────────────────────────────────────
SPORT_MAP = [
    ("biathlon",            "Biathlon"),
    ("saut à ski",          "Saut à ski"),
    ("saut a ski",          "Saut à ski"),
    ("tremplin",            "Saut à ski"),
    ("combiné nordique",    "Combiné nordique"),
    ("combine nordique",    "Combiné nordique"),
    ("nordic combined",     "Combiné nordique"),
    ("ski de fond",         "Ski de fond"),
    ("fond",                "Ski de fond"),
    ("cross-country",       "Ski de fond"),
    ("skiathlon",           "Ski de fond"),
]

MOIS_FR = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def clean_html(raw: str) -> str:
    if not raw:
        return ""
    return BeautifulSoup(raw, "html.parser").get_text(separator=" ", strip=True)


def detect_sport(text: str) -> str:
    lower = text.lower()
    for kw, sport in SPORT_MAP:
        if kw in lower:
            return sport
    return "Nordique"


def is_lieu_tuffes(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in KEYWORDS_LIEU)


def is_competition(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in KEYWORDS_COMPET)


# Mots-clés qui indiquent un article de résultats (passé) → à exclure des events
KEYWORDS_RESULTATS = [
    "victoire", "vainqueur", "vainqueure", "gagne", "remporte", "podium",
    "résultat", "résultats", "classement", "bilan", "palmarès", "palmares",
    "médaille", "medaille", "titre", "sacre", "sacré", "champion",
    "l'émotion", "dernière course", "tire sa révérence", "raccroche",
    "interview", "réaction", "après sa", "après la", "après le",
]

def is_resultat(text: str) -> bool:
    """Retourne True si le texte ressemble à un article de résultats (pas une annonce)."""
    lower = text.lower()
    return any(kw in lower for kw in KEYWORDS_RESULTATS)


def extract_date_from_text(text: str) -> str | None:
    """
    Cherche une date dans le texte.
    Formats supportés : DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD,
    'le DD mois YYYY', 'du DD au DD mois YYYY'.
    Retourne la première date trouvée en format YYYY-MM-DD, ou None.
    """
    # Format ISO YYYY-MM-DD
    m = re.search(r'\b(20\d{2})[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[12]\d|3[01])\b', text)
    if m:
        try:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        except Exception:
            pass

    # Format DD/MM/YYYY ou DD-MM-YYYY
    m = re.search(r'\b(0?[1-9]|[12]\d|3[01])[/\-](0?[1-9]|1[0-2])[/\-](20\d{2})\b', text)
    if m:
        try:
            return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
        except Exception:
            pass

    # Format "DD mois YYYY" ou "du DD mois" (sans année → année courante/suivante)
    lower = text.lower()
    for mois_str, mois_num in MOIS_FR.items():
        patterns = [
            rf'\b(\d{{1,2}})\s+{mois_str}\s+(20\d{{2}})\b',  # DD mois YYYY
            rf'\b(\d{{1,2}})\s+{mois_str}\b',                  # DD mois (sans année)
        ]
        for pat in patterns:
            m = re.search(pat, lower)
            if m:
                day = int(m.group(1))
                year = int(m.group(2)) if len(m.groups()) >= 2 and m.group(2) else None
                if not year:
                    # Estimer l'année (si mois déjà passé → prochain hiver)
                    now = datetime.now(timezone.utc)
                    year = now.year if mois_num >= now.month else now.year + 1
                try:
                    return f"{year}-{mois_num:02d}-{day:02d}"
                except Exception:
                    pass

    return None


def make_event_row(title: str, date_str: str | None, sport: str,
                   source_name: str, source_url: str, notes: str = "",
                   status: str = "pending") -> dict:
    return {
        "title":        title[:255],
        "sport":        sport,
        "date_start":   date_str,
        "date_end":     None,
        "public_access": None,
        "has_catering": None,
        "notes":        notes[:500] if notes else None,
        "source_name":  source_name,
        "source_url":   source_url,
        "status":       status,
        "source_type":  "scraped",
    }


def upsert_event(row: dict) -> bool:
    """
    Insère l'événement s'il n'existe pas déjà (déduplication sur title + date_start).
    Retourne True si inséré, False si déjà présent ou erreur.
    """
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
            return False  # déjà présent
        sb.table("events").insert(row).execute()
        return True
    except Exception as exc:
        log.error("   ✗ upsert events ÉCHOUÉ pour '%s' : %s", row["title"][:60], exc)
        return False


# ── Source 1 : FFS WordPress REST API ─────────────────────────────────────────
# Disciplines : cat=56 fond, cat=9 biathlon, cat=52 saut, cat=55 combiné
FFS_CATEGORIES = [
    (56, "Ski de fond"),
    (9,  "Biathlon"),
    (52, "Saut à ski"),
    (55, "Combiné nordique"),
]

def scrape_ffs_events() -> int:
    """Cherche dans les articles FFS récents ceux qui mentionnent Les Tuffes/Prémanon."""
    inserted = 0
    for cat_id, sport in FFS_CATEGORIES:
        url = f"https://ffs.fr/wp-json/wp/v2/posts?categories={cat_id}&per_page=20&orderby=date&order=desc"
        try:
            resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
            resp.raise_for_status()
            posts = resp.json()
        except Exception as exc:
            log.warning("   FFS cat=%d inaccessible : %s", cat_id, exc)
            continue

        for post in posts:
            title_raw = post.get("title", {}).get("rendered", "")
            title = BeautifulSoup(title_raw, "html.parser").get_text(strip=True)
            excerpt = clean_html(post.get("excerpt", {}).get("rendered", ""))
            link = post.get("link", "")
            date_pub = post.get("date", "")[:10]  # YYYY-MM-DD

            full_text = f"{title} {excerpt}"
            # Le TITRE doit mentionner Tuffes/Prémanon ET être une annonce, pas un résultat
            if not is_lieu_tuffes(title):
                continue
            if is_resultat(title):
                continue
            if not is_competition(title):
                continue

            # Chercher une date dans le texte, sinon utiliser la date de publication + 7j
            date_event = extract_date_from_text(full_text)
            if not date_event:
                try:
                    pub_dt = datetime.strptime(date_pub, "%Y-%m-%d")
                    future_dt = pub_dt + timedelta(days=7)
                    date_event = future_dt.strftime("%Y-%m-%d")
                except Exception:
                    date_event = date_pub

            row = make_event_row(
                title=title,
                date_str=date_event,
                sport=detect_sport(full_text) or sport,
                source_name="FFS",
                source_url=link,
                notes=excerpt[:300] if excerpt else "",
                status="published",  # FFS + lieu confirmé → publié directement
            )
            if upsert_event(row):
                log.info("   + FFS event : %s (%s)", title[:60], date_event)
                inserted += 1

        time.sleep(0.5)

    return inserted


# ── Source 1b : FFS calendrier officiel (https://ffs.fr/calendrier/) ───────────
CALENDRIER_URL = "https://ffs.fr/calendrier/"

def fetch_ffs_calendrier_detail(url: str) -> dict:
    """Récupère le détail d'un événement FFS (titre/date/place/description)."""
    try:
        resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("   FFS detail inaccessible %s : %s", url, exc)
        return {}

    soup = BeautifulSoup(resp.content, "html.parser")
    title = (soup.find("h1") or soup.find("h2") or soup.title)
    title_txt = title.get_text(strip=True) if title else ""

    # contenu textuel pour extraction date / localisation / notes
    full_text = clean_html(soup.get_text(separator=" ", strip=True))
    date_str = extract_date_from_text(full_text)

    # note : on prend une partie du texte comme description
    notes = " ".join(x for x in [
        title_txt,
        full_text[:400],
    ] if x)

    return {
        "title": title_txt,
        "date": date_str,
        "notes": notes,
        "full_text": full_text,
    }


def scrape_ffs_calendrier_events() -> int:
    """Scrape le calendrier FFS et ajoute les événements à Prémanon / Les Tuffes."""
    inserted = 0
    try:
        resp = requests.get(CALENDRIER_URL, timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
    except Exception as exc:
        log.warning("   FFS calendrier inaccessible : %s", exc)
        return 0

    # Exemples de sélecteurs, robustes sur plusieurs versions de page
    selectors = [
        "article", ".agenda-item", ".fc-event", ".calendar-event", ".event-item",
        ".item", "li",
    ]

    candidates = []
    for sel in selectors:
        candidates.extend(soup.select(sel))

    seen_urls = set()
    for node in candidates:
        # lien vers détail potentiellement plus complet
        link = node.find("a", href=True)
        url = link["href"].strip() if link else CALENDRIER_URL

        title = node.get_text(separator=" ", strip=True)
        if not title:
            continue

        # ne conserver que les événements contenant Prémanon/Les Tuffes
        if not is_lieu_tuffes(title):
            continue

        # pour éviter doublons de même lien
        if url in seen_urls:
            continue
        seen_urls.add(url)

        # extraire infos du détail si possible
        detail = fetch_ffs_calendrier_detail(url) if url and url != CALENDRIER_URL else {}
        full_text = " ".join([title, detail.get("full_text", "")])
        date_event = detail.get("date") or extract_date_from_text(full_text)

        # on exige une date pour éviter le parsing de non-événements
        if not date_event:
            continue

        sport = detect_sport(full_text)

        event_title = detail.get("title") or title

        row = make_event_row(
            title=event_title,
            date_str=date_event,
            sport=sport,
            source_name="FFS calendrier",
            source_url=url,
            notes=(detail.get("notes") or title)[:500],
            status="published",
        )

        if upsert_event(row):
            log.info("   + FFS calendrier event : %s (%s)", event_title[:60], date_event)
            inserted += 1

    return inserted


# ── Source 2 : NordicMag RSS ───────────────────────────────────────────────────
def scrape_nordicmag_events() -> int:
    """Parse le RSS NordicMag et détecte les articles sur des compétitions à Prémanon."""
    inserted = 0
    try:
        resp = requests.get("https://www.nordicmag.info/feed/", timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as exc:
        log.warning("   NordicMag RSS inaccessible : %s", exc)
        return 0

    for entry in feed.entries:
        title = getattr(entry, "title", "").strip()
        summary = clean_html(getattr(entry, "summary", ""))
        link = getattr(entry, "link", "")

        full_text = f"{title} {summary}"
        # Le TITRE doit mentionner Tuffes/Prémanon ET être une annonce compétition
        if not is_lieu_tuffes(title):
            continue
        if is_resultat(title):
            continue
        if not is_competition(title):
            continue

        ts = getattr(entry, "published_parsed", None)
        pub_date = None
        if ts:
            try:
                pub_date = datetime(*ts[:6], tzinfo=timezone.utc).strftime("%Y-%m-%d")
            except Exception:
                pass

        date_event = extract_date_from_text(full_text) or pub_date

        row = make_event_row(
            title=title,
            date_str=date_event,
            sport=detect_sport(full_text),
            source_name="NordicMag",
            source_url=link,
            notes=summary[:300] if summary else "",
            status="published",  # lieu Tuffes/Prémanon confirmé → publié directement
        )
        if upsert_event(row):
            log.info("   + NordicMag event : %s (%s)", title[:60], date_event)
            inserted += 1

    return inserted


# ── Source 3 : Clubs locaux (RSS) ─────────────────────────────────────────────
LOCAL_FEEDS = [
    ("CSR Pontarlier",  "https://csrpontarlier.fr/feed/"),
    ("Saugeathlon",     "https://www.saugeathlon.fr/blog-feed.xml"),
    ("Haut-Jura Léman", "https://hautjuraleman.com/blog-feed.xml"),
    ("SC Grandvaux",    "https://scgrandvaux.fr/feed/"),
]

def scrape_club_events() -> int:
    """Parse les RSS des clubs locaux pour détecter des annonces de compétitions."""
    inserted = 0
    for source_name, feed_url in LOCAL_FEEDS:
        try:
            resp = requests.get(feed_url, timeout=TIMEOUT, headers=HEADERS)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)
        except Exception as exc:
            log.warning("   %s inaccessible : %s", source_name, exc)
            continue

        for entry in feed.entries:
            title = getattr(entry, "title", "").strip()
            summary = clean_html(getattr(entry, "summary", ""))
            link = getattr(entry, "link", "")

            full_text = f"{title} {summary}"
            if not is_competition(full_text):
                continue

            # Si le lieu est confirmé → publié, sinon on ignore
            lieu_confirme = is_lieu_tuffes(full_text)
            if not lieu_confirme:
                continue

            ts = getattr(entry, "published_parsed", None)
            pub_date = None
            if ts:
                try:
                    pub_date = datetime(*ts[:6], tzinfo=timezone.utc).strftime("%Y-%m-%d")
                except Exception:
                    pass

            date_event = extract_date_from_text(full_text) or pub_date

            row = make_event_row(
                title=title,
                date_str=date_event,
                sport=detect_sport(full_text),
                source_name=source_name,
                source_url=link,
                notes=summary[:300] if summary else "",
                status="published",  # lieu Tuffes/Prémanon confirmé → publié directement
            )
            if upsert_event(row):
                log.info("   + %s event : %s (%s)", source_name, title[:60], date_event)
                inserted += 1

        time.sleep(0.5)

    return inserted


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=== scrape_events.py démarré ===")

    n1 = scrape_ffs_events()
    log.info("FFS articles : %d événement(s) détecté(s) à Les Tuffes/Prémanon", n1)

    n1b = scrape_ffs_calendrier_events()
    log.info("FFS calendrier : %d événement(s) détecté(s) à Les Tuffes/Prémanon", n1b)

    n2 = scrape_nordicmag_events()
    log.info("NordicMag : %d événement(s) détecté(s)", n2)

    n3 = scrape_club_events()
    log.info("Clubs locaux : %d événement(s) détecté(s)", n3)

    total = n1 + n1b + n2 + n3
    log.info("=== Terminé : %d événement(s) ajouté(s) en 'pending' pour validation admin ===", total)


if __name__ == "__main__":
    main()
