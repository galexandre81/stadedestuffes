"""
scrape_articles.py
Scraper RSS pour la table press_articles (Supabase).
Sources : médias nordiques nationaux, FFS, clubs locaux.

Filtre strict : seuls les articles mentionnant explicitement le stade des Tuffes
ou Prémanon (ou contenu directement lié) sont publiés. Pour les sources sans
flux RSS exploitable, un fallback HTML scanne les pages d'actualités.
"""

import os
import logging
import time
from datetime import datetime, timezone

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

# ── Sources RSS ────────────────────────────────────────────────────────────────
SOURCES = [
    {"name": "NordicMag",          "url": "https://www.nordicmag.info/feed/",                "category": "media"},
    {"name": "Ski-Nordique.net",   "url": "https://www.ski-nordique.net/rss.php/cat/72348",  "category": "media"},
    {"name": "Hebdo39",            "url": "https://hebdo39.net/feed/",                       "category": "media"},
    {"name": "FFS Fond",           "url": "https://ffs.fr/?feed=rss2&cat=56",                "category": "ffs"},
    {"name": "FFS Biathlon",       "url": "https://ffs.fr/?feed=rss2&cat=9",                 "category": "ffs"},
    {"name": "FFS Saut",           "url": "https://ffs.fr/?feed=rss2&cat=52",                "category": "ffs"},
    {"name": "FFS Combiné",        "url": "https://ffs.fr/?feed=rss2&cat=55",                "category": "ffs"},
    {"name": "Haut-Jura Léman",    "url": "https://www.hautjuraleman.com/blog-feed.xml",     "category": "club_local"},
    {"name": "Haut Jura Ski",      "url": "https://www.hautjuraski.fr/saison-2025-2026/actualites-du-club", "category": "club_local"},
    {"name": "SC Bois d'Amont",    "url": "https://www.scboisdamont.com/feed/",              "category": "club_local"},
    {"name": "SC du Grandvaux",    "url": "https://www.scdugrandvaux.fr/feed/",              "category": "club_local"},
]

# ── Mots-clés ──────────────────────────────────────────────────────────────────
KEYWORDS_TUFFES = [
    "tuffes", "prémanon", "premanon",
    "stade nordique des tuffes", "stade des tuffes",
    "jason lamy chappuis", "jason lamy-chappuis", "jason lamy",
    "cnsnmm", "centre national de ski nordique",
    "jura ski events", "juraskievents",
    "cyclo haut jura", "cyclo haut-jura",
    "transju'jeunes", "transjujeunes", "transju jeunes",
    "samse tour",
    "challenge vincent vittoz", "challenge national vincent vittoz",
    "enduro'trail", "endurotrail",
]

KEYWORDS_SPORT = {
    "fond":     ["ski de fond", "fond", "cross-country", "skating", "classique", "skiathlon",
                 "coupe du monde fond", "tour de ski"],
    "biathlon": ["biathlon", "carabine", "tir", "ibu", "biathl"],
    "saut":     ["saut à ski", "saut a ski", "tremplin", "ski jump", "fis saut"],
    "combine":  ["combiné nordique", "combine nordique", "nordic combined"],
}


# ── Helpers ────────────────────────────────────────────────────────────────────
def detect_sport_tags(text: str) -> list[str]:
    lower = text.lower()
    tags = []
    for sport, kws in KEYWORDS_SPORT.items():
        if any(kw in lower for kw in kws):
            tags.append(sport)
    return tags


def detect_mentions_tuffes(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in KEYWORDS_TUFFES)


def extract_image(entry) -> str | None:
    """Cherche une image dans media:content, enclosures, ou media:thumbnail."""
    media = getattr(entry, "media_content", None)
    if media and isinstance(media, list):
        for m in media:
            if isinstance(m, dict) and m.get("url"):
                return m["url"]

    thumb = getattr(entry, "media_thumbnail", None)
    if thumb and isinstance(thumb, list) and thumb[0].get("url"):
        return thumb[0]["url"]

    for enc in getattr(entry, "enclosures", []):
        if enc.get("type", "").startswith("image/") and enc.get("href"):
            return enc["href"]
        if enc.get("url", ""):
            url = enc["url"]
            if any(url.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
                return url

    return None


def parse_published(entry) -> str | None:
    """Retourne une date ISO 8601 UTC ou None."""
    ts = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if ts:
        try:
            dt = datetime(*ts[:6], tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass
    return None


def clean_summary(raw: str) -> str:
    """Supprime les balises HTML et tronque à 400 caractères."""
    if not raw:
        return ""
    text = BeautifulSoup(raw, "html.parser").get_text(separator=" ", strip=True)
    return text[:400] + ("…" if len(text) > 400 else "")


def fetch_article_html(url: str) -> str:
    """Télécharge le HTML d'un article (5000 premiers caractères du body texte)."""
    try:
        resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        return text[:5000]
    except Exception as exc:
        log.debug("   fetch_article_html : %s — %s", url[:60], exc)
        return ""


# ── HTML fallback pour sources sans flux RSS exploitable ──────────────────────
def scrape_html_news_page(source: dict, html: bytes) -> tuple[int, int, int]:
    """
    Fallback HTML : pour les sources où feedparser ne retourne aucune entrée,
    on parse la page d'actualités, on extrait les liens (titre 15-250 chars),
    on visite chaque lien, on regarde s'il mentionne le stade dans les
    5000 premiers caractères du contenu, et on insère dans press_articles.
    Limite : 30 candidats par source.
    """
    added = ignored = errors = 0
    name = source["name"]

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as exc:
        log.warning("   %s — parsing HTML impossible : %s", name, exc)
        return 0, 0, 1

    # Extraire les liens candidats (longueur de texte 15-250)
    candidates: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    for a in soup.find_all("a", href=True):
        text = a.get_text(separator=" ", strip=True)
        if not text or len(text) < 15 or len(text) > 250:
            continue
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        # URL absolue
        if href.startswith("/"):
            from urllib.parse import urlparse
            parsed = urlparse(source["url"])
            href = f"{parsed.scheme}://{parsed.netloc}{href}"
        elif not href.startswith("http"):
            continue
        if href in seen_urls:
            continue
        seen_urls.add(href)
        candidates.append((text, href))
        if len(candidates) >= 30:
            break

    if not candidates:
        log.info("   %s — aucun lien candidat dans HTML fallback", name)
        return 0, 0, 0

    log.info("   %s — HTML fallback : %d candidats", name, len(candidates))

    for title, url in candidates:
        # Fetch détail
        article_text = fetch_article_html(url)
        if not article_text:
            continue
        if not detect_mentions_tuffes(article_text + " " + title):
            ignored += 1
            continue

        sport_tags = detect_sport_tags(article_text + " " + title)
        summary = article_text[:400] + ("…" if len(article_text) > 400 else "")

        row = {
            "title":           title[:255],
            "url":             url,
            "source_name":     name,
            "source_url":      source["url"],
            "published_at":    None,
            "summary":         summary,
            "image_url":       None,
            "sport_tags":      sport_tags,
            "mentions_tuffes": True,
            "status":          "published",
        }
        try:
            sb.table("press_articles").upsert(row, on_conflict="url").execute()
            added += 1
            log.info("   + [HTML] %s", title[:70])
        except Exception as exc:
            log.error("   ✗ upsert HTML ÉCHOUÉ pour '%s' : %s", url[:60], exc)
            errors += 1

        time.sleep(0.5)

    return added, ignored, errors


# ── Scrape une source ──────────────────────────────────────────────────────────
def scrape_source(source: dict) -> tuple[int, int, int]:
    """Retourne (ajoutés, ignorés, erreurs)."""
    added = ignored = errors = 0
    name = source["name"]

    try:
        resp = requests.get(source["url"], timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as exc:
        log.warning("⚠  %s — source inaccessible : %s", name, exc)
        return 0, 0, 1

    if not feed.entries:
        # Pas de flux RSS exploitable : tenter le fallback HTML pour clubs locaux et médias
        if source["category"] in ("club_local", "media"):
            log.info("   %s — RSS vide, tentative fallback HTML", name)
            return scrape_html_news_page(source, resp.content)
        log.info("   %s — aucune entrée dans le feed", name)
        return 0, 0, 0

    for entry in feed.entries:
        url = getattr(entry, "link", None)
        if not url:
            continue

        title   = getattr(entry, "title", "").strip()
        summary_raw = (
            getattr(entry, "summary", "")
            or (entry.get("content", [{}])[0].get("value", "") if entry.get("content") else "")
        )
        summary   = clean_summary(summary_raw)
        image_url = extract_image(entry)
        pub_at    = parse_published(entry)

        # Texte combiné pour détection
        full_text = f"{title} {summary}".lower()

        sport_tags       = detect_sport_tags(full_text)
        mentions_tuffes  = detect_mentions_tuffes(full_text)

        # Si le résumé ne mentionne pas le stade, et la source est un média ou club local,
        # on tente de récupérer l'article complet pour vérifier
        if not mentions_tuffes and source["category"] in ("media", "club_local"):
            article_text = fetch_article_html(url)
            if article_text and detect_mentions_tuffes(article_text):
                mentions_tuffes = True
                # Enrichir les sport_tags avec le contenu complet
                sport_tags = list(set(sport_tags + detect_sport_tags(article_text)))

        # Filtre strict : on garde uniquement les articles mentionnant le stade
        if not mentions_tuffes:
            ignored += 1
            continue

        status = "published"

        row = {
            "title":           title,
            "url":             url,
            "source_name":     name,
            "source_url":      source["url"],
            "published_at":    pub_at,
            "summary":         summary,
            "image_url":       image_url,
            "sport_tags":      sport_tags,
            "mentions_tuffes": mentions_tuffes,
            "status":          status,
        }

        try:
            result = sb.table("press_articles").upsert(row, on_conflict="url").execute()
            if hasattr(result, 'data') and result.data is not None:
                added += 1
            else:
                log.warning("   ✗ upsert sans données pour '%s'", url[:60])
                errors += 1
        except Exception as exc:
            log.error("   ✗ upsert ÉCHOUÉ pour '%s' : %s", url[:60], exc)
            log.error("      → Vérifiez que SUPABASE_KEY est la service_role key ou que les policies RLS INSERT/UPDATE sont actives sur press_articles")
            errors += 1

    return added, ignored, errors


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=== scrape_articles.py démarré ===")
    total_added = total_ignored = total_errors = 0

    for source in SOURCES:
        log.info("→ Scraping : %s", source["name"])
        added, ignored, errors = scrape_source(source)
        log.info("   %s : %d ajoutés, %d ignorés (sans mention stade), %d erreurs",
                 source["name"], added, ignored, errors)
        total_added   += added
        total_ignored += ignored
        total_errors  += errors
        time.sleep(1)  # politesse entre les sources

    log.info("=== Terminé : %d articles ajoutés, %d ignorés, %d erreurs ===",
             total_added, total_ignored, total_errors)


if __name__ == "__main__":
    main()
