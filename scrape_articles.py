"""
scrape_articles.py
Scraper RSS pour la table press_articles (Supabase).
Sources : médias nordiques nationaux, FFS, clubs locaux.
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

# ── Sources RSS ────────────────────────────────────────────────────────────────
SOURCES = [
    # Médias nationaux nordiques
    {"name": "NordicMag",          "url": "https://www.nordicmag.info/feed/",               "category": "media"},
    {"name": "Ski-Nordique.net",   "url": "https://www.ski-nordique.net/feed",               "category": "media"},
    {"name": "Biathlon Magazine",  "url": "https://www.biathlonmag.fr/feed",                "category": "media"},
    # FFS
    {"name": "FFS Fond",           "url": "https://www.ffs.fr/rss/actualites/fond",          "category": "ffs"},
    {"name": "FFS Biathlon",       "url": "https://www.ffs.fr/rss/actualites/biathlon",      "category": "ffs"},
    {"name": "FFS Saut",           "url": "https://www.ffs.fr/rss/actualites/saut",          "category": "ffs"},
    {"name": "FFS Combiné",        "url": "https://www.ffs.fr/rss/actualites/combine-nordique", "category": "ffs"},
    # Clubs locaux (WordPress RSS)
    {"name": "SC Grandvaux",       "url": "https://scgrandvaux.fr/feed/",                    "category": "club"},
    {"name": "CSR Pontarlier",     "url": "https://csrpontarlier.fr/feed/",                  "category": "club"},
    {"name": "Saugeathlon",        "url": "https://saugeathlon.fr/feed/",                    "category": "club"},
    {"name": "Haut-Jura Léman",    "url": "https://hautjuraleman.com/feed/",                 "category": "club"},
]

# ── Mots-clés ──────────────────────────────────────────────────────────────────
KEYWORDS_TUFFES = [
    "tuffes", "prémanon", "premanon", "cnsnmm", "jason lamy"
]

KEYWORDS_REGIONAL = [
    "jura", "les rousses", "massif jurassien",
    "bourgogne-franche-comté", "bourgogne franche comté", "bfc"
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


def detect_regional(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in KEYWORDS_REGIONAL)


def extract_image(entry) -> str | None:
    """Cherche une image dans media:content, enclosures, ou media:thumbnail."""
    # media:content
    media = getattr(entry, "media_content", None)
    if media and isinstance(media, list):
        for m in media:
            if isinstance(m, dict) and m.get("url"):
                return m["url"]

    # media:thumbnail
    thumb = getattr(entry, "media_thumbnail", None)
    if thumb and isinstance(thumb, list) and thumb[0].get("url"):
        return thumb[0]["url"]

    # enclosures (podcasts / images)
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


# ── Scrape une source ──────────────────────────────────────────────────────────
def scrape_source(source: dict) -> tuple[int, int, int]:
    """Retourne (ajoutés, déjà_présents, erreurs)."""
    added = already = errors = 0
    name = source["name"]

    try:
        # feedparser avec timeout via requests
        resp = requests.get(source["url"], timeout=10, headers={"User-Agent": "stadedestuffes-bot/1.0"})
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as exc:
        log.warning("⚠  %s — source inaccessible : %s", name, exc)
        return 0, 0, 1

    if not feed.entries:
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
        regional         = detect_regional(full_text)

        # Filtrage : on garde l'article si…
        # - médias nationaux / FFS : toujours publiés
        # - clubs locaux : toujours publiés
        # - sinon : seulement si Tuffes / régional / sport nordique trouvé
        if source["category"] in ("media", "ffs", "club"):
            status = "published"
        elif mentions_tuffes or regional or sport_tags:
            status = "published"
        else:
            continue  # article hors-scope, on ignore

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
            # Supabase upsert : si la ligne était déjà là, elle est mise à jour
            # On ne peut pas distinguer insert vs update facilement, on compte juste
            added += 1
        except Exception as exc:
            log.warning("   ✗ upsert échoué pour '%s' : %s", url[:60], exc)
            errors += 1

    return added, already, errors


# ── Scrape FFS fallback (si RSS FFS renvoie 0 entrées) ────────────────────────
FFS_SECTIONS = {
    "FFS Fond":    ("https://www.ffs.fr/actualites?discipline=fond",    "fond"),
    "FFS Biathlon":("https://www.ffs.fr/actualites?discipline=biathlon","biathlon"),
}

def scrape_ffs_html(section_name: str, url: str, sport: str) -> int:
    """Fallback scraping HTML FFS si RSS non disponible."""
    inserted = 0
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "stadedestuffes-bot/1.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Les articles FFS sont dans des <article> ou <div class="news-item">
        items = soup.select("article") or soup.select(".news-item") or soup.select(".article-item")
        for item in items[:20]:
            a_tag = item.find("a", href=True)
            if not a_tag:
                continue
            href = a_tag["href"]
            if not href.startswith("http"):
                href = "https://www.ffs.fr" + href
            title_tag = item.find(["h2", "h3", "h4"])
            title = title_tag.get_text(strip=True) if title_tag else a_tag.get_text(strip=True)
            if not title:
                continue

            row = {
                "title":           title,
                "url":             href,
                "source_name":     section_name,
                "source_url":      url,
                "sport_tags":      [sport],
                "mentions_tuffes": detect_mentions_tuffes(title),
                "status":          "published",
            }
            try:
                sb.table("press_articles").upsert(row, on_conflict="url").execute()
                inserted += 1
            except Exception:
                pass
    except Exception as exc:
        log.warning("FFS HTML fallback échoué (%s) : %s", section_name, exc)
    return inserted


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=== scrape_articles.py démarré ===")
    total_added = total_errors = 0

    for source in SOURCES:
        log.info("→ Scraping : %s", source["name"])
        added, _, errors = scrape_source(source)
        log.info("   %s : %d upsertés, %d erreurs", source["name"], added, errors)
        total_added  += added
        total_errors += errors
        time.sleep(1)  # politesse entre les sources

    log.info("=== Terminé : %d articles upsertés, %d erreurs ===", total_added, total_errors)


if __name__ == "__main__":
    main()
