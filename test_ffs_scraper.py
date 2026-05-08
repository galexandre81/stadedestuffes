"""
test_ffs_scraper.py
Suite pytest qui valide le scraper FFS sur la fixture HTML locale
`ffs_calendrier_biathlon.html`. Couvre :

  * Structure HTML attendue de la fixture
  * `is_lieu_tuffes` (cas positifs / négatifs)
  * `detect_sport` (biathlon / saut / combiné / fond + fallback default_sport)
  * `parse_ffs_date` (jour unique, plage de jours, mois variés, année manquante)
  * Test d'intégration : extraction des 4 événements Tuffes attendus

Usage :

    python -m pytest test_ffs_scraper.py -v

Mode "dry-run" qui liste les événements détectés sur la fixture :

    python test_ffs_scraper.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# `scrape_events.py` lit SUPABASE_URL / SUPABASE_KEY au moment de l'import
# (création du client). On fournit des valeurs factices pour permettre l'import
# en environnement de test (le client n'est jamais utilisé par cette suite).
os.environ.setdefault("SUPABASE_URL", "https://placeholder.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "placeholder-anon-key")

import pytest
from bs4 import BeautifulSoup

from scrape_events import (
    FFS_DISCIPLINES,
    KEYWORDS_LIEU,
    MOIS_ABBR,
    SPORT_MAP,
    detect_sport,
    is_lieu_tuffes,
    parse_ffs_date,
)


FIXTURE_PATH = Path(__file__).parent / "ffs_calendrier_biathlon.html"


# ── Fixtures pytest ────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def fixture_html() -> str:
    """Contenu brut de la fixture biathlon."""
    assert FIXTURE_PATH.is_file(), f"Fixture introuvable : {FIXTURE_PATH}"
    return FIXTURE_PATH.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def fixture_soup(fixture_html: str) -> BeautifulSoup:
    return BeautifulSoup(fixture_html, "html.parser")


def _make_date_div(day_text: str, month_abbr: str | None, year: str | None) -> BeautifulSoup:
    """Construit un `<div class="el-date cbo-date">` minimal pour les tests unitaires."""
    parts = [f'<span class="date-day">{day_text}</span>']
    if month_abbr is not None:
        parts.append(f"<br/>{month_abbr}")
    if year is not None:
        parts.append(f"<br/>{year}")
    html = '<div class="el-date cbo-date">' + "".join(parts) + "</div>"
    return BeautifulSoup(html, "html.parser").select_one(".el-date")


# ── 1. Structure de la fixture ─────────────────────────────────────────────────


class TestFixtureStructure:
    """La fixture doit exposer la structure HTML attendue par le scraper."""

    def test_fichier_existe(self):
        assert FIXTURE_PATH.is_file(), f"Fixture absente : {FIXTURE_PATH}"

    def test_items_el_present(self, fixture_soup: BeautifulSoup):
        items = fixture_soup.select("div.items-el")
        assert items, "Aucun div.items-el trouvé dans la fixture"
        # Sanity : la page doit contenir plusieurs événements (pas qu'un seul)
        assert len(items) >= 4

    def test_title_text_present(self, fixture_soup: BeautifulSoup):
        h3s = fixture_soup.select("div.items-el h3.title-text")
        assert h3s, "Aucun h3.title-text trouvé dans la fixture"
        # Tous les items devraient avoir un titre
        items = fixture_soup.select("div.items-el")
        assert len(h3s) == len(items)

    def test_cbo_date_present(self, fixture_soup: BeautifulSoup):
        dates = fixture_soup.select("div.items-el div.cbo-date")
        assert dates, "Aucun div.cbo-date trouvé dans la fixture"
        # Chaque cbo-date doit contenir un span.date-day
        for d in dates:
            assert d.select_one("span.date-day") is not None


# ── 2. is_lieu_tuffes ──────────────────────────────────────────────────────────


class TestIsLieuTuffes:
    """Détection de Prémanon / Les Tuffes / CNSNMM."""

    POSITIFS = [
        "SAMSE BIATHLON NATIONAL TOUR 3 - REGIONAL EVENT (LES TUFFES)",
        "BIATHLON REGIONAL U15 (STADE DES TUFFES)",
        "CHAMPIONNATS DE FRANCE (LES TUFFES)",
        "Coupe du Monde de ski de fond - Prémanon",
        "Coupe du monde - Premanon (sans accent)",
        "Stage CNSNMM",
        "Compétition au stade nordique des Tuffes",
    ]

    NEGATIFS = [
        "SAMSE BIATHLON NATIONAL TOUR 1 (BESSANS)",
        "Coupe de France de saut à ski (Chaux-Neuve)",
        "La Clusaz",
        "Coupe d'Autrans",
        "",
        "Sprint à Méribel",
    ]

    @pytest.mark.parametrize("title", POSITIFS)
    def test_positifs(self, title: str):
        assert is_lieu_tuffes(title), f"Devrait matcher : {title!r}"

    @pytest.mark.parametrize("title", NEGATIFS)
    def test_negatifs(self, title: str):
        assert not is_lieu_tuffes(title), f"Ne devrait PAS matcher : {title!r}"

    def test_keywords_lieu_couvre_tuffes_et_premanon(self):
        # Garde-fou : si quelqu'un retire un mot-clé essentiel, on le voit ici.
        joined = " ".join(KEYWORDS_LIEU).lower()
        assert "tuffes" in joined
        assert "prémanon" in joined or "premanon" in joined


# ── 3. detect_sport ────────────────────────────────────────────────────────────


class TestDetectSport:
    """Détection de la discipline depuis le titre + type d'épreuve."""

    @pytest.mark.parametrize(
        "text, expected",
        [
            ("BIATHLON REGIONAL U15", "Biathlon"),
            ("Samse Biathlon National Tour", "Biathlon"),
            ("FFS-BIATH-NA", "Biathlon"),
            ("Coupe de saut à ski", "Saut à ski"),
            ("Concours sur le tremplin K90", "Saut à ski"),
            ("FFS-SAUT-NA", "Saut à ski"),
            ("Combiné nordique senior", "Combiné nordique"),
            ("Combine nordique (sans accent)", "Combiné nordique"),
            ("Nordic Combined World Cup", "Combiné nordique"),
            ("FFS-CN-NA", "Combiné nordique"),
            ("Ski de fond — sprint libre", "Ski de fond"),
            ("Cross-country classic", "Ski de fond"),
            ("Skiathlon dames", "Ski de fond"),
            ("FFS-FOND-NA", "Ski de fond"),
        ],
    )
    def test_detect_known_sports(self, text: str, expected: str):
        # default_sport ne doit PAS être renvoyé quand un mot-clé matche
        assert detect_sport(text, default_sport="ZZZ") == expected

    @pytest.mark.parametrize(
        "text, default, expected",
        [
            ("événement obscur sans mot-clé", "Nordique", "Nordique"),
            ("RAS", "Biathlon", "Biathlon"),
            ("", "Ski de fond", "Ski de fond"),
        ],
    )
    def test_default_fallback(self, text: str, default: str, expected: str):
        assert detect_sport(text, default_sport=default) == expected

    def test_sport_map_couvre_les_4_disciplines(self):
        sports = {sport for _, sport in SPORT_MAP}
        for _, sport in FFS_DISCIPLINES:
            assert sport in sports, f"{sport} absent de SPORT_MAP"


# ── 4. parse_ffs_date ──────────────────────────────────────────────────────────


class TestParseFFSDate:
    """Extraction de date_start/date_end depuis div.el-date."""

    def test_jour_unique(self):
        div = _make_date_div("15", "Jan.", "2026")
        ds, de = parse_ffs_date(div)
        assert ds == "2026-01-15"
        assert de is None  # un seul jour → date_end = None

    def test_plage_de_jours(self):
        div = _make_date_div("04-05", "Jan.", "2026")
        ds, de = parse_ffs_date(div)
        assert ds == "2026-01-04"
        assert de == "2026-01-05"

    def test_plage_de_jours_3_chiffres(self):
        div = _make_date_div("27-29", "Mar.", "2026")
        ds, de = parse_ffs_date(div)
        assert ds == "2026-03-27"
        assert de == "2026-03-29"

    @pytest.mark.parametrize(
        "month_abbr, expected_month",
        [
            ("Jan.", "01"),
            ("Fév.", "02"),
            ("Mar.", "03"),
            ("Avr.", "04"),
            ("Sep.", "09"),
            ("Déc.", "12"),
        ],
    )
    def test_mois_varies(self, month_abbr: str, expected_month: str):
        div = _make_date_div("10", month_abbr, "2026")
        ds, de = parse_ffs_date(div)
        assert ds == f"2026-{expected_month}-10"
        assert de is None

    def test_annee_manquante(self):
        div = _make_date_div("12", "Mar.", year=None)
        ds, de = parse_ffs_date(div)
        assert ds is None
        assert de is None

    def test_mois_manquant(self):
        div = _make_date_div("12", month_abbr=None, year="2026")
        ds, de = parse_ffs_date(div)
        assert ds is None
        assert de is None

    def test_div_none(self):
        ds, de = parse_ffs_date(None)
        assert ds is None and de is None

    def test_jour_invalide(self):
        # "abc" n'est ni un nombre ni une plage → pas de date
        div = _make_date_div("abc", "Jan.", "2026")
        ds, de = parse_ffs_date(div)
        assert ds is None and de is None

    def test_mois_abbr_couvre_tous_les_mois(self):
        # Garde-fou : on doit avoir au moins une entrée par mois 1..12
        months_present = set(MOIS_ABBR.values())
        for m in range(1, 13):
            assert m in months_present, f"Mois {m} absent de MOIS_ABBR"


# ── 5. Intégration sur la fixture ──────────────────────────────────────────────


# Sous-chaînes attendues dans les titres (insensible à la casse / aux dashes
# unicode). On utilise des bouts de titre pour être robuste à un éventuel
# caractère "–" (en-dash) vs "-" dans la fixture.
EXPECTED_TUFFES_TITLE_FRAGMENTS = [
    "SAMSE BIATHLON NATIONAL TOUR 3",
    "BIATHLON REGIONAL U15",
    "SAMSE BIATHLON NATIONAL TOUR 7",
    "CHAMPIONNATS DE FRANCE",
]


def _extract_tuffes_events_from_fixture(soup: BeautifulSoup) -> list[dict]:
    """Reproduit la logique de filtrage du scraper sur la fixture."""
    events: list[dict] = []
    for item in soup.select("div.items-el"):
        title_el = item.select_one("h3.title-text")
        if not title_el:
            continue
        title = title_el.get_text(separator=" ", strip=True)
        if not title:
            continue

        full_text = item.get_text(separator=" ", strip=True)
        if not is_lieu_tuffes(title) and not is_lieu_tuffes(full_text):
            continue

        date_div = item.select_one(".el-date")
        date_start, date_end = parse_ffs_date(date_div)

        type_el = item.select_one(".title-type")
        type_txt = type_el.get_text(separator=" ", strip=True) if type_el else ""
        sport = detect_sport(f"{title} {type_txt}", default_sport="Biathlon")

        events.append(
            {
                "title": title,
                "date_start": date_start,
                "date_end": date_end,
                "sport": sport,
                "type": type_txt,
            }
        )
    return events


class TestIntegrationFixture:
    """Test d'intégration sur ffs_calendrier_biathlon.html."""

    def test_exactement_4_events_tuffes(self, fixture_soup: BeautifulSoup):
        events = _extract_tuffes_events_from_fixture(fixture_soup)
        assert len(events) == 4, (
            f"Attendu 4 événements Tuffes, trouvé {len(events)} : "
            f"{[e['title'] for e in events]}"
        )

    def test_titres_attendus_presents(self, fixture_soup: BeautifulSoup):
        events = _extract_tuffes_events_from_fixture(fixture_soup)
        titles_upper = [e["title"].upper() for e in events]
        for fragment in EXPECTED_TUFFES_TITLE_FRAGMENTS:
            assert any(fragment in t for t in titles_upper), (
                f"Fragment manquant dans les titres extraits : {fragment!r}\n"
                f"Titres trouvés : {titles_upper}"
            )

    def test_chaque_event_a_une_date_parseable(self, fixture_soup: BeautifulSoup):
        events = _extract_tuffes_events_from_fixture(fixture_soup)
        for ev in events:
            assert ev["date_start"] is not None, (
                f"date_start non parsée pour : {ev['title']!r}"
            )
            assert ev["date_start"].startswith("2026-"), (
                f"date_start={ev['date_start']!r} n'est pas en 2026"
            )

    def test_chaque_event_a_un_sport(self, fixture_soup: BeautifulSoup):
        events = _extract_tuffes_events_from_fixture(fixture_soup)
        valid_sports = {"Biathlon", "Combiné nordique", "Ski de fond", "Saut à ski"}
        for ev in events:
            assert ev["sport"] in valid_sports, (
                f"Sport inattendu pour {ev['title']!r} : {ev['sport']!r}"
            )
        # Fixture biathlon → tous les events doivent être en Biathlon
        sports = {ev["sport"] for ev in events}
        assert sports == {"Biathlon"}, (
            f"Fixture biathlon mais sports détectés = {sports}"
        )

    def test_pas_de_doublons(self, fixture_soup: BeautifulSoup):
        events = _extract_tuffes_events_from_fixture(fixture_soup)
        keys = [(e["title"], e["date_start"]) for e in events]
        assert len(keys) == len(set(keys)), f"Doublons détectés : {keys}"


# ── Live test (skippé par défaut) ──────────────────────────────────────────────


@pytest.mark.skipif(
    os.environ.get("LIVE_TEST") != "1",
    reason="Live test désactivé (mettre LIVE_TEST=1 pour l'activer)",
)
class TestLiveFFS:
    """Test live qui requête ffs.fr/calendrier/. Désactivé par défaut."""

    def test_live_calendrier_biathlon(self, tmp_path: Path):
        import requests

        from scrape_events import (
            CALENDRIER_BASE_URL,
            HEADERS,
            TIMEOUT,
            season_start_date,
        )

        url = (
            f"{CALENDRIER_BASE_URL}"
            f"?discipline=2&date_du={season_start_date()}&filters=1&page_number=1"
        )
        resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()

        # Snapshot
        snap_dir = Path(__file__).parent / "tests" / "snapshots"
        snap_dir.mkdir(parents=True, exist_ok=True)
        from datetime import datetime

        snap_file = snap_dir / f"ffs_live_{datetime.now():%Y%m%d}.html"
        snap_file.write_bytes(resp.content)

        soup = BeautifulSoup(resp.content, "html.parser")
        assert soup.select("div.items-el"), "Structure FFS changée : plus de div.items-el"


# ── Mode dry-run : `python test_ffs_scraper.py` ────────────────────────────────


def _run_dry_run() -> int:
    """Affiche les événements Tuffes détectés sur la fixture. Code de sortie = 0
    si exactement 4 events trouvés, sinon 1."""
    print(f"=== Dry-run : {FIXTURE_PATH.name} ===")
    if not FIXTURE_PATH.is_file():
        print(f"ERREUR : fixture introuvable : {FIXTURE_PATH}")
        return 2

    soup = BeautifulSoup(FIXTURE_PATH.read_text(encoding="utf-8"), "html.parser")
    total_items = len(soup.select("div.items-el"))
    events = _extract_tuffes_events_from_fixture(soup)

    print(f"Items div.items-el dans la fixture : {total_items}")
    print(f"Événements filtrés Tuffes/Prémanon  : {len(events)}\n")

    for i, ev in enumerate(events, 1):
        date_str = ev["date_start"] or "??"
        if ev["date_end"]:
            date_str += f" -> {ev['date_end']}"
        print(f"{i}. [{ev['sport']}] {ev['title']}")
        print(f"   date  : {date_str}")
        if ev["type"]:
            print(f"   type  : {ev['type']}")
        print()

    if len(events) != 4:
        print(f"ATTENTION : 4 événements attendus, {len(events)} trouvés.")
        return 1
    print("OK : 4 événements Tuffes détectés sur la fixture.")
    return 0


if __name__ == "__main__":
    sys.exit(_run_dry_run())
