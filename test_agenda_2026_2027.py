"""
test_agenda_2026_2027.py
Tests des ajouts « agenda saison 2026-2027 » :

  * `parse_date_loose` — plages avec/sans année, jour unique, formats abrégés
  * `_future_year` — une date sans millésime ne tombe jamais dans le passé
  * `prepare_row` — retrait des colonnes étendues quand Supabase ne les a pas
  * `CONFIRMED_EVENTS` — cohérence des 4 épreuves confirmées à la main
  * parité avec le seed front `redesign/events-seed.js`

Usage : python -m pytest test_agenda_2026_2027.py -v
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("SUPABASE_URL", "https://placeholder.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "placeholder-anon-key")

import pytest

import scrape_events as se

REF = datetime(2026, 8, 27, tzinfo=timezone.utc)  # milieu d'intersaison


class TestParseDateLoose:
    @pytest.mark.parametrize("text, expected", [
        ("du 19 au 20 décembre 2026", ("2026-12-19", "2026-12-20")),
        ("19-20 décembre 2026", ("2026-12-19", "2026-12-20")),
        ("19 – 20 décembre 2026", ("2026-12-19", "2026-12-20")),
        ("Tour de Ski du 1 au 3 janvier 2027", ("2027-01-01", "2027-01-03")),
        ("27-28 févr. 2027 à Prémanon", ("2027-02-27", "2027-02-28")),
        ("Cyclo Haut-Jura le 5 juillet 2026", ("2026-07-05", None)),
        ("12/01/2027", ("2027-01-12", None)),
    ])
    def test_formats(self, text, expected):
        assert se.parse_date_loose(text, reference=REF) == expected

    def test_range_wins_over_single_day(self):
        # Le parseur strict ne voyait que « 20 décembre » dans « 19-20 décembre ».
        start, end = se.parse_date_loose("19-20 décembre 2026", reference=REF)
        assert start == "2026-12-19"
        assert end == "2026-12-20"

    @pytest.mark.parametrize("text", ["", None, "aucune date ici", "saison 2026-2027"])
    def test_no_date(self, text):
        assert se.parse_date_loose(text, reference=REF) == (None, None)


class TestFutureYear:
    def test_missing_year_is_never_in_the_past(self):
        # Fin août 2026 : « 27 et 28 février » désigne février 2027, pas 2026.
        start, end = se.parse_date_loose("les 27 et 28 février", reference=REF)
        assert start == "2027-02-27"
        assert end == "2027-02-28"

    def test_missing_year_keeps_upcoming_season(self):
        start, _ = se.parse_date_loose("le 19 décembre", reference=REF)
        assert start == "2026-12-19"

    def test_explicit_year_wins(self):
        start, _ = se.parse_date_loose("le 19 décembre 2028", reference=REF)
        assert start == "2028-12-19"


class TestPrepareRow:
    def _row(self):
        return {"title": "T", "date_start": "2027-01-01", "level": "International",
                "organizer": "FIS", "is_highlight": True}

    def test_keeps_extended_columns_when_supported(self, monkeypatch):
        monkeypatch.setattr(se, "extended_columns_supported", lambda: True)
        assert se.prepare_row(self._row()) == self._row()

    def test_strips_extended_columns_when_unsupported(self, monkeypatch):
        monkeypatch.setattr(se, "extended_columns_supported", lambda: False)
        row = se.prepare_row(self._row())
        assert row == {"title": "T", "date_start": "2027-01-01"}


PUBLISHED_CONFIRMED = [e for e in se.CONFIRMED_EVENTS
                       if e.get("status", "published") == "published"]


class TestConfirmedEvents:
    def test_expected_count(self):
        # 6 épreuves publiées + la Transju'Trails en attente de validation
        assert len(se.CONFIRMED_EVENTS) == 7
        assert len(PUBLISHED_CONFIRMED) == 6

    def test_no_duplicate_dates_and_titles(self):
        keys = [(e["title"], e["date_start"]) for e in se.CONFIRMED_EVENTS]
        assert len(keys) == len(set(keys))

    def test_transju_jeunes_has_no_invented_day(self):
        """Le jour n'étant pas publié, l'épreuve est marquée date_tbd."""
        transju = next(e for e in se.CONFIRMED_EVENTS if "Jeunes" in e["title"])
        assert transju["date_tbd"] is True
        assert transju["date_start"].startswith("2027-01")

    def test_transju_trails_stays_pending(self):
        """Parcours 2027 non publié → file d'attente admin, pas le site."""
        trails = next(e for e in se.CONFIRMED_EVENTS if "Trails" in e["title"])
        assert trails["status"] == "pending"

    @pytest.mark.parametrize("event", se.CONFIRMED_EVENTS, ids=lambda e: e["title"])
    def test_shape(self, event):
        assert event["title"]
        assert event["sport"]
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", event["date_start"])
        if event["date_end"]:
            assert event["date_end"] >= event["date_start"]
        assert event["level"] in ("Régional", "National", "International")
        assert event["source_url"].startswith("https://")
        assert event["organizer"]

    def test_tour_de_ski_is_the_highlight(self):
        highlights = [e for e in se.CONFIRMED_EVENTS if e.get("is_highlight")]
        assert len(highlights) == 1
        assert "Tour de Ski" in highlights[0]["title"]
        assert highlights[0]["level"] == "International"

    def test_seed_matches_frontend(self):
        """Le seed JS du front doit décrire les mêmes épreuves publiées."""
        js = Path("redesign/events-seed.js").read_text(encoding="utf-8")
        for event in PUBLISHED_CONFIRMED:
            assert f"'{event['date_start']}'" in js, event["date_start"]
            # le titre exact, tel qu'affiché
            assert event["title"] in js, event["title"]

    def test_pending_events_stay_out_of_the_frontend_seed(self):
        js = Path("redesign/events-seed.js").read_text(encoding="utf-8")
        for event in se.CONFIRMED_EVENTS:
            if event.get("status") == "pending":
                assert event["title"] not in js, event["title"]


class TestSourcesRegistered:
    def test_new_sources_are_wired(self):
        assert se.VELO_CYCLOSPORT_URL.startswith("https://www.velo-cyclosport.com/")
        assert "ski-nordique.net" in se.SKI_NORDIQUE_CALENDAR_URL
        assert any("worldcupstationdesrousses" in u for u in se.WORLD_CUP_ROUSSES_URLS)

    def test_scrapers_are_callable(self):
        for name in ("scrape_velo_cyclosport", "scrape_ski_nordique_calendrier",
                     "scrape_world_cup_rousses", "seed_confirmed_events"):
            assert callable(getattr(se, name))


class TestSeedResilience:
    def test_retries_without_source_type_on_error(self, monkeypatch):
        """Un schéma qui refuse source_type='manual' ne doit rien faire perdre."""
        calls = []

        def fake_upsert(row):
            calls.append(row)
            return "error" if "source_type" in row else "added"

        monkeypatch.setattr(se, "upsert_extended", fake_upsert)
        added, merged, errors = se.seed_confirmed_events()

        assert added == len(se.CONFIRMED_EVENTS)
        assert errors == 0
        assert len(calls) == 2 * len(se.CONFIRMED_EVENTS)
        assert all("source_type" not in c for c in calls[1::2])

    def test_no_retry_when_first_upsert_succeeds(self, monkeypatch):
        calls = []
        monkeypatch.setattr(se, "upsert_extended", lambda row: calls.append(row) or "added")
        added, _, errors = se.seed_confirmed_events()
        assert (added, errors) == (len(se.CONFIRMED_EVENTS), 0)
        assert len(calls) == len(se.CONFIRMED_EVENTS)


class TestListingAndTransjuScrapers:
    def test_listing_blocks_picks_up_a_season_table(self, monkeypatch):
        """Un tableau de saison sans lien détail (type Haut-Jura Ski) est capté."""
        from bs4 import BeautifulSoup
        html = """
        <table><tbody>
          <tr><td>Samedi 24 octobre 2026</td><td>Sprint Classique Les Tuffes - U11 à Sen</td></tr>
          <tr><td>Samedi 7 novembre 2026</td><td>Sortie club à Bois d'Amont</td></tr>
        </tbody></table>
        """
        rows = []
        monkeypatch.setattr(se, "upsert_extended", lambda row: rows.append(row) or "added")
        added, _, _ = se.scrape_listing_blocks(
            "Haut-Jura Ski", BeautifulSoup(html, "html.parser"),
            "https://www.hautjuraski.fr/evenements",
        )
        assert added == 1
        assert rows[0]["date_start"] == "2026-10-24"
        assert "Sprint Classique" in rows[0]["title"]
        assert rows[0]["status"] == "pending"   # à valider en admin
        assert rows[0]["sport"] == "Ski de fond"

    def test_transju_page_without_a_day_is_marked_tbd(self, monkeypatch):
        """« Janvier 2027 » sans jour → date_tbd, pas de jour inventé."""
        from bs4 import BeautifulSoup

        class FakeResp:
            content = ("<html><body><h1>La Transju'Jeunes</h1>"
                       "<p>Janvier 2027 — départ du Stade Nordique des Tuffes à "
                       "Prémanon.</p></body></html>").encode()

            def raise_for_status(self):
                return None

        rows = []
        monkeypatch.setattr(se.requests, "get", lambda *a, **k: FakeResp())
        monkeypatch.setattr(se.time, "sleep", lambda *a: None)
        monkeypatch.setattr(se, "upsert_extended", lambda row: rows.append(row) or "added")
        monkeypatch.setattr(se, "TRANSJU_EVENT_PAGES", [
            {"url": "https://www.latransju.com/la-transjeunes/",
             "sport": "Ski de fond", "label": "La Transju'Jeunes"},
        ])
        added, _, errors = se.scrape_transju_event_pages()
        assert (added, errors) == (1, 0)
        assert rows[0]["date_tbd"] is True
        assert rows[0]["date_start"] == "2027-01-31"
        assert rows[0]["status"] == "published"
