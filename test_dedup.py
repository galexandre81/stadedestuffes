"""test_dedup.py - Tests TDD pour le module dedup."""
import pytest
from dedup import (
    normalize_text, signature_tokens, jaccard,
    parse_french_date, is_future_date, MOIS_FR,
    event_interval, intervals_overlap, sports_conflict,
    find_duplicate, merge_into_existing, source_outranks, warn_if_date_shift,
)


class TestNormalizeText:
    def test_lowercase_no_accents(self):
        assert normalize_text("Coupe du Monde") == "coupe du monde"

    def test_strip_accents(self):
        assert normalize_text("Champ. de France 2026") == "champ de france 2026"

    def test_punctuation_to_space(self):
        assert normalize_text("Tour-de-Ski") == "tour de ski"

    def test_empty(self):
        assert normalize_text("") == ""

    def test_collapse_spaces(self):
        assert normalize_text("a   b") == "a b"


class TestSignatureTokens:
    def test_filters_stopwords(self):
        toks = signature_tokens("Coupe du Monde de Ski")
        assert "ski" not in toks  # ski est stopword
        assert "coupe" in toks
        assert "monde" in toks

    def test_filters_short(self):
        toks = signature_tokens("le ok")
        assert toks == set()

    def test_handles_accents(self):
        toks = signature_tokens("Championnats régionaux")
        assert "championnats" in toks
        assert "regionaux" in toks


class TestJaccard:
    def test_identical(self):
        assert jaccard({"a", "b"}, {"a", "b"}) == 1.0

    def test_disjoint(self):
        assert jaccard({"a"}, {"b"}) == 0.0

    def test_partial(self):
        assert jaccard({"a", "b"}, {"a", "c"}) == pytest.approx(1 / 3)

    def test_both_empty(self):
        assert jaccard(set(), set()) == 1.0

    def test_one_empty(self):
        assert jaccard({"a"}, set()) == 0.0


class TestParseFrenchDate:
    def test_range(self):
        ds, de = parse_french_date("du 27 au 29 mars 2026")
        assert ds == "2026-03-27" and de == "2026-03-29"

    def test_simple(self):
        ds, de = parse_french_date("Le 15 mars 2026 aura lieu")
        assert ds == "2026-03-15" and de is None

    def test_numeric(self):
        ds, de = parse_french_date("Date : 31/01/2026")
        assert ds == "2026-01-31"

    def test_decembre_accent(self):
        ds, _ = parse_french_date("le 27 décembre 2025")
        assert ds == "2025-12-27"

    def test_no_date(self):
        ds, de = parse_french_date("Aucune date ici")
        assert ds is None and de is None

    def test_empty(self):
        ds, de = parse_french_date("")
        assert ds is None and de is None


class TestIsFutureDate:
    def test_future(self):
        assert is_future_date("2099-01-01") is True

    def test_past(self):
        assert is_future_date("2000-01-01") is False

    def test_invalid(self):
        assert is_future_date("foo") is False

    def test_today_tolerated(self):
        from datetime import date
        assert is_future_date(date.today().isoformat()) is True


# ── Stub Supabase minimal ─────────────────────────────────────────────────────

class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows
        self.updates = None
        self.updated_id = None

    def select(self, *_a, **_k):
        return self

    def gte(self, _col, value):
        self._rows = [r for r in self._rows if r.get("date_start", "") >= value]
        return self

    def lte(self, _col, value):
        self._rows = [r for r in self._rows if r.get("date_start", "") <= value]
        return self

    def update(self, updates):
        self.updates = updates
        return self

    def eq(self, _col, value):
        self.updated_id = value
        return self

    def execute(self):
        return type("Res", (), {"data": list(self._rows)})()


class _FakeSB:
    def __init__(self, rows):
        self.query = _FakeQuery(rows)

    def table(self, _name):
        return self.query


# ── Intervalles ───────────────────────────────────────────────────────────────

class TestEventInterval:
    def test_date_end_absente_retombe_sur_start(self):
        from datetime import date
        assert event_interval({"date_start": "2027-01-01"}) == (date(2027, 1, 1), date(2027, 1, 1))

    def test_date_end_incoherente_est_bornee(self):
        start, end = event_interval({"date_start": "2027-01-05", "date_end": "2027-01-01"})
        assert start == end

    def test_date_start_illisible(self):
        assert event_interval({"date_start": "foo"}) == (None, None)


class TestIntervalsOverlap:
    def test_chevauchement_reel(self):
        """Tour de Ski du 1er au 3 janvier contre une entrée datée du 3."""
        a = event_interval({"date_start": "2027-01-01", "date_end": "2027-01-03"})
        b = event_interval({"date_start": "2027-01-03"})
        assert intervals_overlap(a, b) is True

    def test_jours_consecutifs_toleres(self):
        a = event_interval({"date_start": "2026-03-28"})
        b = event_interval({"date_start": "2026-03-29"})
        assert intervals_overlap(a, b) is True

    def test_trop_eloignes(self):
        a = event_interval({"date_start": "2026-03-01"})
        b = event_interval({"date_start": "2026-03-20"})
        assert intervals_overlap(a, b) is False

    def test_date_invalide(self):
        assert intervals_overlap(event_interval({"date_start": "x"}),
                                 event_interval({"date_start": "2026-03-01"})) is False


class TestSportsConflict:
    def test_sports_differents(self):
        assert sports_conflict("Biathlon", "Ski de fond") is True

    def test_sport_inconnu_ne_bloque_pas(self):
        assert sports_conflict("Biathlon", None) is False
        assert sports_conflict("", "Ski de fond") is False

    def test_meme_sport_insensible_a_la_casse(self):
        assert sports_conflict("biathlon", "Biathlon ") is False


# ── find_duplicate : les trois cas observés en base ───────────────────────────

class TestFindDuplicateCasReels:
    def test_tour_de_ski_multi_jours_est_detecte(self):
        """L'écart de 2 jours dépassait l'ancienne fenêtre de plus ou moins 1 jour."""
        existant = {
            "id": 1, "title": "FIS Tour de Ski 2027 — Coupe du monde",
            "date_start": "2027-01-01", "date_end": "2027-01-03",
            "sport": "Ski de fond",
        }
        candidat = {
            "title": "FIS Tour de Ski 2027 — Coupe du monde",
            "date_start": "2027-01-03", "date_end": None,
            "sport": "Ski de fond",
        }
        assert find_duplicate(_FakeSB([existant]), candidat) is not None

    def test_championnats_de_france_disciplines_differentes_non_fusionnes(self):
        """Titre identique, jours consécutifs, mais biathlon puis ski de fond."""
        existant = {
            "id": 2, "title": "CHAMPIONNATS DE FRANCE (LES TUFFES)",
            "date_start": "2026-03-27", "date_end": None, "sport": "Biathlon",
        }
        candidat = {
            "title": "CHAMPIONNATS DE FRANCE (LES TUFFES)",
            "date_start": "2026-03-28", "date_end": None, "sport": "Ski de fond",
        }
        assert find_duplicate(_FakeSB([existant]), candidat) is None

    def test_meme_discipline_jours_consecutifs_fusionne(self):
        """SAMSE National Tour 6, 14 et 15 mars, même discipline."""
        existant = {
            "id": 3, "title": "SAMSE NATIONAL TOUR 6 (LAMOURA – LES TUFFES)",
            "date_start": "2026-03-14", "date_end": None, "sport": "Ski de fond",
        }
        candidat = {
            "title": "SAMSE NATIONAL TOUR 6 (LAMOURA – LES TUFFES)",
            "date_start": "2026-03-15", "date_end": None, "sport": "Ski de fond",
        }
        assert find_duplicate(_FakeSB([existant]), candidat) is not None

    def test_evenements_sans_rapport_non_fusionnes(self):
        existant = {
            "id": 4, "title": "LA TRAVERSEE DU MASSACRE (PREMANON)",
            "date_start": "2026-03-01", "date_end": None, "sport": "Ski de fond",
        }
        candidat = {
            "title": "BIATHLON REGIONAL U15 (STADE DES TUFFES)",
            "date_start": "2026-02-28", "date_end": None, "sport": "Biathlon",
        }
        assert find_duplicate(_FakeSB([existant]), candidat) is None


class TestMergeElargitLIntervalle:
    def test_date_start_reculee_et_date_end_etendue(self):
        existant = {
            "id": 9, "title": "FIS Tour de Ski 2027", "date_start": "2027-01-03",
            "date_end": None, "sport": "Ski de fond", "source_name": "A",
        }
        candidat = {
            "title": "FIS Tour de Ski 2027", "date_start": "2027-01-01",
            "date_end": "2027-01-03", "sport": "Ski de fond", "source_name": "A",
        }
        sb = _FakeSB([existant])
        assert merge_into_existing(sb, existant, candidat) is True
        assert sb.query.updates["date_start"] == "2027-01-01"
        assert sb.query.updates["date_end"] == "2027-01-03"

    def test_pas_de_retrecissement(self):
        existant = {
            "id": 10, "title": "X", "date_start": "2027-01-01",
            "date_end": "2027-01-03", "sport": "Ski de fond", "source_name": "A",
        }
        candidat = {
            "title": "X", "date_start": "2027-01-02", "date_end": "2027-01-02",
            "sport": "Ski de fond", "source_name": "A",
        }
        sb = _FakeSB([existant])
        merge_into_existing(sb, existant, candidat)
        updates = sb.query.updates or {}
        assert "date_start" not in updates
        assert "date_end" not in updates


class TestPreferenceLignePubliee:
    def test_a_score_egal_la_ligne_publiee_gagne(self):
        """Un doublon mis de côté en pending ne doit pas capter l'enrichissement."""
        pending = {
            "id": 50, "title": "FIS Tour de Ski 2027", "date_start": "2027-01-03",
            "date_end": None, "sport": "Ski de fond", "status": "pending",
        }
        publie = {
            "id": 51, "title": "FIS Tour de Ski 2027", "date_start": "2027-01-01",
            "date_end": "2027-01-03", "sport": "Ski de fond", "status": "published",
        }
        candidat = {
            "title": "FIS Tour de Ski 2027", "date_start": "2027-01-02",
            "date_end": None, "sport": "Ski de fond",
        }
        trouve = find_duplicate(_FakeSB([pending, publie]), candidat)
        assert trouve is not None and trouve["id"] == 51


class TestAutoriteDeSource:
    def test_ffs_prime_sur_un_agregateur(self):
        assert source_outranks("FFS calendrier", "Ski-Nordique.net — calendrier national FFS") is True

    def test_un_agregateur_ne_prime_pas_sur_la_ffs(self):
        assert source_outranks("Ski-Nordique.net", "FFS calendrier") is False

    def test_ffs_ne_prime_pas_sur_elle_meme(self):
        assert source_outranks("FFS calendrier", "FFS calendrier") is False

    def test_le_sport_est_corrige_par_la_source_qui_fait_autorite(self):
        """Le sport FFS vient du code d'épreuve, il prime sur une déduction de titre."""
        existant = {
            "id": 20, "title": "SAMSE National Tour Biathlon — Étape 2",
            "date_start": "2026-12-19", "date_end": None,
            "sport": "Ski de fond", "source_name": "Ski-Nordique.net",
            "notes": "Première étape sur neige de la saison.",
        }
        candidat = {
            "title": "SAMSE BIATHLON NATIONAL TOUR 2 (LES TUFFES)",
            "date_start": "2026-12-19", "date_end": None,
            "sport": "Biathlon", "source_name": "FFS calendrier",
            "notes": "Type d’épreuves : FFS-BIATH-NA",
        }
        sb = _FakeSB([existant])
        assert merge_into_existing(sb, existant, candidat) is True
        assert sb.query.updates["sport"] == "Biathlon"
        # La prose de la fiche existante n'est pas remplacée par le code FFS.
        assert "notes" not in sb.query.updates

    def test_le_sport_n_est_pas_touche_par_une_source_ordinaire(self):
        existant = {
            "id": 21, "title": "X", "date_start": "2026-12-19", "date_end": None,
            "sport": "Ski de fond", "source_name": "FFS calendrier",
        }
        candidat = {
            "title": "X", "date_start": "2026-12-19", "date_end": None,
            "sport": "Biathlon", "source_name": "Un club",
        }
        sb = _FakeSB([existant])
        merge_into_existing(sb, existant, candidat)
        assert "sport" not in (sb.query.updates or {})


class TestAlerteDecalage:
    def test_alerte_quand_la_meme_epreuve_est_reprogrammee_plus_loin(self, caplog):
        existant = {
            "id": 30, "title": "SAMSE National Tour Biathlon — Étape 2",
            "date_start": "2026-12-19", "date_end": None, "sport": "Biathlon",
        }
        candidat = {
            "title": "SAMSE National Tour Biathlon — Étape 2",
            "date_start": "2027-01-09", "date_end": None, "sport": "Biathlon",
            "source_name": "FFS calendrier",
        }
        import logging
        with caplog.at_level(logging.WARNING):
            warn_if_date_shift(_FakeSB([existant]), candidat)
        assert "decalage possible" in caplog.text

    def test_pas_d_alerte_pour_un_evenement_sans_rapport(self, caplog):
        existant = {
            "id": 31, "title": "LA TRAVERSEE DU MASSACRE (PREMANON)",
            "date_start": "2026-12-19", "date_end": None, "sport": "Ski de fond",
        }
        candidat = {
            "title": "Cyclo Haut-Jura", "date_start": "2027-01-09",
            "date_end": None, "sport": "Ski de fond", "source_name": "FFS calendrier",
        }
        import logging
        with caplog.at_level(logging.WARNING):
            warn_if_date_shift(_FakeSB([existant]), candidat)
        assert "decalage possible" not in caplog.text

    def test_pas_d_alerte_quand_la_deduplication_a_deja_fusionne(self, caplog):
        """Chevauchement : c'est le travail de find_duplicate, pas une alerte."""
        existant = {
            "id": 32, "title": "CHAMPIONNATS DE FRANCE (LES TUFFES)",
            "date_start": "2026-03-28", "date_end": "2026-03-29", "sport": "Ski de fond",
        }
        candidat = {
            "title": "CHAMPIONNATS DE FRANCE (LES TUFFES)",
            "date_start": "2026-03-29", "date_end": None, "sport": "Ski de fond",
            "source_name": "FFS calendrier",
        }
        import logging
        with caplog.at_level(logging.WARNING):
            warn_if_date_shift(_FakeSB([existant]), candidat)
        assert "decalage possible" not in caplog.text
