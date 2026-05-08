# Phase B — Dedup + scrapers + filtre articles Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ajouter la déduplication d'événements, 5 nouveaux scrapers (CNSNMM, Cyclo Haut Jura, Transju', Jura Tourisme, clubs locaux), durcir le filtre des articles, ajouter un fallback HTML pour sources sans RSS.

**Architecture:** Nouveau module `dedup.py`, nouveau script de migration SQL (exécuté à la main), refactor de `scrape_articles.py`, ajout de scrapers dans `scrape_events.py`, nouveau module de tests `test_dedup.py`.

**Tech Stack:** Python 3.13, pytest, beautifulsoup4, requests, feedparser, supabase-py.

**Pre-requis:** Phase A terminée (fixture FFS validée).

**Source de vérité détaillée :** `docs/plan-contenu-claude-code-v2.md` — la présente plan est l'enchaînement TDD ; les snippets de code y sont dans le brief.

---

## File Structure

- **À créer** :
  - `migration_dedup.sql` à la racine (exécution manuelle par utilisateur)
  - `dedup.py` à la racine
  - `test_dedup.py` à la racine
  - `tests/fixtures/` pour les HTML de test des nouveaux scrapers

- **À modifier** :
  - `scrape_articles.py` (sources + filtre + fallback HTML)
  - `scrape_events.py` (5 nouvelles fonctions + branchement dans main)

---

## Task B.1 : Migration SQL (HUMAN-IN-THE-LOOP)

- [ ] **Step 1.1: Créer migration_dedup.sql**

Contenu (depuis `docs/plan-contenu-claude-code-v2.md` Partie 0) :

```sql
ALTER TABLE events
  ADD COLUMN IF NOT EXISTS additional_sources JSONB DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_events_date_start ON events(date_start);
```

- [ ] **Step 1.2: Commit**

```powershell
git add migration_dedup.sql
git commit -m "feat: migration SQL ajout colonne additional_sources + index date_start"
```

- [ ] **Step 1.3: HAND-OFF UTILISATEUR**

> "Migration SQL créée. **À toi de l'exécuter dans Supabase SQL editor**, puis confirme-moi que c'est passé pour que je continue. Sans cette migration, les `upsert_event` échoueront sur l'écriture de `additional_sources`."

**ATTENDRE confirmation explicite avant Task B.2.**

## Task B.2 : Module dedup.py — squelette + tests

- [ ] **Step 2.1: Créer test_dedup.py vide avec imports**

Fichier `test_dedup.py` :

```python
import pytest
from dedup import (
    normalize_text, signature_tokens, jaccard,
    parse_french_date, is_future_date, MOIS_FR,
)
```

Lancer pour valider erreur d'import attendue :

```powershell
pytest test_dedup.py
```

Expected: ERROR — module `dedup` introuvable.

- [ ] **Step 2.2: Créer dedup.py squelette**

Créer `dedup.py` avec les imports et stubs (pas d'implémentation) :

```python
"""dedup.py - Helpers de déduplication d'événements."""
import re
import unicodedata
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)

STOPWORDS = set()  # à remplir
MOIS_FR = {}  # à remplir

def normalize_text(text: str) -> str: ...
def signature_tokens(title: str) -> set: ...
def jaccard(a: set, b: set) -> float: ...
def parse_french_date(text: str): ...
def is_future_date(date_str: str, tolerate_today: bool = True) -> bool: ...
def find_duplicate(sb, candidate_row: dict, similarity_threshold: float = 0.5): ...
def merge_into_existing(sb, existing: dict, new_row: dict) -> bool: ...
def upsert_event(sb, row: dict) -> str: ...
```

- [ ] **Step 2.3: Re-lancer pytest**

```powershell
pytest test_dedup.py
```

Expected: PASS sur les imports, mais 0 test collecté.

## Task B.3 : TDD — normalize_text

- [ ] **Step 3.1: Test failing**

Ajouter dans `test_dedup.py` :

```python
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
```

- [ ] **Step 3.2: Run, vérifier rouge**

```powershell
pytest test_dedup.py::TestNormalizeText -v
```

Expected: 5 FAIL.

- [ ] **Step 3.3: Implémenter (depuis le brief Partie 1)**

```python
def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()
```

- [ ] **Step 3.4: Re-run vert**

```powershell
pytest test_dedup.py::TestNormalizeText -v
```

Expected: 5 PASS.

- [ ] **Step 3.5: Commit**

```powershell
git add dedup.py test_dedup.py
git commit -m "feat(dedup): normalize_text avec tests TDD"
```

## Task B.4 : TDD — signature_tokens + jaccard

- [ ] **Step 4.1: Tests failing**

Ajouter STOPWORDS dans `dedup.py` (depuis le brief) puis tests :

```python
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
        assert jaccard({"a","b"}, {"a","b"}) == 1.0
    def test_disjoint(self):
        assert jaccard({"a"}, {"b"}) == 0.0
    def test_partial(self):
        assert jaccard({"a","b"}, {"a","c"}) == pytest.approx(1/3)
    def test_both_empty(self):
        assert jaccard(set(), set()) == 1.0
    def test_one_empty(self):
        assert jaccard({"a"}, set()) == 0.0
```

- [ ] **Step 4.2: Run rouge**

- [ ] **Step 4.3: Implémenter STOPWORDS, signature_tokens, jaccard**

Copier exactement depuis `docs/plan-contenu-claude-code-v2.md` Partie 1.

- [ ] **Step 4.4: Re-run vert + commit**

```powershell
pytest test_dedup.py -v
git add dedup.py test_dedup.py
git commit -m "feat(dedup): signature_tokens + jaccard avec stopwords FR"
```

## Task B.5 : TDD — parse_french_date + is_future_date

- [ ] **Step 5.1: Tests failing**

```python
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
```

- [ ] **Step 5.2: Run rouge → implémenter (depuis brief Partie 2) → vert → commit**

```powershell
git add dedup.py test_dedup.py
git commit -m "feat(dedup): parse_french_date + is_future_date avec MOIS_FR"
```

## Task B.6 : Implémenter find_duplicate, merge_into_existing, upsert_event

Ces 3 fonctions font appel à Supabase (`sb`) → tests d'intégration plus lourds, dépendances.

- [ ] **Step 6.1: Implémenter les 3 fonctions depuis le brief Partie 1**

Pas de TDD strict ici (mock Supabase serait coûteux). Tests via dry-run en B.10.

- [ ] **Step 6.2: Vérifier que dedup.py se charge sans erreur**

```powershell
python -c "from dedup import upsert_event, find_duplicate, merge_into_existing; print('OK')"
```

Expected: `OK`.

- [ ] **Step 6.3: Commit**

```powershell
git add dedup.py
git commit -m "feat(dedup): find_duplicate + merge_into_existing + upsert_event"
```

## Task B.7 : Refactor scrape_articles.py — sources + filtre

- [ ] **Step 7.1: Backup et lecture**

```powershell
Copy-Item scrape_articles.py scrape_articles.py.bak
```

Lire `scrape_articles.py` pour identifier la liste `SOURCES`, la boucle `for entry in feed.entries` et la logique `if mentions_tuffes`.

- [ ] **Step 7.2: Remplacer SOURCES**

Coller depuis brief Partie 3-A. Retirer CSR Pontarlier, Saugeathlon, ancien SC Grandvaux. Ajouter Hebdo39, garder clubs locaux avec catégorie `club_local`.

- [ ] **Step 7.3: Élargir KEYWORDS_TUFFES**

Coller depuis brief Partie 3-E.

- [ ] **Step 7.4: Remplacer la logique de publication**

Trouver le bloc `if mentions_tuffes: status = "published" elif source["category"] == "club":...`. Remplacer par filtre strict (Partie 3-C) :

```python
if not mentions_tuffes:
    ignored += 1
    continue
status = "published"
```

- [ ] **Step 7.5: Ajouter le HTML fallback**

Pour les sources `club_local` sans entrées RSS, implémenter `scrape_html_news_page(source, html)` (brief Partie 3-B).

- [ ] **Step 7.6: Ajouter fetch HTML pour vérifier mentions**

Avant le `if not mentions_tuffes`, tenter un fetch de l'URL et regarder les 5000 premiers caractères (brief Partie 3-D).

- [ ] **Step 7.7: Améliorer logging**

Modifier la signature `scrape_source(source)` pour retourner `(added, ignored, errors)`. Logger en fin : `"%s : %d ajoutés, %d ignorés (sans mention stade), %d erreurs"`.

- [ ] **Step 7.8: Validation syntaxique**

```powershell
python -c "import ast; ast.parse(open('scrape_articles.py', encoding='utf-8').read()); print('OK')"
```

- [ ] **Step 7.9: Commit**

```powershell
git add scrape_articles.py
git commit -m "feat(articles): sources nettoyées + filtre strict + HTML fallback + Hebdo39"
```

## Task B.8 : Ajouter helpers et 5 scrapers events

- [ ] **Step 8.1: Imports et helpers en tête de scrape_events.py**

Ajouter en haut (après les imports actuels) :

```python
from dedup import upsert_event, parse_french_date, is_future_date, MOIS_FR
```

Et le helper `is_at_tuffes(text)` (brief Partie 4-A).

- [ ] **Step 8.2: scrape_cnsnmm()**

Coller depuis brief Partie 4-B. URL : `https://www.ensm.sports.gouv.fr/stade-nordique-des-tuffes-cnsnmm/`. Parsing des liens contenant des dates FR, status='published'.

- [ ] **Step 8.3: scrape_cyclo_haut_jura()**

Coller depuis brief Partie 4-C.

- [ ] **Step 8.4: scrape_transju()**

Coller depuis brief Partie 4-D.

- [ ] **Step 8.5: scrape_jura_tourism()**

Coller depuis brief Partie 4-E.

- [ ] **Step 8.6: scrape_club_events() + CLUB_EVENT_PAGES**

Coller depuis brief Partie 4-F. Status='pending' pour validation admin.

- [ ] **Step 8.7: Validation syntaxique**

```powershell
python -c "import ast; ast.parse(open('scrape_events.py', encoding='utf-8').read()); print('OK')"
python -c "from scrape_events import scrape_cnsnmm, scrape_cyclo_haut_jura, scrape_transju, scrape_jura_tourism, scrape_club_events, CLUB_EVENT_PAGES; print('OK')"
```

- [ ] **Step 8.8: Commit (un par scraper si possible)**

```powershell
git add scrape_events.py dedup.py
git commit -m "feat(events): 5 nouveaux scrapers (CNSNMM, Cyclo, Transju, Jura Tourisme, clubs)"
```

## Task B.9 : Brancher les scrapers dans main()

- [ ] **Step 9.1: Trouver le main() de scrape_events.py**

À la fin de `main()`, après le scraping FFS, ajouter le bloc du brief Partie 4-G.

- [ ] **Step 9.2: Validation**

```powershell
python -c "import scrape_events; print('OK')"
```

- [ ] **Step 9.3: Commit**

```powershell
git add scrape_events.py
git commit -m "feat(events): branchement nouveaux scrapers dans main()"
```

## Task B.10 : Test isolé local (pas de Supabase prod)

- [ ] **Step 10.1: Créer test_new_scrapers.py**

Coller depuis brief Partie 5-A. Adapter les valeurs SUPABASE_URL/KEY pour l'env actuel.

- [ ] **Step 10.2: HUMAN-IN-THE-LOOP — clé Supabase**

> "Pour le test isolé, j'ai besoin de la clé Supabase service_role (ou anon avec écriture). Tu me la donnes via une variable d'env ? Ne me la mets pas en clair dans un fichier."

- [ ] **Step 10.3: Lancer le test**

```powershell
$env:SUPABASE_URL="https://arkbrvzacbereyukqfte.supabase.co"
$env:SUPABASE_KEY="<clé fournie>"
python test_new_scrapers.py
```

Expected: tests unitaires passent (normalize, jaccard, parse_french_date), puis chaque scraper logge ses résultats.

- [ ] **Step 10.4: Évaluer les résultats**

Vérifier que :
- CNSNMM : au moins 1 event détecté (Championnats France 27-29 mars 2026 idéalement)
- Cyclo Haut Jura : 1 event ou 0 (si pas de date future sur la page)
- Jura Tourisme : événements stade détectés ou 0
- Clubs : ratio raisonnable de candidats vs faux positifs

- [ ] **Step 10.5: Commit**

```powershell
git add test_new_scrapers.py
git commit -m "test: script isolé de validation des nouveaux scrapers"
```

## Task B.11 : Run prod (HUMAN-IN-THE-LOOP)

- [ ] **Step 11.1: Demander confirmation**

> "Test isolé OK. Je vais lancer scrape_events.py et scrape_articles.py contre la BDD Supabase **prod**. Confirme ?"

- [ ] **Step 11.2: Lancer scrape_events.py (CONDITIONNEL)**

```powershell
python scrape_events.py
```

Expected: logs montrent FFS + 5 nouveaux scrapers, total events ajoutés/fusionnés.

- [ ] **Step 11.3: Lancer scrape_articles.py**

```powershell
python scrape_articles.py
```

Expected: logs montrent ratio ignored/added forte, Hebdo39 publie quelques articles.

- [ ] **Step 11.4: Synthèse à l'utilisateur**

Tableau récap : par source → nb ajoutés / fusionnés / ignorés / erreurs.

## Task B.12 : Push final

- [ ] **Step 12.1: Demander confirmation push**

> "Phase B terminée. Confirme push origin main ?"

- [ ] **Step 12.2: Push**

```powershell
git push origin main
```

---

## Critères d'acceptation Phase B

- [ ] `migration_dedup.sql` créée et confirmée exécutée par utilisateur
- [ ] `pytest test_dedup.py -v` → tous verts
- [ ] `dedup.py` créé, `scrape_articles.py` durci, `scrape_events.py` étendu (5 scrapers)
- [ ] `python test_new_scrapers.py` → résultats raisonnables sur les sources
- [ ] Run prod : ≥ 1 event ajouté/fusionné, ≥ 1 article publié depuis Hebdo39
- [ ] Logs montrent fort ratio "ignorés" (preuve filtre strict)

---

## Self-review (interne)

- ✅ Couvre toutes les Parties 0-5 de plan-contenu-claude-code-v2.md
- ✅ TDD pour fonctions pures (normalize, jaccard, parse_french_date)
- ✅ Hand-off explicite pour migration SQL et clé Supabase
- ✅ Confirmation utilisateur avant run prod
- ✅ Statut "pending" pour clubs respecté dans le brief
