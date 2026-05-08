# Phase A — Validation scraper FFS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prouver via tests automatisés que le scraper FFS fonctionne sur la fixture, identifier toute régression, mettre en place une alerte précoce.

**Architecture:** Suite pytest sur fixture HTML locale + tests live optionnels. Si tests rouges → debugging systématique avant fix. Si verts → on s'arrête là.

**Tech Stack:** Python 3.13, pytest, beautifulsoup4, requests.

**Pre-requis:** Phase 0 terminée (pytest installé, scrape_events.py modifs locales committées).

---

## File Structure

- **À créer** :
  - `test_ffs_scraper.py` à la racine (contenu fourni dans le chat — c'est le 4e document du brief)
  - `tests/snapshots/` (créé automatiquement par les tests live si lancés)

- **À modifier (UNIQUEMENT si tests échouent et que le code FFS a une régression)** :
  - `scrape_events.py`

---

## Task A.1 : Importer test_ffs_scraper.py

- [ ] **Step 1.1: Créer le fichier**

Le contenu de `test_ffs_scraper.py` est dans le chat (4e document du brief). Le créer à la racine du repo. Vérifier les imports en tête :

```python
from scrape_events import (
    is_lieu_tuffes,
    is_competition,
    is_resultat,
    detect_sport,
    extract_date_ffs_calendrier,
    extract_date_from_text,
    KEYWORDS_LIEU,
    MOIS_ABBREV,
    MOIS_FR,
)
```

- [ ] **Step 1.2: Vérifier l'import statique**

```powershell
python -c "import test_ffs_scraper; print('Import OK')"
```

Expected: `Import OK`. Si `ImportError`, c'est qu'une fonction/constante manque dans `scrape_events.py` — le noter pour l'étape A.4.

## Task A.2 : Tour de chauffe pytest

- [ ] **Step 2.1: Lancer pytest --collect-only**

```powershell
pytest test_ffs_scraper.py --collect-only -q
```

Expected: liste les ~30+ tests sans les exécuter. Si erreur de collection, fixer avant de continuer.

## Task A.3 : Première run

- [ ] **Step 3.1: Lancer la suite complète**

```powershell
pytest test_ffs_scraper.py -v 2>&1 | Tee-Object -FilePath pytest_run1.log
```

Note les résultats : combien passent, combien échouent.

- [ ] **Step 3.2: Décision**

Si **TOUS verts** → passer directement à Task A.5 (mode dry-run). Ne pas modifier `scrape_events.py`.
Si **rouges** → passer à Task A.4 (debugging) avant de continuer.

## Task A.4 : Debugging des tests rouges (CONDITIONNEL)

- [ ] **Step 4.1: Invoquer skill systematic-debugging**

```
/skills systematic-debugging
```

Phase 1 reproduce : isoler le test qui échoue (`pytest test_ffs_scraper.py::TestX::test_y -v`).
Phase 2 root cause : pour chaque échec, déterminer si c'est :
- Un bug dans le code FFS de `scrape_events.py` (à corriger)
- Un test mal écrit (à corriger côté test)
- Un changement légitime (ex : la fixture n'a plus 4 events Tuffes)

- [ ] **Step 4.2: Pour chaque test rouge, fixer la cause racine**

Pour bug code → modif minimale dans `scrape_events.py`, conserver l'API publique.
Pour bug test → ajuster l'attendu en se basant sur la vérité terrain.
Pour fixture obsolète → mettre à jour les `EXPECTED_TITLES_TUFFES` ou autres constantes du test.

- [ ] **Step 4.3: Re-run jusqu'à ce que tout soit vert**

```powershell
pytest test_ffs_scraper.py -v
```

Expected: tous les tests verts.

- [ ] **Step 4.4: Commit des fixes**

```powershell
git add scrape_events.py test_ffs_scraper.py
git commit -m "fix: corrections scraper FFS / suite pytest pour valider la fixture"
```

## Task A.5 : Mode dry-run sur fixture

- [ ] **Step 5.1: Lancer le mode main**

```powershell
python test_ffs_scraper.py
```

Expected: liste les 4 events Tuffes :
- SAMSE BIATHLON NATIONAL TOUR 3 (LES TUFFES)
- BIATHLON REGIONAL U15 (STADE DES TUFFES)
- SAMSE BIATHLON NATIONAL TOUR 7 (LES TUFFES)
- CHAMPIONNATS DE FRANCE (LES TUFFES)

Avec date et sport pour chacun.

- [ ] **Step 5.2: Si moins de 4 events ou autres titres**

Investiguer : la fixture a peut-être été regénérée à une date différente. Mettre à jour `EXPECTED_TITLES_TUFFES` dans le test si la fixture est légitimement différente.

## Task A.6 : Test live (DEMANDER UTILISATEUR)

- [ ] **Step 6.1: Demander confirmation pour le live test**

> "Phase A passe sur la fixture. Veux-tu que je lance le test live qui fait des requêtes HTTP vers ffs.fr et sauvegarde une snapshot dans tests/snapshots/ ? (LIVE_TEST=1 pytest test_ffs_scraper.py -k live)"

Attendre la réponse.

- [ ] **Step 6.2: Lancer le test live (CONDITIONNEL)**

Si utilisateur confirme :

```powershell
$env:LIVE_TEST="1"
pytest test_ffs_scraper.py -k live -v
$env:LIVE_TEST=$null
```

Expected: 1 test passant, 1 fichier `tests/snapshots/ffs_live_YYYYMMDD.html` créé.

- [ ] **Step 6.3: Décision sur la snapshot live**

Si test live passe : continuer.
Si test live échoue : c'est un signal que ffs.fr a changé sa structure. Documenter dans `pytest_run1.log` et alerter l'utilisateur. Ne pas tenter de fix maintenant — c'est un autre chantier.

## Task A.7 : Mini-rapport et commit final

- [ ] **Step 7.1: Synthèse à fournir à l'utilisateur**

Un message de 5 lignes max :
- Statut global (OK / régression / FFS a changé)
- Modifs faites (si applicable)
- Recommandations pour Phase B

- [ ] **Step 7.2: Si la suite a déjà été committée en A.4, sauter. Sinon :**

```powershell
git add test_ffs_scraper.py
git commit -m "test: suite pytest validation scraper FFS sur fixture"
```

- [ ] **Step 7.3: Push (avec confirmation)**

> "Phase A terminée, X commits prêts. OK pour push origin main ?"

Si confirmation :
```powershell
git push origin main
```

---

## Critères d'acceptation Phase A

- [ ] `pytest test_ffs_scraper.py -v` → tous verts
- [ ] `python test_ffs_scraper.py` → liste les 4 events Tuffes attendus
- [ ] (Optionnel) `tests/snapshots/ffs_live_YYYYMMDD.html` créé si live test lancé
- [ ] `test_ffs_scraper.py` committé sur main
- [ ] Mini-rapport remis à l'utilisateur

---

## Self-review (interne)

- ✅ Couvre les Étapes 1-5 du Phase 1 du BRIEF
- ✅ TDD respecté (tests d'abord)
- ✅ Skill systematic-debugging invoqué si tests rouges
- ✅ Checkpoint utilisateur avant live test
- ✅ Pas de modification spéculative du code FFS si tests verts
