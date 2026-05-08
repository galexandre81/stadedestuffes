# Phase 0 — Setup Git & dépôt Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Aligner le dépôt local avec origin/main, préserver les modifs locales utiles, committer les documents source du brief, installer les dépendances Python.

**Architecture:** Stash → pull → unstash → commits docs.

**Tech Stack:** git, pip, Windows PowerShell.

---

## File Structure

- **À créer dans le repo** :
  - `docs/BRIEF-CLAUDE-CODE-MASTER.md` (depuis chat)
  - `docs/plan-contenu-claude-code-v2.md` (depuis chat)
  - `docs/cinq-cibles-rotation.html` (depuis chat — référence visuelle)
  - `test_ffs_scraper.py` à la racine (depuis chat)
  - `requirements-dev.txt` à la racine (nouveau)
  - `.gitignore` mis à jour pour exclure les artefacts de dev

- **Déjà présent à conserver** :
  - `scrape_events.py` modifs locales (stashé puis restauré)
  - Fixtures `ffs_calendrier.html` et `ffs_calendrier_biathlon.html` (utiles pour Phase A)

---

## Task 0.1 : Vérifier état initial

- [ ] **Step 1.1: Lister l'état avant toute modification**

```powershell
cd "C:\Users\guill\Desktop\stadedestuffes"
git status
git log origin/main --oneline -10 ^HEAD
```

Expected: 9 commits behind origin/main, scrape_events.py modifié, plusieurs fichiers untracked (fixtures FFS, parse_ffs_local.py, scraper_output.log, test_scraper.py, .claude/).

## Task 0.2 : Stash modifs locales scrape_events.py

- [ ] **Step 2.1: Stasher uniquement scrape_events.py**

```powershell
git stash push scrape_events.py -m "Phase A FFS local fixes (extract_date_ffs_calendrier + MOIS_ABBREV)"
```

Expected: stash@{0} créé, scrape_events.py revient à HEAD.

- [ ] **Step 2.2: Vérifier le stash**

```powershell
git stash list
git status
```

Expected: 1 stash listé, scrape_events.py n'apparaît plus dans modified.

## Task 0.3 : Mettre les artefacts dev hors du tracking

- [ ] **Step 3.1: Mettre à jour .gitignore**

Ajouter ces lignes à `.gitignore` (créer si absent) :

```
# Logs et outputs locaux
scraper_output.log
*.log

# Artefacts dev locaux (pas en prod)
parse_ffs_local.py
test_scraper.py

# Claude Code
.claude/

# Snapshots de tests
tests/snapshots/

# Python
__pycache__/
*.pyc
.pytest_cache/
```

- [ ] **Step 3.2: Vérifier que les artefacts ne sont plus untracked**

```powershell
git status
```

Expected: parse_ffs_local.py, scraper_output.log, .claude/, test_scraper.py n'apparaissent plus dans Untracked. Les fixtures `ffs_calendrier*.html` apparaissent toujours (à committer en Phase A).

- [ ] **Step 3.3: Commit du .gitignore**

```powershell
git add .gitignore
git commit -m "chore: gitignore logs, dev artifacts, Claude Code, pytest cache"
```

## Task 0.4 : Pull origin/main

- [ ] **Step 4.1: Pull fast-forward**

```powershell
git pull origin main
```

Expected: fast-forward de 9 commits. Si pas fast-forward (parce que le commit du .gitignore vient d'être créé), faire un rebase :

```powershell
git pull --rebase origin main
```

Expected après rebase : working tree clean, 1 commit local en avance (le gitignore), 0 derrière.

- [ ] **Step 4.2: Vérifier l'historique**

```powershell
git log --oneline -15
```

Expected: voir les 9 commits récents (sécurité, Cinq Cibles SVG, etc.) + le commit gitignore en tête.

## Task 0.5 : Restaurer le stash et résoudre conflits éventuels

- [ ] **Step 5.1: Pop le stash**

```powershell
git stash pop
```

Expected: scrape_events.py modifié à nouveau. Si conflit (peu probable car les 9 commits récents ne touchent pas scrape_events.py), voir Step 5.2.

- [ ] **Step 5.2: En cas de conflit (CONDITIONNEL)**

Si conflit signalé :

```powershell
git status
```

Pour chaque section conflictuelle dans `scrape_events.py` :
- Garder la version locale (modifs Phase A : `extract_date_ffs_calendrier`, `MOIS_ABBREV`, `detect_sport` retournant `None`)
- Si origin a aussi modifié quelque chose, comparer manuellement
- Marquer résolu avec `git add scrape_events.py`

- [ ] **Step 5.3: Vérifier que le code Python est valide**

```powershell
python -c "import ast; ast.parse(open('scrape_events.py', encoding='utf-8').read()); print('OK')"
```

Expected: `OK`.

## Task 0.6 : Committer les modifs scrape_events.py restaurées

- [ ] **Step 6.1: Voir les modifs à committer**

```powershell
git diff scrape_events.py | Select-Object -First 50
```

Expected: voir `extract_date_ffs_calendrier`, `MOIS_ABBREV`, `detect_sport` retournant `None`, IDs de disciplines FFS corrigés.

- [ ] **Step 6.2: Commit**

```powershell
git add scrape_events.py
git commit -m "fix: parser dates calendrier FFS + IDs disciplines + detect_sport renvoie None"
```

## Task 0.7 : Committer les fixtures FFS

- [ ] **Step 7.1: Vérifier que les fixtures existent et sont raisonnables**

```powershell
Get-ChildItem ffs_calendrier*.html | Select-Object Name, Length
```

Expected: 2 fichiers, taille 50KB-2MB chacun.

- [ ] **Step 7.2: Commit des fixtures**

```powershell
git add ffs_calendrier.html ffs_calendrier_biathlon.html
git commit -m "test: ajout fixtures HTML calendrier FFS pour tests scraper"
```

## Task 0.8 : Créer requirements-dev.txt

- [ ] **Step 8.1: Créer le fichier**

Contenu de `requirements-dev.txt` :

```
pytest>=7.0
beautifulsoup4>=4.10
requests>=2.28
feedparser>=6.0
supabase>=2.0
python-dateutil>=2.8
```

- [ ] **Step 8.2: Installer les dépendances**

```powershell
pip install -r requirements-dev.txt
```

Expected: installation OK, pytest disponible.

- [ ] **Step 8.3: Vérifier pytest**

```powershell
pytest --version
```

Expected: `pytest 7.x.x` ou supérieur.

- [ ] **Step 8.4: Commit**

```powershell
git add requirements-dev.txt
git commit -m "chore: requirements-dev.txt pour pytest et libs scrapers"
```

## Task 0.9 : Committer les documents source du brief

- [ ] **Step 9.1: Créer docs/ si absent et copier les 4 documents**

Les 4 documents (`BRIEF-CLAUDE-CODE-MASTER.md`, `plan-contenu-claude-code-v2.md`, `cinq-cibles-rotation.html`) ont été partagés dans la conversation initiale. L'engineer doit demander à l'utilisateur de les fournir comme fichiers ou bien copier-coller leur contenu depuis l'historique.

```powershell
mkdir -Force docs
# Le contenu est à coller depuis le chat. L'engineer crée les 3 fichiers :
# - docs\BRIEF-CLAUDE-CODE-MASTER.md
# - docs\plan-contenu-claude-code-v2.md
# - docs\cinq-cibles-rotation.html
# Le 4e (test_ffs_scraper.py) va à la racine, voir Phase A.
```

- [ ] **Step 9.2: Commit des 3 docs de référence**

```powershell
git add docs/BRIEF-CLAUDE-CODE-MASTER.md docs/plan-contenu-claude-code-v2.md docs/cinq-cibles-rotation.html
git commit -m "docs: brief master + plan contenu v2 + maquette CTA Cinq Cibles"
```

## Task 0.10 : Committer la spec et les plans déjà rédigés

- [ ] **Step 10.1: Vérifier la spec et les plans**

```powershell
Get-ChildItem docs\superpowers -Recurse
```

Expected: voir `specs/2026-05-08-stadedestuffes-improvements-design.md` et 5 fichiers dans `plans/`.

- [ ] **Step 10.2: Commit**

```powershell
git add docs/superpowers
git commit -m "docs: spec et plans phases 0/A/B/C/D + master overview"
```

## Task 0.11 : Push (DEMANDER À L'UTILISATEUR D'ABORD)

- [ ] **Step 11.1: Demander confirmation utilisateur avant push**

> "Phase 0 terminée. 5 commits prêts à être poussés sur origin/main : .gitignore, scrape_events.py fixes, fixtures FFS, requirements-dev.txt, docs (4 fichiers de référence + spec + 5 plans). OK pour push ?"

Attendre la réponse explicite. Ne pas push sans confirmation.

- [ ] **Step 11.2: Push (CONDITIONNEL — uniquement si utilisateur a confirmé)**

```powershell
git push origin main
```

Expected: push OK, GitHub Pages se redéploie (les changements ne sont que docs + .gitignore + Python — pas d'impact visible côté site).

---

## Critères d'acceptation Phase 0

- [ ] `git status` → working tree clean
- [ ] `git log` → la spec, les 5 plans, les 4 docs source, les fixtures, et les fixes scrape_events.py sont committés
- [ ] `pytest --version` → fonctionne
- [ ] `python -c "from scrape_events import extract_date_ffs_calendrier, MOIS_ABBREV; print('OK')"` → OK
- [ ] Push sur origin/main effectué (avec confirmation utilisateur)

---

## Self-review (interne)

- ✅ Spec coverage : couvre la section 3 de la spec (Setup Git préalable)
- ✅ Pas de placeholder
- ✅ Commandes exactes
- ✅ Checkpoint utilisateur explicite avant push
