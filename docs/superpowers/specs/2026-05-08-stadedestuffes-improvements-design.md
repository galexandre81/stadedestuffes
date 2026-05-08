# Spec : Améliorations stadedestuffes.fr — Phases 1-3 + SEO/GEO

**Date** : 2026-05-08
**Auteur** : Guillaume Alexandre + Claude
**Statut** : À valider par l'utilisateur avant transition vers `writing-plans`

---

## 1. Contexte et état du dépôt

stadedestuffes.fr est un agrégateur **non officiel** des événements et actualités au Stade Nordique des Tuffes (Prémanon, Jura). Stack : GitHub Pages + Supabase + GitHub Actions, scrapers Python.

### 1.1 État Git au démarrage

- Branche locale `main` : **9 commits derrière `origin/main`** (commits sécurité, CTA Cinq Cibles SVG officiel, refactor scraper FFS, page mentions légales, etc.).
- `scrape_events.py` a **128 ajouts / 85 suppressions non committés** localement, qui implémentent partiellement les fixes du Sous-projet A (`extract_date_ffs_calendrier`, `MOIS_ABBREV`, `detect_sport` qui retourne `None` au lieu de `"Nordique"`).
- Fichiers non-trackés : fixtures FFS (`ffs_calendrier.html`, `ffs_calendrier_biathlon.html`), `parse_ffs_local.py`, `scraper_output.log`, `test_scraper.py`.
- Les 4 documents source (`BRIEF-CLAUDE-CODE-MASTER.md`, `plan-contenu-claude-code-v2.md`, `test_ffs_scraper.py`, `cinq-cibles-rotation.html`) ne sont **pas dans le repo** ; ils ont été produits sur Claude.ai.

### 1.2 Découvertes importantes vs brief original

- **Phase 3 partiellement obsolète** : commit `90e7ec5` du 2026-04-02 a déjà refait la pub Cinq Cibles avec logo SVG officiel, couleurs site, format compact (90px). Le travail restant est uniquement la **rotation entre 5 variations**, à adapter au format compact actuel.
- **Audit sécurité existant** (commit `55b3d1e`) a ajouté CSP, Referrer-Policy, échappement XSS, Supabase Auth pour admin. Toute insertion de contenu HTML/JS doit respecter cette CSP.
- **Modifs locales non committées** sur `scrape_events.py` correspondent exactement à ce que `test_ffs_scraper.py` du brief attend → ne pas les jeter.

---

## 2. Décomposition en 4 sous-projets

| # | Sous-projet | Source de vérité | Durée estimée | Ordre |
|---|---|---|---|---|
| **A** | Phase 1 — FFS validation | `test_ffs_scraper.py` | 30-45 min | 1 |
| **B** | Phase 2 — Dedup + scrapers + articles | `plan-contenu-claude-code-v2.md` | 1h30-2h | 2 |
| **D** | SEO/GEO ambitieux | Cette spec, section 5 | 2h-2h30 | 3 |
| **C** | Phase 3 — CTA Cinq Cibles rotation | `cinq-cibles-rotation.html` | 30 min | 4 |

**Justification ordre A → B → D → C** :
- A en premier : valide la fondation FFS avant d'empiler dessus.
- B avant D : la spec SEO ajoute du contenu HTML aux 3 pages publiques ; faire B d'abord évite les conflits de merge si B touchait aussi ces pages (en pratique B ne touche pas le HTML, mais l'ordre logique reste cohérent).
- D avant C : SEO impacte les 3 pages HTML publiques (`<head>`, intros, FAQ). Faire D avant C évite de remplacer le bloc CTA puis de le retoucher pour SEO.

Chaque sous-projet pourra être exécuté **séparément**, avec un commit propre entre chaque. L'utilisateur peut s'arrêter entre 2 sous-projets sans bloquer les suivants.

---

## 3. Setup Git préalable (sous-projet 0)

À exécuter **avant** tout sous-projet de fond :

1. `git stash push scrape_events.py -m "Phase 1 FFS local fixes"` — préserve les 128 ajouts non committés.
2. Déplacer hors du repo (ou ajouter au `.gitignore` ad hoc) : `parse_ffs_local.py`, `scraper_output.log`, fixtures HTML — décision utilisateur. Par défaut : **garder dans le repo** parce que les fixtures servent aux tests et doivent être versionnées.
3. `git pull origin main` — fast-forward (les 9 commits ne touchent pas `scrape_events.py`, donc pas de conflit attendu sur le pull lui-même).
4. `git stash pop` — réapplique les modifs locales sur la nouvelle base.
5. **Si conflit sur `scrape_events.py`** : prendre la version locale (qui contient déjà les fixes Phase 1), puis examiner manuellement ce qui aurait pu être modifié sur origin (peu probable vu les commits récents).
6. Committer les 4 documents source du brief dans le repo :
   - `docs/BRIEF-CLAUDE-CODE-MASTER.md`
   - `docs/plan-contenu-claude-code-v2.md`
   - `test_ffs_scraper.py` à la racine
   - `docs/cinq-cibles-rotation.html` (référence visuelle, pas de la prod)
7. Committer cette spec : `docs/superpowers/specs/2026-05-08-stadedestuffes-improvements-design.md`.

---

## 4. Sous-projets A, B, C — Référencement aux specs existantes

Pour ces 3 sous-projets, la spec détaillée existe déjà dans les documents committés à l'étape 6 ci-dessus. Cette spec ne les redocumente pas pour éviter la dérive.

### Sous-projet A — Phase 1 FFS validation
- **Spec** : `test_ffs_scraper.py` (suite de tests pytest avec 4 classes : `TestFixtureStructure`, `TestIsLieuTuffes`, `TestDetectSport`, `TestExtractDateFFSCalendrier`, `TestExtractDateFromText`, `TestIntegrationFixture`, `TestLiveFFS`).
- **Critères d'acceptation** :
  - `pytest test_ffs_scraper.py -v` → tous verts
  - Mode dry-run liste les 4 events Tuffes attendus
  - Si LIVE_TEST=1 lancé : snapshot sauvée dans `tests/snapshots/`
  - Aucune modif non documentée à `scrape_events.py` (les modifs locales restaurées par stash pop sont OK)

### Sous-projet B — Phase 2 contenu
- **Spec** : `docs/plan-contenu-claude-code-v2.md` (sections 0 à 5 du document).
- **Migration SQL préalable** : `migration_dedup.sql` créé mais **PAS exécuté par Claude** — c'est l'utilisateur qui l'exécute manuellement dans le SQL editor Supabase. Hand-off explicite.
- **TDD strict** : pour chaque fonction de `dedup.py` (normalize_text, signature_tokens, jaccard, parse_french_date, etc.), test unitaire avant implémentation.
- **Critères d'acceptation** :
  - `migration_dedup.sql` créé et exécuté (confirmé par utilisateur)
  - `dedup.py` créé, `test_dedup.py` tous verts
  - `scrape_articles.py` modifié, dry-run montre ratio ignorés/total
  - 5 nouveaux scrapers events branchés dans `main()`
  - Run réel : ≥ 1 event ajouté/fusionné, ≥ 1 article publié depuis Hebdo39

### Sous-projet C — Phase 3 CTA rotation
- **Spec** : `docs/cinq-cibles-rotation.html` (les 5 variations + JS de rotation aléatoire).
- **Adaptation requise** : le bloc CTA actuel sur origin (commit 90e7ec5) est en format compact (90px hauteur). Il faut adapter le code de la maquette à ce format **sans repasser à 320px** comme dans la maquette.
- **Décision** : extraire un partial `partials/cinq-cibles-cta.html` chargé via `fetch()`, comme prévu dans le brief, MAIS en respectant le format compact existant (le compact wins).
- **Critères d'acceptation** :
  - `partials/cinq-cibles-cta.html` créé
  - `assets/css/cinq-cibles-cta.css` créé (ou intégré au CSS existant si plus simple — décision pendant implémentation)
  - 3 pages modifiées (index, articles, venir-aux-tuffes)
  - F5 montre une variation différente sur 5 reloads
  - Pas d'erreur console, CSP respectée

---

## 5. Sous-projet D — SEO/GEO ambitieux (spec détaillée)

### 5.1 Périmètre

**Objectif n°1** : trafic organique recherche Google sur les requêtes longue traîne géographiques (validé utilisateur).
**Objectif n°2** (bonus) : présence dans les réponses des assistants IA (GEO).

**Pas de Google Maps, pas de Google Business Profile, pas de schema `LocalBusiness` ou `SportsActivityLocation`** — cohérence avec le caractère non officiel du site (validé utilisateur).

### 5.2 Architecture : approche A (hybride statique + dynamique)

- Meta tags, OG, canonical, robots.txt, FAQ, intros : **statiques** dans les fichiers HTML.
- Sitemap.xml : **statique**, pages uniquement (5 URLs). Pas de regénération automatique.
- Schema.org `Event` JSON-LD : **injecté côté client** après fetch Supabase, dans la page index.html, un `<script type="application/ld+json">` par event.
- Schema.org `FAQPage` : **statique** sur venir-aux-tuffes.html.
- Schema.org `WebSite` : **statique** dans toutes les pages (head).
- Schema.org `BreadcrumbList` : **statique** sur articles.html et venir-aux-tuffes.html.

### 5.3 Fichiers à créer

| Fichier | Contenu |
|---|---|
| `robots.txt` | `User-agent: *` / `Allow: /` / `Disallow: /admin.html` / `Sitemap: https://stadedestuffes.fr/sitemap.xml` |
| `sitemap.xml` | 5 URLs : `/`, `/articles.html`, `/venir-aux-tuffes.html`, `/mentions-legales.html`. Pas `/admin.html`. Lastmod = date du commit. Changefreq=weekly pour `/`, monthly pour le reste, priority cohérent. |
| `og-default.jpg` | Image OG 1200×630, à fournir par l'utilisateur ou à générer (décision en plan) |

### 5.4 Modifications par page

#### `index.html`
- `<head>` : OG title/description/image/url/type=website/locale=fr_FR + Twitter Card summary_large_image + canonical absolu + meta description retravaillée (~150 chars, inclut "Prémanon" et "Jura")
- `<head>` : `<script type="application/ld+json">` statique avec schéma `WebSite`
- Au début du `<body>` après le hero : courte intro de ~80 mots situant le stade, mentionnant les 4 disciplines, précisant le caractère agrégateur. À insérer dans la structure existante sans casser le design.
- Vérification `<h1>` unique
- JS : après le `fetch` Supabase qui peuple les events, **injecter dynamiquement** un `<script type="application/ld+json">` par event avec schéma `Event` :
  ```json
  {
    "@context": "https://schema.org",
    "@type": "SportsEvent",
    "name": "{event.title}",
    "startDate": "{event.date_start}",
    "endDate": "{event.date_end || event.date_start}",
    "eventStatus": "https://schema.org/EventScheduled",
    "eventAttendanceMode": "https://schema.org/OfflineEventAttendanceMode",
    "location": {
      "@type": "Place",
      "name": "Stade Nordique des Tuffes",
      "address": {
        "@type": "PostalAddress",
        "addressLocality": "Prémanon",
        "postalCode": "39220",
        "addressRegion": "Jura",
        "addressCountry": "FR"
      }
    },
    "sport": "{event.sport}",
    "url": "https://stadedestuffes.fr/#event-{event.id}"
  }
  ```
  Note : utilise `escHtml()`/`safeUrl()` existants pour respecter l'audit sécurité.

#### `articles.html`
- `<head>` : OG, Twitter Card, canonical, meta description différenciée
- `<head>` : Schema.org `WebSite` + `BreadcrumbList` (Accueil → Actualités)
- Intro de ~50 mots avant la liste

#### `venir-aux-tuffes.html` (page haute valeur GEO)
- `<head>` : OG, Twitter Card, canonical, meta description ciblée "comment aller, accès, parking"
- `<head>` : Schema.org `WebSite` + `BreadcrumbList` (Accueil → Venir aux Tuffes) + `FAQPage`
- Section FAQ avec 7 questions :
  1. Comment se rendre au Stade Nordique des Tuffes en voiture / transport en commun ?
  2. Où se garer pour assister à une compétition ?
  3. Quelles disciplines peut-on voir au Stade des Tuffes ?
  4. Le stade est-il ouvert au public en dehors des compétitions ?
  5. Y a-t-il une restauration sur place pendant les événements ?
  6. Quelle est la meilleure période pour visiter le stade nordique ?
  7. Ce site est-il officiel ?
- Chaque réponse = paragraphe HTML structuré + entrée correspondante dans le JSON-LD `FAQPage`
- Si la page contient déjà des sections proches, les fusionner — pas de duplication.

#### `mentions-legales.html`
- `<head>` : `<meta name="robots" content="noindex, follow">` + canonical absolu + meta description minimale
- Pas de OG/Twitter (page non partageable)

#### `admin.html`
- `<head>` : `<meta name="robots" content="noindex, nofollow">`
- Pas de modifications SEO ailleurs

### 5.5 Vérification CSP

L'audit sécurité a ajouté une CSP. Avant d'injecter du JSON-LD dynamique, vérifier la directive `script-src`. Si elle exclut inline JS, ajouter une exception pour `application/ld+json` (qui n'est pas exécuté, c'est juste des données) — généralement `script-src` ne s'applique pas aux scripts non exécutables, mais à confirmer dans l'implémentation.

### 5.6 Critères d'acceptation sous-projet D

- `robots.txt` et `sitemap.xml` accessibles à la racine déployée
- Validation manuelle via Google Rich Results Test (https://search.google.com/test/rich-results) sur :
  - `/` → détecte `WebSite` + au moins 1 `SportsEvent`
  - `/venir-aux-tuffes.html` → détecte `FAQPage` avec les 7 questions
- Validation `view-source:` sur chaque page : OG, Twitter, canonical présents
- Aucune erreur console liée à la CSP après injection JSON-LD
- Aucune régression visuelle (FAQ stylée correctement, intros bien intégrées)

### 5.7 Out-of-scope déclaré

- Pas de Google Business Profile, pas de Maps embed, pas de schema `LocalBusiness`
- Pas d'optimisation Core Web Vitals (perf)
- Pas de réécriture de la copy existante au-delà des intros mentionnées
- Pas de nouvelles pages SEO (ex : "Histoire du stade", "Athlètes")
- Pas de hreflang (mono-langue FR)
- Pas de Google Search Console setup (à faire par l'utilisateur si pas déjà fait)

---

## 6. Risques globaux et mitigation

| # | Risque | Probabilité | Impact | Mitigation |
|---|---|---|---|---|
| R1 | CSP bloque inline JSON-LD | Moyen | Bas | Vérifier dans l'audit, élargir si nécessaire |
| R2 | Conflit stash sur scrape_events.py | Moyen | Bas | Prendre version locale, examiner si origin a touché le fichier (peu probable) |
| R3 | Dedup Jaccard fusionne events distincts | Moyen | Moyen | Statut `pending` par défaut, admin valide ; seuil ajustable |
| R4 | Bing/DDG indexe mal JSON-LD client-side | Faible-Moyen | Bas | Acceptée, observation 4-6 sem post-deploy, fallback Approche B si > 50 % perte |
| R5 | Nouveaux scrapers events tombent en erreur (sites changent) | Moyen | Bas | Logs explicites, échec silencieux d'une source ne bloque pas les autres |
| R6 | Migration SQL exécutée 2 fois | Moyen | Bas | `IF NOT EXISTS` partout dans la migration |

---

## 7. Hand-off : ce qui revient à l'utilisateur (humain)

À ne **pas** automatiser, à effectuer manuellement :

1. **Migration SQL** (`migration_dedup.sql`) avant Sous-projet B run réel
2. **Confirmation explicite** avant chaque run réel des scrapers contre Supabase prod (B et follow-ups)
3. **Fournir `og-default.jpg`** ou décider de générer une image neutre depuis le code (décision en plan)
4. **Google Search Console** : déclaration du sitemap si pas déjà fait, surveillance ultérieure

---

## 8. Définition de "fait" globale

Le projet entier (les 4 sous-projets) est considéré "fait" quand :

- ✅ Tous les commits de chaque sous-projet sont sur `origin/main`
- ✅ Le site déployé sur GitHub Pages est en ligne et fonctionnel
- ✅ `pytest` global vert (FFS + dedup + scrapers)
- ✅ Validation manuelle Rich Results Test OK
- ✅ Run réel scrape_events.py ajoute des events (au moins 1)
- ✅ Run réel scrape_articles.py publie des articles (au moins 1, depuis Hebdo39)
- ✅ Rotation CTA visible au F5
- ✅ Aucune erreur console sur les 3 pages publiques
- ✅ Capture d'écran avant/après partagée

Phase 4 (refonte design) reste **explicitement hors scope** de cette spec, à traiter en session Claude.ai séparée comme prévu dans le brief original.
