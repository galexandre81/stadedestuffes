# stadedestuffes

Site d'information sur le Stade des Tuffes (Prémanon, Jura) :
[stadedestuffes.fr](https://stadedestuffes.fr). Agenda des épreuves de ski nordique et de
biathlon, revue de presse, infos pratiques pour venir.

## Statut (26.10.2026)

**En ligne et alimenté automatiquement.** Le code n'a pas changé depuis le 03.09.2026, mais
deux robots tournent chaque jour sur GitHub et remplissent la base :

- `scrape_events.py` (toutes les 8 h) collecte l'agenda (calendrier FFS, CNSNMM, La Transju',
  Jura Tourisme, Prémanon, clubs locaux…) ;
- `scrape_articles.py` (toutes les 6 h) collecte les articles de presse ;
- un troisième robot (`keepalive`, le 1er du mois) empêche GitHub de couper les deux premiers
  faute d'activité.

## Comment c'est fait

| Élément | Où |
|---|---|
| Site publié | `redesign/` : pages en JSX compilées par Babel, publiées sur GitHub Pages à chaque push sur `main` qui touche `redesign/` |
| Données | Supabase (événements, articles) ; règles d'accès dans les `migration_*.sql` |
| Robots de collecte | `scrape_events.py`, `scrape_articles.py`, `dedup.py` (Python 3.11) |
| Tests | `test_*.py` (pytest) ; `ffs_calendrier*.html` sont des pages de test |
| Automatisations | `.github/workflows/` |

Les pages HTML à la racine (`index.html`, `articles.html`…) et `partials/` sont l'ancienne
version du site : elles restent dans le dépôt mais ne sont plus publiées.

## Démarrage

```bash
git clone https://github.com/galexandre81/stadedestuffes.git
cd stadedestuffes
```

**Site** (aperçu local) — détail dans [`redesign/README.md`](redesign/README.md) :

```bash
cd redesign
npm install
npm run build              # compile les .jsx en .js
python -m http.server 8000 # http://localhost:8000
```

**Robots et tests** :

```bash
pip install -r requirements-dev.txt
python -m pytest
```

Les robots ont besoin de `SUPABASE_URL` et `SUPABASE_KEY` (secrets du dépôt GitHub en
production ; jamais écrits dans le code). Les tests n'appellent pas Supabase.

La clé Supabase présente dans `redesign/config.js` est la clé publique « anon », faite pour
être lue par le navigateur ; ce sont les règles d'accès de Supabase qui protègent les données.

## Changer de saison

Une fois par an, voir « Changer de saison » dans [`redesign/README.md`](redesign/README.md).

## Documentation

- [`redesign/README.md`](redesign/README.md) : le site, l'agenda, les sources, la mise en production.
- `docs/superpowers/` : plan d'amélioration de mai 2026.
