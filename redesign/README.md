# stadedestuffes.fr — Refonte 2026

Version pré-compilée du site, prête pour la production. Source en JSX, build Babel automatique via GitHub Actions, déploiement GitHub Pages.

## Pages

| URL | Source JSX | Compilé |
|---|---|---|
| `/` | `app.jsx` | `app.js` |
| `/articles.html` | `articles.jsx` | `articles.js` |
| `/venir-aux-tuffes.html` | `venir.jsx` | `venir.js` |
| `/a-propos.html` | `apropos.jsx` | `apropos.js` |
| `/mentions-legales.html` | `mentions.jsx` | `mentions.js` |

Partagés : `shell.jsx` (Disclaimer/Nav/Footer), `tweaks-panel.jsx` (no-op), `data.js` (Supabase à brancher), `styles.css`, `photos/`, `favicon.svg`.

## Workflow d'édition

**Tu modifies uniquement les `.jsx`.** Les `.js` sont générés automatiquement à chaque push.

```
1. Édite app.jsx (ou n'importe quel .jsx)
2. git commit && git push
3. GitHub Action recompile + déploie sur stadedestuffes.fr
```

Le workflow est dans `.github/workflows/deploy-redesign.yml`. Build typique : 30-45 s.

## Aperçu local (avant push)

Une seule fois pour installer :
```bash
cd redesign
npm install
```

Puis à chaque preview :
```bash
npm run build           # compile .jsx → .js
python -m http.server 8000
```
→ http://localhost:8000

## Bascule en production (à faire une fois)

### 1. Configurer GitHub Pages pour utiliser Actions

Dans le repo GitHub → **Settings → Pages → Source** : choisir **« GitHub Actions »** (au lieu de « Deploy from a branch »).

### 2. Sauvegarder le logo Cinq Cibles

Place le JPEG dans `redesign/photos/cinqcibles-logo.jpeg`. Référencé par `app.jsx` dans la section sponsor.

### 3. Brancher Supabase (sinon le live affichera les données de démo)

Édite `data.js` et remplace les arrays statiques par :

```js
const SUPABASE_URL = "https://XXXX.supabase.co";   // récupère depuis l'ancien index.html
const SUPABASE_ANON_KEY = "eyJ...";

window.STT_EVENTS = [];
window.STT_ARTICLES = [];

(async () => {
  const headers = { apikey: SUPABASE_ANON_KEY, Authorization: `Bearer ${SUPABASE_ANON_KEY}` };
  const [evRes, artRes] = await Promise.all([
    fetch(`${SUPABASE_URL}/rest/v1/events?status=eq.published&order=date_start.asc`, { headers }),
    fetch(`${SUPABASE_URL}/rest/v1/press_articles?status=eq.published&order=published_at.desc&limit=24`, { headers }),
  ]);
  window.STT_EVENTS = await evRes.json();
  window.STT_ARTICLES = await artRes.json();
  window.dispatchEvent(new CustomEvent('stt-data-ready'));
})();
```

Puis dans `app.jsx` et `articles.jsx`, ajoute en haut du composant principal :

```js
const [, force] = React.useReducer(x => x + 1, 0);
React.useEffect(() => {
  const h = () => force();
  window.addEventListener('stt-data-ready', h);
  return () => window.removeEventListener('stt-data-ready', h);
}, []);
```

### 4. Push

```
git add redesign/ .github/
git commit -m "Refonte production"
git push
```

Le workflow se lance, compile, déploie. Vérifier dans l'onglet **Actions** sur GitHub.

### 5. Archive l'ancien site (optionnel)

Les anciens fichiers à la racine (`index.html`, `articles.html`…) ne servent plus dès que GitHub Pages bascule sur Actions, mais tu peux les déplacer dans `legacy/` pour faire le ménage.

## Ce qui est en place côté SEO / GEO / perf

- **JSX pré-compilé** : pas de Babel à l'exécution → first paint rapide
- **React production min** (~140 KB au lieu de ~4 MB Babel + React dev)
- **Meta SEO complets** sur chaque page : `description`, canonical, favicon
- **Open Graph + Twitter Cards** : preview riche sur WhatsApp / LinkedIn / Twitter / Facebook
- **JSON-LD structuré** :
  - `WebSite` + `SportsActivityLocation` du stade sur la home
  - `FAQPage` sur la page Venir (5 Q/R indexables par les IA et Google)
  - `AboutPage` sur À propos
  - `CollectionPage` sur Articles
- **`sitemap.xml`** mis à jour (5 pages, dates 2026-05-09)
- **`robots.txt`** : indexation OK partout sauf admin
- **`<meta name="robots" content="index, nofollow">`** sur mentions légales (indexable mais ne diffuse pas le PageRank)
- **`hreflang`** : non requis (mono-langue FR)
- **Lang `fr`** sur `<html>`

## Points de vigilance

- **Le contenu dynamique (events, articles) reste rendu côté client** après hydration React. C'est OK pour Google (qui exécute le JS) mais pas idéal pour les autres bots. Si tu veux un SEO parfait sur le calendrier, il faudra plus tard générer un dump statique au build (ex: `build-events.js` qui fetch Supabase et écrit du HTML pré-rendu). Hors scope pour ce premier release.
- **Logo Cinq Cibles** : dépend du JPEG sauvé manuellement. Tant qu'il n'est pas en place : image cassée dans la section sponsor.
- **`og:image` absolue** : pointe sur `https://stadedestuffes.fr/photos/hero-range-sun.jpg`. Vérifie que cette URL répond avant de partager le site.

## Mapping photos

| Nom prototype | Source WhatsApp |
|---|---|
| `hero-range-sun.jpg` | `15.56.59.jpeg` |
| `range-blue-sky.jpg` | `15.56.59 (1).jpeg` |
| `biathlon-shooting.jpg` | `15.57.00.jpeg` |
| `aerial.jpg` | `15.59.41 (1).jpeg` (substitut) |
| `cinqcibles-logo.jpeg` | **À fournir manuellement** |
