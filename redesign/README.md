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
| `/admin.html` *(noindex)* | `admin.jsx` | `admin.js` |

Partagés : `shell.jsx` (Disclaimer/Nav/Footer), `tweaks-panel.jsx` (no-op), `submit-event.jsx` (modal de soumission), `data.js` (Supabase fetch), `config.js` (clés), `styles.css`, `photos/`, `favicon.svg`.

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

### 3. Appliquer la migration auth + RLS dans Supabase

Le fichier [`../migration_auth.sql`](../migration_auth.sql) à la racine du repo contient tout le SQL nécessaire pour activer l'authentification et les rôles (admin / publisher / guest).

1. Va sur https://supabase.com/dashboard/project/arkbrvzacbereyukqfte/sql/new
2. Copie-colle l'intégralité du contenu de `migration_auth.sql`
3. Clique **Run**
4. Vérifie qu'aucune erreur n'apparaît (les `DROP POLICY IF EXISTS` peuvent être no-op, c'est normal)

### 4. Bootstrap : se créer un admin

Une fois la migration passée, suis cette séquence **dans l'ordre** :

1. Ouvre https://stadedestuffes.fr/admin.html
2. Entre ton email (`cinqcibles@gmail.com`)
3. Clique sur le lien magic link reçu par mail
4. Tu te retrouves loggé mais en rôle `guest` → aucun droit (page « Compte en attente »)
5. Reviens dans le SQL Editor de Supabase, exécute :
   ```sql
   UPDATE public.profiles SET role = 'admin'
   WHERE id = (SELECT id FROM auth.users WHERE email = 'cinqcibles@gmail.com');
   ```
6. Recharge `admin.html` → tu as les droits complets

### 5. Onboarder un utilisateur CNSNMM (rôle publisher = peut publier sans validation)

**Option A — Le user s'inscrit lui-même** :
1. Tu lui donnes l'URL `https://stadedestuffes.fr/admin.html`
2. Il entre son email, clique le magic link, atterrit en `guest`
3. Tu lui assignes le rôle `publisher` :
   ```sql
   UPDATE public.profiles SET role = 'publisher'
   WHERE id = (SELECT id FROM auth.users WHERE email = 'leur@email.fr');
   ```

**Option B — Tu l'invites depuis Supabase** : Dashboard → Authentication → Users → Add user → Invite. Pareil ensuite pour le rôle.

### 6. (Optionnel) Désactiver les inscriptions publiques

Si tu veux que personne ne puisse demander un magic link sans être déjà invité :
- Supabase Dashboard → Authentication → Providers → Email
- Décoche **"Enable signups"** (ou "Allow new users to sign up")

Ce n'est pas strictement nécessaire (les `guest` n'ont aucun droit de toute façon), mais ça évite que ta table `auth.users` se remplisse aléatoirement.

### 7. Push

```
git add redesign/ .github/
git commit -m "Refonte production"
git push
```

Le workflow se lance, compile, déploie. Vérifier dans l'onglet **Actions** sur GitHub.

### 8. Archive l'ancien site (optionnel)

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
