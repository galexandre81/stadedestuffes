# Phase D1 — SEO meta, sitemap, robots, canonical Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mettre en place les fondamentaux SEO statiques : robots.txt, sitemap.xml, balises Open Graph + Twitter Card + canonical sur les 5 pages, schema.org WebSite + BreadcrumbList.

**Architecture:** 100% statique, pas de build, pas de JS. Modifications HTML uniquement dans le `<head>`.

**Tech Stack:** HTML, XML, schema.org JSON-LD.

**Pre-requis:** Phases 0, A, B terminées. Repo en état propre.

**Source de vérité :** `docs/superpowers/specs/2026-05-08-stadedestuffes-improvements-design.md` section 5.

---

## File Structure

- **À créer** :
  - `robots.txt` à la racine
  - `sitemap.xml` à la racine

- **À modifier** :
  - `index.html` (head)
  - `articles.html` (head)
  - `venir-aux-tuffes.html` (head)
  - `mentions-legales.html` (head)
  - `admin.html` (head — noindex)

---

## Task D1.1 : Créer robots.txt

- [ ] **Step 1.1: Créer le fichier**

Contenu de `robots.txt` :

```
User-agent: *
Allow: /
Disallow: /admin.html

Sitemap: https://stadedestuffes.fr/sitemap.xml
```

- [ ] **Step 1.2: Vérifier accessibilité locale**

```powershell
Get-Content robots.txt
```

Expected: contenu visible.

- [ ] **Step 1.3: Commit**

```powershell
git add robots.txt
git commit -m "feat(seo): robots.txt avec disallow admin et lien sitemap"
```

## Task D1.2 : Créer sitemap.xml

- [ ] **Step 2.1: Créer le fichier**

Contenu de `sitemap.xml` (remplacer `<lastmod>` par la date du jour, format YYYY-MM-DD) :

```xml
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://stadedestuffes.fr/</loc>
    <lastmod>2026-05-08</lastmod>
    <changefreq>weekly</changefreq>
    <priority>1.0</priority>
  </url>
  <url>
    <loc>https://stadedestuffes.fr/articles.html</loc>
    <lastmod>2026-05-08</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>
  <url>
    <loc>https://stadedestuffes.fr/venir-aux-tuffes.html</loc>
    <lastmod>2026-05-08</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.9</priority>
  </url>
  <url>
    <loc>https://stadedestuffes.fr/mentions-legales.html</loc>
    <lastmod>2026-05-08</lastmod>
    <changefreq>yearly</changefreq>
    <priority>0.2</priority>
  </url>
</urlset>
```

- [ ] **Step 2.2: Validation XML**

```powershell
[xml]$x = Get-Content sitemap.xml; $x.urlset.url.Count
```

Expected: `4`.

- [ ] **Step 2.3: Commit**

```powershell
git add sitemap.xml
git commit -m "feat(seo): sitemap.xml avec 4 pages publiques"
```

## Task D1.3 : Audit CSP existante

- [ ] **Step 3.1: Lire la CSP actuelle**

```powershell
Select-String -Path "*.html" -Pattern "Content-Security-Policy"
```

Note la directive `script-src` exacte.

- [ ] **Step 3.2: Vérifier que script type="application/ld+json" est autorisé**

JSON-LD est techniquement un `<script>` mais avec un type non exécutable. La CSP `script-src` peut le bloquer selon les navigateurs. Si la CSP a `script-src 'self'` strict, il faudra ajouter `'unsafe-inline'` UNIQUEMENT pour les JSON-LD ou bien utiliser un hash CSP (sha256).

**Décision pragmatique** : conserver la CSP actuelle, vérifier en console après injection. Si bloqué, fallback : générer un hash sha256 du JSON-LD et l'ajouter à la directive.

- [ ] **Step 3.3: Documenter ce qui a été trouvé**

Logger dans la console : "CSP actuelle : <copier la directive>".

## Task D1.4 : index.html — head SEO

- [ ] **Step 4.1: Lire le head actuel**

```powershell
Get-Content index.html -TotalCount 30
```

- [ ] **Step 4.2: Améliorer la meta description**

Dans `<meta name="description">`, remplacer par :

```html
<meta name="description" content="Calendrier des compétitions au Stade Nordique des Tuffes (Prémanon, Jura) : biathlon, ski de fond, combiné nordique, saut à ski. Site agrégateur non officiel.">
```

- [ ] **Step 4.3: Ajouter canonical**

Après la meta description :

```html
<link rel="canonical" href="https://stadedestuffes.fr/">
```

- [ ] **Step 4.4: Ajouter Open Graph**

```html
<meta property="og:title" content="Stade Nordique des Tuffes — Calendrier">
<meta property="og:description" content="Toutes les compétitions de biathlon, ski de fond, combiné nordique et saut à ski au Stade des Tuffes (Prémanon, Jura).">
<meta property="og:type" content="website">
<meta property="og:url" content="https://stadedestuffes.fr/">
<meta property="og:image" content="https://stadedestuffes.fr/og-default.jpg">
<meta property="og:locale" content="fr_FR">
<meta property="og:site_name" content="Stade des Tuffes">
```

- [ ] **Step 4.5: Ajouter Twitter Card**

```html
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="Stade Nordique des Tuffes — Calendrier">
<meta name="twitter:description" content="Calendrier non officiel des compétitions au Stade des Tuffes.">
<meta name="twitter:image" content="https://stadedestuffes.fr/og-default.jpg">
```

- [ ] **Step 4.6: Ajouter schema.org WebSite**

```html
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "WebSite",
  "name": "Stade Nordique des Tuffes",
  "alternateName": "Stade des Tuffes",
  "url": "https://stadedestuffes.fr/",
  "description": "Agrégateur non officiel des compétitions et actualités au Stade Nordique des Tuffes (Prémanon, Jura).",
  "inLanguage": "fr-FR"
}
</script>
```

- [ ] **Step 4.7: Validation HTML**

```powershell
# Vérifier qu'on n'a pas cassé le HTML
Select-String -Path index.html -Pattern "</head>"
```

Expected: 1 match.

- [ ] **Step 4.8: Commit**

```powershell
git add index.html
git commit -m "feat(seo): meta OG, Twitter Card, canonical, JSON-LD WebSite sur index"
```

## Task D1.5 : articles.html — head SEO

- [ ] **Step 5.1: Mêmes balises adaptées**

Dans `<head>` de `articles.html` :

```html
<meta name="description" content="Actualités et revue de presse autour du Stade Nordique des Tuffes : biathlon, ski de fond, combiné nordique. Articles filtrés sur Prémanon et le Haut-Jura.">
<link rel="canonical" href="https://stadedestuffes.fr/articles.html">

<meta property="og:title" content="Actualités — Stade des Tuffes">
<meta property="og:description" content="Revue de presse des sports nordiques au Stade des Tuffes (Prémanon, Jura).">
<meta property="og:type" content="website">
<meta property="og:url" content="https://stadedestuffes.fr/articles.html">
<meta property="og:image" content="https://stadedestuffes.fr/og-default.jpg">
<meta property="og:locale" content="fr_FR">

<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="Actualités — Stade des Tuffes">
<meta name="twitter:description" content="Revue de presse des sports nordiques au Stade des Tuffes.">
<meta name="twitter:image" content="https://stadedestuffes.fr/og-default.jpg">
```

- [ ] **Step 5.2: Schema WebSite + BreadcrumbList**

```html
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "BreadcrumbList",
  "itemListElement": [
    {"@type": "ListItem", "position": 1, "name": "Accueil", "item": "https://stadedestuffes.fr/"},
    {"@type": "ListItem", "position": 2, "name": "Actualités", "item": "https://stadedestuffes.fr/articles.html"}
  ]
}
</script>
```

- [ ] **Step 5.3: Commit**

```powershell
git add articles.html
git commit -m "feat(seo): meta OG, canonical, BreadcrumbList sur articles"
```

## Task D1.6 : venir-aux-tuffes.html — head SEO

- [ ] **Step 6.1: Balises principales**

```html
<meta name="description" content="Comment se rendre au Stade Nordique des Tuffes (Prémanon, Jura) : accès, parking, transports, horaires des compétitions.">
<link rel="canonical" href="https://stadedestuffes.fr/venir-aux-tuffes.html">

<meta property="og:title" content="Venir au Stade des Tuffes — Accès, parking, infos pratiques">
<meta property="og:description" content="Itinéraire, parking et infos pratiques pour assister à une compétition au Stade Nordique des Tuffes.">
<meta property="og:type" content="article">
<meta property="og:url" content="https://stadedestuffes.fr/venir-aux-tuffes.html">
<meta property="og:image" content="https://stadedestuffes.fr/og-default.jpg">
<meta property="og:locale" content="fr_FR">

<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="Venir au Stade des Tuffes">
<meta name="twitter:description" content="Itinéraire, parking, infos pratiques.">
<meta name="twitter:image" content="https://stadedestuffes.fr/og-default.jpg">
```

- [ ] **Step 6.2: Schema BreadcrumbList**

```html
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "BreadcrumbList",
  "itemListElement": [
    {"@type": "ListItem", "position": 1, "name": "Accueil", "item": "https://stadedestuffes.fr/"},
    {"@type": "ListItem", "position": 2, "name": "Venir aux Tuffes", "item": "https://stadedestuffes.fr/venir-aux-tuffes.html"}
  ]
}
</script>
```

(Note : le FAQPage sera ajouté en Phase D2.)

- [ ] **Step 6.3: Commit**

```powershell
git add venir-aux-tuffes.html
git commit -m "feat(seo): meta OG, canonical, BreadcrumbList sur venir-aux-tuffes"
```

## Task D1.7 : mentions-legales.html — noindex follow

- [ ] **Step 7.1: Balises minimales**

```html
<meta name="robots" content="noindex, follow">
<link rel="canonical" href="https://stadedestuffes.fr/mentions-legales.html">
<meta name="description" content="Mentions légales du site stadedestuffes.fr.">
```

- [ ] **Step 7.2: Commit**

```powershell
git add mentions-legales.html
git commit -m "feat(seo): noindex follow + canonical sur mentions légales"
```

## Task D1.8 : admin.html — noindex nofollow

- [ ] **Step 8.1: Balises**

```html
<meta name="robots" content="noindex, nofollow">
```

Pas de canonical, pas de meta description (page interne).

- [ ] **Step 8.2: Commit**

```powershell
git add admin.html
git commit -m "feat(seo): noindex nofollow strict sur admin"
```

## Task D1.9 : OG image par défaut (HUMAN-IN-THE-LOOP)

- [ ] **Step 9.1: Demander à l'utilisateur**

> "Pour l'image Open Graph par défaut (`og-default.jpg`, 1200×630 px) : tu en fournis une (photo du stade, logo, paysage Jura) ou je dois en générer une sobre depuis le code (ex : nom du site sur fond bleu nuit) ?"

Si génération : créer une image SVG simple convertie en JPG (script Python ou outil externe).

Si fournie : la sauver à la racine, vérifier dimensions :

```powershell
# Vérifier la taille en pixels
Add-Type -AssemblyName System.Drawing
$img = [System.Drawing.Image]::FromFile((Resolve-Path og-default.jpg))
"$($img.Width)x$($img.Height)"
$img.Dispose()
```

Expected: `1200x630`.

- [ ] **Step 9.2: Commit**

```powershell
git add og-default.jpg
git commit -m "feat(seo): image Open Graph par défaut 1200x630"
```

---

## Critères d'acceptation Phase D1

- [ ] `robots.txt` et `sitemap.xml` à la racine
- [ ] 5 fichiers HTML mis à jour avec balises SEO appropriées
- [ ] `og-default.jpg` 1200x630 présente
- [ ] `view-source:` sur chaque page → balises OG, Twitter, canonical visibles
- [ ] Aucune erreur console liée à la CSP

---

## Self-review (interne)

- ✅ Couvre 5.3 (fichiers à créer) et 5.4 partiellement (head des 5 pages)
- ✅ Pas de claim "site officiel"
- ✅ noindex pour admin et mentions
- ✅ Hand-off explicite pour image OG
