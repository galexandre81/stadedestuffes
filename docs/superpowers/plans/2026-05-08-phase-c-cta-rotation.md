# Phase C — CTA Cinq Cibles rotation 5 variations

> **REQUIRED SUB-SKILL:** superpowers:subagent-driven-development.

**Goal:** Remplacer le CTA Cinq Cibles statique actuel (commit `90e7ec5`) par une rotation aléatoire entre 5 variations, en respectant le format compact (~90px hauteur) déjà adopté.

**Pre-requis:** Phases 0, A, B, D1, D2 terminées.

**Source de vérité visuelle :** `docs/cinq-cibles-rotation.html` (5 variations + JS de rotation). **Adaptation requise :** la maquette utilise un format 320px ; il faut transposer dans le format compact 90px existant.

---

## Task C.1 : Audit du bloc CTA actuel

- [ ] Identifier dans `index.html`, `articles.html`, `venir-aux-tuffes.html` le bloc CTA Cinq Cibles actuel (chercher "cinqcibles", "Voir la boutique", classe `.cta-strip` ou similaire).
- [ ] Capturer la structure HTML/CSS actuelle (logo SVG local, hauteur 90px, couleurs --surface/--accent, layout horizontal).
- [ ] Documenter dans un fichier scratch ce qui est commun aux 3 pages.

## Task C.2 : Créer le partial

- [ ] Fichier `partials/cinq-cibles-cta.html` :
  - 5 variations dans un même conteneur, mais **adaptées au format compact** : 90px hauteur, layout horizontal (logo gauche, texte central, CTA droite), pas le format 320px de la maquette.
  - Chaque variation = un `<div class="cta-variant" data-name="...">` avec un texte court (1 ligne accroche + 1 ligne explication maximum, pas le bloc h2 de 50px de la maquette).
  - Le logo SVG (chemin `/cinqcibles-logo.svg` ou wrapping `<img>`) reste statique, pas dans la rotation.
  - Le bouton CTA "Voir la boutique" garde son style actuel.

- [ ] Les 5 angles éditoriaux à conserver (formulations courtes adaptées au format compact) :
  1. **Voisin** : "Brodé à Prémanon, à 800m du pas de tir."
  2. **Sponsor** : "Vous suivez ce calendrier. Vous êtes déjà le sponsor."
  3. **Caisse de pommes** : "Une caisse de pommes pour une victoire mondiale. Cinq Cibles existe pour ça."
  4. **Slow fashion** : "Brodé à la commande. Pas de surstock. Une part pour les athlètes."
  5. **Collaborations** : "Drops athlètes en précommande. Vous portez leur identité."

- [ ] CSS dans `assets/css/cinq-cibles-cta.css` :
  - Reprendre les variables CSS du site (`--bg`, `--surface`, `--accent`, etc.).
  - Animation `fadeIn` discrète sur la variation active.
  - Display: none sur les variations inactives, .active pour celle visible.
  - Compact (~90px hauteur).

- [ ] Script de rotation aléatoire (JS inline dans le partial ou externalisé). Logique : au load, `Math.random() * 5` pour choisir laquelle a la classe `.active`.

## Task C.3 : Intégrer le partial dans les 3 pages

Pour `index.html`, `articles.html`, `venir-aux-tuffes.html` :

- [ ] Ajouter dans le `<head>` :
```html
<link rel="stylesheet" href="/assets/css/cinq-cibles-cta.css">
```

- [ ] Remplacer le bloc CTA actuel par :
```html
<div id="cc-cta-mount"></div>
<script>
fetch('/partials/cinq-cibles-cta.html')
  .then(r => r.text())
  .then(html => {
    const mount = document.getElementById('cc-cta-mount');
    mount.innerHTML = html;
    // Re-exécuter le script inline pour activer la rotation
    const inline = mount.querySelector('script');
    if (inline) {
      const s = document.createElement('script');
      s.textContent = inline.textContent;
      document.body.appendChild(s);
    }
  });
</script>
```

- [ ] Vérifier que la CSP autorise le `fetch` interne (même origine, donc OK normalement).

- [ ] Vérifier qu'on supprime bien l'ancien bloc CTA (pas de doublon).

## Task C.4 : Tests visuels

- [ ] Ouvrir chaque page localement (file://) ou via serveur statique.
- [ ] F5 plusieurs fois → constater que la variation change.
- [ ] Vérifier desktop (≥720px) et mobile (≤720px).
- [ ] Vérifier console : pas d'erreur, pas de warning CSP.
- [ ] Si MCP Chrome dispo : prendre des captures avant/après.

## Task C.5 : Commit + push

- [ ] Commit unique :
```powershell
git add partials/cinq-cibles-cta.html assets/css/cinq-cibles-cta.css index.html articles.html venir-aux-tuffes.html
git commit -m "feat(cta): rotation aléatoire 5 variations Cinq Cibles (format compact)"
```
- [ ] Push avec confirmation utilisateur.

---

## Critères d'acceptation Phase C

- [ ] `partials/cinq-cibles-cta.html` créé avec 5 variations
- [ ] `assets/css/cinq-cibles-cta.css` créé
- [ ] 3 pages publiques modifiées
- [ ] Au F5 sur 10 reloads : ≥ 3 variations distinctes vues (statistiquement)
- [ ] Format compact respecté (~90px), pas régression à 320px
- [ ] Aucune erreur console
- [ ] Push effectué (avec confirmation)
