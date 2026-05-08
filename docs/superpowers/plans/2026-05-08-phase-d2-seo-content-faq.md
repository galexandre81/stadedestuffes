# Phase D2 — SEO contenu, FAQ, JSON-LD Event

> **REQUIRED SUB-SKILL:** superpowers:subagent-driven-development.

**Goal:** Ajouter intros, FAQ, et injection JSON-LD Event dynamique. Spec : section 5.4.

**Pre-requis:** Phase D1 terminée.

---

## Task D2.1 : Intro index.html

- [ ] Insérer après le hero, avant le calendrier, un `<section class="intro-seo">` ~80 mots situant le stade (Prémanon, Jura), citant les 4 disciplines, précisant "agrégateur non officiel".
- [ ] Style minimal cohérent avec le CSS existant (couleurs --frost, --muted).
- [ ] Vérifier qu'il n'y a qu'un seul `<h1>` sur la page.
- [ ] Commit : `feat(seo): intro éditoriale index pour mots-clés géo`.

## Task D2.2 : Intro articles.html

- [ ] Section ~50 mots : "Revue de presse autour du Stade Nordique des Tuffes…" + caractère agrégateur.
- [ ] Commit.

## Task D2.3 : FAQ venir-aux-tuffes.html

- [ ] Lire la page existante. Identifier sections en double éventuelles.
- [ ] Ajouter section `<section id="faq">` avec 7 questions (spec 5.4 venir-aux-tuffes).
- [ ] Markup HTML structuré : `<h2>FAQ</h2>` puis pour chaque question `<details><summary>Question</summary><p>Réponse</p></details>` (collapsible accessible).
- [ ] CSS minimal pour visibilité sur fond sombre.
- [ ] Ajouter le JSON-LD `FAQPage` dans le `<head>` :
```html
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "FAQPage",
  "mainEntity": [
    {"@type": "Question", "name": "Comment se rendre…", "acceptedAnswer": {"@type": "Answer", "text": "<copie de la réponse>"}},
    ... (7 entrées)
  ]
}
</script>
```
- [ ] Validation : le texte du JSON-LD doit correspondre aux réponses HTML.
- [ ] Commit.

## Task D2.4 : Injection JSON-LD Event sur index.html

- [ ] Identifier la fonction JS qui peuple le calendrier après fetch Supabase.
- [ ] Après l'insertion DOM des events, ajouter pour chaque event :
```js
function injectEventJsonLd(event) {
  const ld = {
    "@context": "https://schema.org",
    "@type": "SportsEvent",
    "name": event.title,
    "startDate": event.date_start,
    "endDate": event.date_end || event.date_start,
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
    "sport": event.sport,
    "url": `https://stadedestuffes.fr/#event-${event.id}`
  };
  const s = document.createElement("script");
  s.type = "application/ld+json";
  s.textContent = JSON.stringify(ld);
  document.head.appendChild(s);
}
```
- [ ] Boucler sur la liste d'events après fetch et appeler `injectEventJsonLd`.
- [ ] Vérifier en DevTools : `<head>` contient N scripts JSON-LD après chargement.
- [ ] Vérifier console : aucune erreur CSP. Si erreur CSP : ajouter le hash sha256 du script ou élargir `script-src`.
- [ ] Commit : `feat(seo): JSON-LD SportsEvent injecté pour chaque event Supabase`.

## Task D2.5 : Validation Rich Results Test (HUMAN)

- [ ] Push origin main avec confirmation utilisateur.
- [ ] Demander à l'utilisateur de tester sur https://search.google.com/test/rich-results :
  - `https://stadedestuffes.fr/` → doit détecter au moins 1 SportsEvent
  - `https://stadedestuffes.fr/venir-aux-tuffes.html` → doit détecter FAQPage avec 7 entrées
- [ ] Si erreurs reportées → corriger et re-push.

---

## Critères d'acceptation Phase D2

- [ ] Intros visibles sur index et articles
- [ ] FAQ avec 7 questions sur venir-aux-tuffes.html, accessible (details/summary)
- [ ] JSON-LD Event injecté dynamiquement (visible en DevTools)
- [ ] Rich Results Test valide WebSite, BreadcrumbList, FAQPage, SportsEvent
- [ ] Aucune régression visuelle
