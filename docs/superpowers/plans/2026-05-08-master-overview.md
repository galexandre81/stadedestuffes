# Plan master — Améliorations stadedestuffes.fr

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement these plans phase by phase. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply Phases 1-3 from BRIEF + ambitious SEO/GEO to stadedestuffes.fr, end-to-end.

**Architecture:** Static GitHub Pages site + Supabase backend + Python scrapers. SEO via hybrid (static meta/sitemap/FAQ + client-side JSON-LD for events).

**Tech Stack:** Python 3.13 (scrapers), HTML/CSS/JS (front), Supabase (DB), pytest (tests), GitHub Actions (cron scrapers).

---

## Spec source

`docs/superpowers/specs/2026-05-08-stadedestuffes-improvements-design.md`

## Plans (à exécuter dans l'ordre)

| Phase | Fichier | Durée |
|---|---|---|
| 0 | `2026-05-08-phase-0-setup-git.md` | 15-30 min |
| A | `2026-05-08-phase-a-ffs-validation.md` | 30-45 min |
| B | `2026-05-08-phase-b-content.md` | 1h30-2h |
| D | `2026-05-08-phase-d-seo-geo.md` | 2h-2h30 |
| C | `2026-05-08-phase-c-cta-rotation.md` | 30 min |

**Ordre justifié** dans la spec section 2. Chaque phase se termine par un commit propre. Tu peux t'arrêter entre 2 phases sans bloquer les suivantes.

## Checkpoints utilisateur (humain dans la boucle)

- **Avant Phase B** : confirmer que `migration_dedup.sql` a été exécuté manuellement dans Supabase
- **Avant chaque run réel des scrapers** contre Supabase prod : confirmation explicite
- **Entre chaque phase** : revue rapide + approbation pour passer à la suivante

## Définition de "fait"

Voir spec section 8.
