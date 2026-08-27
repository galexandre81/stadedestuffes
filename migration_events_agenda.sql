-- ============================================================
-- MIGRATION : agenda 2026-2027 — niveau, organisateur, mise en avant
-- ============================================================
-- À exécuter UNE SEULE FOIS dans Supabase :
--   Dashboard → SQL Editor → New query → coller tout → Run
--
-- Ajoute trois colonnes facultatives à `events` :
--   level        — 'Régional' | 'National' | 'International' (texte libre :
--                  les libellés composés type « National / Inter-nations »
--                  restent possibles, le front retient le niveau reconnu)
--   organizer    — organisateur affiché dans la fiche événement
--   is_highlight — met l'épreuve en avant (badge « Événement phare »)
--   date_tbd     — épreuve annoncée pour un mois sans jour publié : le site
--                  affiche « date à confirmer » au lieu du jour stocké
--
-- Le site et le scraper fonctionnent SANS cette migration : ils détectent
-- l'absence des colonnes et retombent sur l'ancien schéma (le niveau est alors
-- déduit du titre et des notes, et la mise en avant vient du seed front).
-- ============================================================

ALTER TABLE public.events
  ADD COLUMN IF NOT EXISTS level        text,
  ADD COLUMN IF NOT EXISTS organizer    text,
  ADD COLUMN IF NOT EXISTS is_highlight boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS date_tbd     boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_events_level ON public.events(level);

-- Reprise des données déjà en base : niveau déduit du titre / des notes.
UPDATE public.events
   SET level = 'International'
 WHERE level IS NULL
   AND (title ILIKE '%coupe du monde%' OR title ILIKE '%world cup%'
        OR title ILIKE '%tour de ski%' OR title ILIKE '%OPA%'
        OR notes ILIKE '%coupe du monde%');

UPDATE public.events
   SET level = 'National'
 WHERE level IS NULL
   AND (title ILIKE '%national%' OR title ILIKE '%samse%'
        OR title ILIKE '%championnat de france%' OR title ILIKE '%grand prix%'
        OR notes ILIKE '%-NA%');

UPDATE public.events
   SET level = 'Régional'
 WHERE level IS NULL;

-- Mise en avant du Tour de Ski 2027 si l'épreuve est déjà en base.
UPDATE public.events
   SET is_highlight = true
 WHERE title ILIKE '%tour de ski%'
   AND date_start >= DATE '2026-12-01';
