-- stadedestuffes.fr / table public.events
-- Migration 2 : taxonomie level, date_tbd, consolidation des doublons
-- A jouer APRES migration_level_organizer.sql.
-- Idempotent : rejouable sans risque.
--
-- MODE OPERATOIRE : Supabase Dashboard > SQL Editor > New query > coller > Run.
--
-- AUCUN DELETE, AUCUN TRUNCATE, AUCUNE MODIFICATION DE POLICY RLS.
-- Une seule contrainte est reconstruite (events_level_check), celle posee par
-- la migration precedente, pour lui ajouter une quatrieme valeur.
--
-- ---------------------------------------------------------------------------
-- 1. Le CHECK a 3 valeurs forcait a mentir
-- ---------------------------------------------------------------------------
-- Sur les 14 lignes classees 'Regional' par la migration precedente, 4 ne sont
-- pas des competitions : deux seances d'entrainement de course d'orientation,
-- un symposium et une journee "Portail Montagne". Le fallback les a etiquetees
-- Regional faute de mieux, et le front leur collerait un badge de niveau faux.
-- Une quatrieme valeur 'Autre' regle le probleme a la racine.
--
-- La cyclosportive "Cyclo Haut Jura" est volontairement laissee en Regional :
-- c'est une competition, meme si elle n'est pas au calendrier FFS.
--
-- ---------------------------------------------------------------------------
-- 2. date_tbd avait deja deux clients, en clair dans les notes
-- ---------------------------------------------------------------------------
-- La Transju'Jeunes : "jour exact encore non publie par l'organisation".
-- La Transju'Trails : "le parcours 2027 officiel n'est pas encore publie,
-- a revalider avant publication".
--
-- ---------------------------------------------------------------------------
-- 3. Consolidation des doublons multi-jours
-- ---------------------------------------------------------------------------
-- Le scraper FFS creait une ligne par jour de competition. Le correctif est
-- dans le code (scrape_events.py passe desormais par dedup_upsert_event), mais
-- les lignes deja en base restent a consolider.
--
-- Methode, sans rien supprimer : on etend date_end sur la ligne qui porte la
-- date de debut, puis on repasse la ligne redondante en status 'pending'.
-- La policy "events_read" n'expose que status = 'published' a anon : la ligne
-- disparait donc du site public sans quitter la base. Elle reapparaitra dans
-- /admin.html parmi les evenements en attente : c'est voulu, tu peux la
-- supprimer toi-meme depuis la (le DELETE est reserve au role admin).
--
-- ROLLBACK de cette section, si le resultat ne te convient pas :
--   UPDATE public.events SET status = 'published'
--    WHERE status = 'pending' AND source_name = 'FFS calendrier';
--   UPDATE public.events SET status = 'published', is_highlight = true
--    WHERE date_start = DATE '2027-01-03' AND title ILIKE '%tour de ski%';
--
-- Trois groupes traites. Seul le premier est encore a venir, les deux autres
-- sont passes et n'apparaissent que dans la section "evenements passes".
--   * Tour de Ski        2027-01-01 + 2027-01-03  -> 1 evenement du 01 au 03
--   * Championnats FR    2026-03-28 + 2026-03-29  -> 1 evenement du 28 au 29
--     (la ligne du 2026-03-27 est CONSERVEE : c'est le biathlon, pas le fond)
--   * SAMSE Nat. Tour 6  2026-03-14 + 2026-03-15  -> 1 evenement du 14 au 15
-- ---------------------------------------------------------------------------

BEGIN;

-- 1. Vocabulaire elargi -------------------------------------------------------

ALTER TABLE public.events DROP CONSTRAINT IF EXISTS events_level_check;

ALTER TABLE public.events
  ADD CONSTRAINT events_level_check
  CHECK (level IN ('International', 'National', 'Régional', 'Autre'));

UPDATE public.events
   SET level = 'Autre'
 WHERE level = 'Régional'
   AND (title ILIKE '%entraînement%' OR title ILIKE '%entrainement%'
        OR title ILIKE '%symposium%'
        OR title ILIKE '%portail montagne%'
        OR title ILIKE '%portes ouvertes%'
        OR title ~* '\mstages?\M');

-- 2. date_tbd -----------------------------------------------------------------

UPDATE public.events
   SET date_tbd = true
 WHERE date_tbd IS NULL
   AND (notes ILIKE '%non publié%'
        OR notes ILIKE '%pas encore publié%'
        OR notes ILIKE '%à revalider%'
        OR notes ILIKE '%date à confirmer%');

-- 3. Consolidation des doublons -----------------------------------------------

-- 3a. Tour de Ski : 1er au 3 janvier 2027
UPDATE public.events
   SET date_end = DATE '2027-01-03'
 WHERE date_start = DATE '2027-01-01'
   AND title ILIKE '%tour de ski%'
   AND (date_end IS NULL OR date_end < DATE '2027-01-03');

UPDATE public.events
   SET is_highlight = false,
       status       = 'pending'
 WHERE date_start = DATE '2027-01-03'
   AND title ILIKE '%tour de ski%'
   AND status = 'published';

-- 3b. Championnats de France de ski de fond : 28 au 29 mars 2026
--     (le 27 mars est le biathlon, il reste une ligne distincte)
UPDATE public.events
   SET date_end = DATE '2026-03-29'
 WHERE date_start = DATE '2026-03-28'
   AND title ILIKE '%championnats de france%'
   AND (date_end IS NULL OR date_end < DATE '2026-03-29');

UPDATE public.events
   SET status = 'pending'
 WHERE date_start = DATE '2026-03-29'
   AND title ILIKE '%championnats de france%'
   AND status = 'published';

-- 3c. SAMSE National Tour 6 : 14 au 15 mars 2026
UPDATE public.events
   SET date_end = DATE '2026-03-15'
 WHERE date_start = DATE '2026-03-14'
   AND title ILIKE '%samse national tour 6%'
   AND (date_end IS NULL OR date_end < DATE '2026-03-15');

UPDATE public.events
   SET status = 'pending'
 WHERE date_start = DATE '2026-03-15'
   AND title ILIKE '%samse national tour 6%'
   AND status = 'published';

COMMIT;

-- ---------------------------------------------------------------------------
-- Verification (a lancer separement)
-- ---------------------------------------------------------------------------
--
-- Repartition apres coup. Attendu : Autre = 4, plus aucun badge de niveau faux.
--
-- SELECT level, status, count(*) AS nb,
--        count(*) FILTER (WHERE is_highlight)  AS phares,
--        count(*) FILTER (WHERE date_tbd)      AS dates_a_confirmer
--   FROM public.events
--  GROUP BY level, status
--  ORDER BY level, status;
--
-- Un seul evenement phare doit rester publie. Attendu : 1 ligne.
--
-- SELECT id, title, date_start, date_end, status
--   FROM public.events
--  WHERE is_highlight AND status = 'published';
--
-- Les lignes mises de cote, a supprimer toi-meme depuis /admin.html si tu veux.
--
-- SELECT id, title, date_start, sport, status
--   FROM public.events
--  WHERE status = 'pending'
--  ORDER BY date_start;
