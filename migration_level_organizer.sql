-- stadedestuffes.fr / table public.events
-- Ajout des colonnes level, organizer, is_highlight, date_tbd
-- Idempotent : rejouable sans risque.
--
-- MODE OPERATOIRE : Supabase Dashboard > SQL Editor > New query > coller > Run.
-- Ce projet n'est pas gere par la CLI Supabase (aucun dossier supabase/ dans le
-- repo, ni sur disque ni suivi par git). Meme mode operatoire que
-- migration_auth.sql et migration_dedup.sql.
--
-- Choix assumes par rapport a la version initiale :
--   * is_highlight et date_tbd sont NULLABLES et sans DEFAULT.
--     NULL = "non renseigne en base", ce qui laisse le front continuer a
--     deviner comme aujourd'hui. Un NOT NULL DEFAULT false aurait ecrase
--     silencieusement toutes les dates incertaines en dates fermes.
--     `WHERE is_highlight` exclut naturellement les NULL, rien a changer
--     cote requetes.
--   * level porte un DEFAULT, un CHECK et un NOT NULL : sans ca, la prochaine
--     insertion du scraper repart a NULL et l'invariant "0 NULL" saute.
--   * pas d'index sur level : 3 valeurs distinctes sur quelques centaines de
--     lignes, Postgres fera un seq scan de toute facon.
--
-- ---------------------------------------------------------------------------
-- CORRECTIONS apportees a la version initiale (audit du 2026-08-28)
-- ---------------------------------------------------------------------------
-- 1. notes ILIKE '%-NA%' resserre en notes ~* 'FFS-[A-Z]+-NA'.
--    Audit mene sur les snapshots FFS du repo (ffs_calendrier.html et
--    ffs_calendrier_biathlon.html), avec le selecteur .title-type que le
--    scraper utilise pour remplir notes (scrape_events.py L305).
--    Resultat : 12 lignes portent "-NA", toutes des codes de type d'epreuve
--    FFS-ALP-NA / FFS-BIATH-NA. NA = National ; les autres suffixes sont des
--    codes de comite regional (RSA, RMB, RDA, RPE, RMV, RAP, RCA, RLY, RMJ).
--    4 de ces 12 lignes ne sont rattrapees par AUCUN autre motif du fichier :
--      - Memorial H.SCARAFIOTI Qualification SCARA U16 (VAL D'ISERE)
--      - Memorial H.SCARAFIOTI Qualification SCARA U14 (VAL D'ISERE)
--      - 31eme Chpt de France Ski Alpin U16 EO (VAL THORENS)
--      - CHAMPIONNATS DE FRANCE (LES TUFFES)
--    La clause est donc porteuse, pas du bruit : la retirer basculait 4
--    evenements nationaux (dont 2 championnats de France) en Régional.
--    Le resserrage protege du seul vrai vecteur de faux positifs : notes
--    recoit aussi de la prose libre pour les sources non FFS (Jura Tourisme,
--    fiches club, soumissions communaute via submit-event.jsx).
--
-- 2. '%championnat de france%' ratait le pluriel ("CHAMPIONNATS DE FRANCE",
--    le S casse la contiguite) et l'abrege ("Chpt de France"). Motifs ajoutes.
--
-- 3. is_highlight : ajout de la garde AND is_highlight IS NULL. Sans elle le
--    fichier n'etait pas reellement idempotent : un rejeu ecrasait a true une
--    mise a false faite a la main dans /admin.html.
--
-- 4. '%OPA%' en ILIKE attrapait "Europa" par accident. Remplace par une
--    limite de mot sur OPA + un motif europa explicite, pour garder les
--    Europa Cup en International sans dependre d'un faux positif heureux.
--
-- 5. level SET NOT NULL apres backfill. Le DEFAULT seul ne protege pas d'un
--    client qui enverrait level: null explicitement (le DEFAULT ne s'applique
--    que si la colonne est omise), et le CHECK initial tolerait NULL :
--    l'invariant "0 NULL" serait reparti a la derive au premier import.
--    Verifie avant de poser la contrainte : ni scrape_events.py (row L309-320),
--    ni admin.jsx (L181), ni submit-event.jsx (L61) n'envoient la colonne
--    level. SET NOT NULL ne casse donc aucun insert existant.
--
-- POINT LAISSE EN L'ETAT, a revoir la saison prochaine : la borne
-- date_start >= DATE '2026-12-01' de l'UPDATE is_highlight est en dur. C'est
-- un classement one-shot pour la saison 2026-2027, pas une regle perenne.
-- ---------------------------------------------------------------------------

BEGIN;

ALTER TABLE public.events
  ADD COLUMN IF NOT EXISTS level        text,
  ADD COLUMN IF NOT EXISTS organizer    text,
  ADD COLUMN IF NOT EXISTS is_highlight boolean,
  ADD COLUMN IF NOT EXISTS date_tbd     boolean;

-- Classement des lignes existantes, du plus specifique au plus general.
UPDATE public.events
   SET level = 'International'
 WHERE level IS NULL
   AND (title ILIKE '%coupe du monde%' OR title ILIKE '%world cup%'
        OR title ILIKE '%tour de ski%'
        OR title ~* '\mOPA\M' OR title ILIKE '%europa%'
        OR notes ILIKE '%coupe du monde%');

UPDATE public.events
   SET level = 'National'
 WHERE level IS NULL
   AND (title ILIKE '%national%' OR title ILIKE '%samse%'
        OR title ILIKE '%championnat% de france%'
        OR title ILIKE '%chpt% de france%'
        OR title ILIKE '%grand prix%'
        OR notes ~* 'FFS-[A-Z]+-NA');

UPDATE public.events
   SET level = 'Régional'
 WHERE level IS NULL;

UPDATE public.events
   SET is_highlight = true
 WHERE is_highlight IS NULL
   AND title ILIKE '%tour de ski%'
   AND date_start >= DATE '2026-12-01';

-- Verrouillage du vocabulaire + valeur par defaut pour les futurs imports.
ALTER TABLE public.events
  ALTER COLUMN level SET DEFAULT 'Régional';

ALTER TABLE public.events
  ALTER COLUMN level SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'events_level_check'
       AND conrelid = 'public.events'::regclass
  ) THEN
    ALTER TABLE public.events
      ADD CONSTRAINT events_level_check
      CHECK (level IN ('International', 'National', 'Régional'));
  END IF;
END $$;

COMMIT;

-- Verification (a lancer separement, apres le Run ci-dessus) :
--
-- SELECT level,
--        count(*)                                   AS nb,
--        count(*) FILTER (WHERE is_highlight)       AS phares,
--        count(*) FILTER (WHERE date_tbd IS NULL)   AS tbd_non_renseigne
--   FROM public.events
--  GROUP BY level
--  ORDER BY nb DESC;
--
-- SELECT count(*) AS level_null FROM public.events WHERE level IS NULL;  -- attendu : 0
--
-- Controle de l'audit distant (a comparer avec l'audit local sur snapshots) :
--
-- SELECT id, title, notes FROM public.events
--  WHERE notes ILIKE '%-NA%' AND notes !~* 'FFS-[A-Z]+-NA';
-- -- attendu : 0 ligne. Si ca ramene quelque chose, ce sont les faux positifs
-- -- que le motif initial aurait classes National a tort. Envoie-les moi.
