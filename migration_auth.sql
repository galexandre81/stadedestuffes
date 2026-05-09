-- ============================================================
-- MIGRATION : auth + RLS pour stadedestuffes.fr
-- ============================================================
-- À exécuter UNE SEULE FOIS dans Supabase :
--   Dashboard → SQL Editor → New query → coller tout → Run
--
-- Ce script :
--  1. Crée la table profiles (1 ligne par utilisateur authentifié, avec un rôle)
--  2. Auto-crée un profil "guest" à chaque nouvelle inscription
--  3. Active la Row Level Security sur events
--  4. Définit les policies par rôle :
--     - anon       : SELECT events publiés, INSERT en status='pending'
--     - guest      : pareil que anon (compte créé mais pas encore validé)
--     - publisher  : SELECT all, INSERT directement publié, UPDATE
--     - admin      : tout (y compris DELETE)
-- ============================================================

-- 1. Table profiles
CREATE TABLE IF NOT EXISTS public.profiles (
  id           uuid PRIMARY KEY REFERENCES auth.users ON DELETE CASCADE,
  role         text NOT NULL DEFAULT 'guest'
                 CHECK (role IN ('admin', 'publisher', 'guest')),
  display_name text,
  created_at   timestamptz DEFAULT now()
);

ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

-- Tout user authentifié peut lire son propre profil (pour connaître son rôle)
DROP POLICY IF EXISTS "profiles_self_read" ON public.profiles;
CREATE POLICY "profiles_self_read" ON public.profiles
  FOR SELECT TO authenticated
  USING (auth.uid() = id);

-- 2. Trigger : à chaque user créé dans auth.users, on insère son profil
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  INSERT INTO public.profiles (id, role) VALUES (NEW.id, 'guest');
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();

-- 3. Helper SQL : retourne le rôle de l'utilisateur courant
CREATE OR REPLACE FUNCTION public.user_role()
RETURNS text LANGUAGE sql SECURITY DEFINER STABLE AS $$
  SELECT role FROM public.profiles WHERE id = auth.uid();
$$;

-- 4. RLS sur events (drop des anciennes policies "tout ouvert" si présentes)
ALTER TABLE public.events ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "anon_select_all" ON public.events;
DROP POLICY IF EXISTS "anon_insert_all" ON public.events;
DROP POLICY IF EXISTS "anon_update_all" ON public.events;
DROP POLICY IF EXISTS "anon_delete_all" ON public.events;
DROP POLICY IF EXISTS "events_read" ON public.events;
DROP POLICY IF EXISTS "events_anon_insert_pending" ON public.events;
DROP POLICY IF EXISTS "events_authenticated_insert" ON public.events;
DROP POLICY IF EXISTS "events_authenticated_update" ON public.events;
DROP POLICY IF EXISTS "events_admin_delete" ON public.events;

-- Lecture : anon voit les published, publishers/admins voient tout (y compris pending)
CREATE POLICY "events_read" ON public.events FOR SELECT
  USING (
    status = 'published'
    OR public.user_role() IN ('admin', 'publisher')
  );

-- Insertion anon : seulement en pending
CREATE POLICY "events_anon_insert_pending" ON public.events FOR INSERT TO anon
  WITH CHECK (status = 'pending');

-- Insertion authentifié : publisher/admin peuvent insérer en n'importe quel status
CREATE POLICY "events_authenticated_insert" ON public.events FOR INSERT TO authenticated
  WITH CHECK (public.user_role() IN ('admin', 'publisher'));

-- Update : publisher/admin (validation, modification, dépublication)
CREATE POLICY "events_authenticated_update" ON public.events FOR UPDATE TO authenticated
  USING (public.user_role() IN ('admin', 'publisher'))
  WITH CHECK (public.user_role() IN ('admin', 'publisher'));

-- Delete : seul l'admin
CREATE POLICY "events_admin_delete" ON public.events FOR DELETE TO authenticated
  USING (public.user_role() = 'admin');

-- ============================================================
-- BOOTSTRAP : se créer un admin (à faire APRÈS s'être loggé une 1ère fois)
-- ============================================================
-- 1. Va sur https://stadedestuffes.fr/admin.html
-- 2. Entre ton email (cinqcibles@gmail.com) → clique le magic link reçu
-- 3. Tu es loggé, mais en rôle 'guest' → tu vois rien
-- 4. Reviens ici dans le SQL Editor, exécute :
--
--      UPDATE public.profiles SET role = 'admin'
--      WHERE id = (SELECT id FROM auth.users WHERE email = 'cinqcibles@gmail.com');
--
-- 5. Recharge la page admin → tu as tous les droits.
--
-- Pour onboarder un utilisateur CNSNMM (rôle publisher = peut publier sans validation) :
--   - Soit il s'inscrit lui-même via admin.html (magic link)
--   - Soit tu l'invites depuis Supabase Dashboard → Auth → Add user
--   - Puis tu lui assignes le rôle :
--
--      UPDATE public.profiles SET role = 'publisher'
--      WHERE id = (SELECT id FROM auth.users WHERE email = 'leur@email.fr');
--
-- Pour révoquer un accès :
--      UPDATE public.profiles SET role = 'guest'
--      WHERE id = (SELECT id FROM auth.users WHERE email = 'leur@email.fr');
-- ============================================================
