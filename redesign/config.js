// Shared config — Supabase URL + anon key.
// The anon key is public by design; row-level security in Supabase
// is what actually protects the data (see migration_auth.sql).

window.STT_CONFIG = {
  SUPABASE_URL: 'https://arkbrvzacbereyukqfte.supabase.co',
  SUPABASE_ANON_KEY: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFya2JydnphY2JlcmV5dWtxZnRlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ5NDg3ODUsImV4cCI6MjA5MDUyNDc4NX0.quPd-OGwu0HYjyvbdcdELlMvNieIBkyOuJHbC3TcM9E',
  FORMSUBMIT_EMAIL: 'cinqcibles@gmail.com',
};
