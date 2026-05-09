// Data layer — fetches events + articles from Supabase, then notifies React.
// Uses Supabase REST directly (no SDK) to avoid an extra CDN load.

const SUPABASE_URL = 'https://arkbrvzacbereyukqfte.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFya2JydnphY2JlcmV5dWtxZnRlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ5NDg3ODUsImV4cCI6MjA5MDUyNDc4NX0.quPd-OGwu0HYjyvbdcdELlMvNieIBkyOuJHbC3TcM9E';

window.STT_EVENTS = [];
window.STT_ARTICLES = [];
window.STT_LOADING = true;
window.STT_ERROR = null;

(async function loadData() {
  const headers = {
    apikey: SUPABASE_ANON_KEY,
    Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
  };
  const eventsUrl = `${SUPABASE_URL}/rest/v1/events?status=eq.published&order=date_start.asc&select=id,title,sport,date_start,date_end,public_access,has_catering,notes,source_name,source_url`;
  const articlesUrl = `${SUPABASE_URL}/rest/v1/press_articles?status=eq.published&order=published_at.desc&limit=24&select=id,title,url,source_name,published_at,summary,image_url,sport_tags,mentions_tuffes`;

  try {
    const [evRes, artRes] = await Promise.all([
      fetch(eventsUrl, { headers }),
      fetch(articlesUrl, { headers }),
    ]);
    if (!evRes.ok) throw new Error(`events HTTP ${evRes.status}`);
    if (!artRes.ok) throw new Error(`articles HTTP ${artRes.status}`);
    window.STT_EVENTS = await evRes.json();
    window.STT_ARTICLES = await artRes.json();
  } catch (err) {
    window.STT_ERROR = err.message || 'Erreur de chargement';
    console.error('STT data load failed:', err);
  } finally {
    window.STT_LOADING = false;
    window.dispatchEvent(new CustomEvent('stt-data-ready'));
  }
})();
