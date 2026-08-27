// Data layer — fetches events + articles from Supabase, then notifies React.
// Uses Supabase REST directly (no SDK) to avoid an extra CDN load.

const SUPABASE_URL = window.STT_CONFIG.SUPABASE_URL;
const SUPABASE_ANON_KEY = window.STT_CONFIG.SUPABASE_ANON_KEY;

window.STT_EVENTS = [];
window.STT_ARTICLES = [];
window.STT_LOADING = true;
window.STT_ERROR = null;

// Colonnes ajoutées par migration_events_agenda.sql. Tant que la migration
// n'est pas passée, PostgREST renvoie une 400 sur ces colonnes : on retombe
// alors sur le jeu de colonnes historique plutôt que de casser le calendrier.
const EVENT_COLUMNS_BASE =
  'id,title,sport,date_start,date_end,public_access,has_catering,notes,source_name,source_url';
const EVENT_COLUMNS_EXTENDED = `${EVENT_COLUMNS_BASE},additional_sources,level,organizer,is_highlight,date_tbd`;

const STOPWORDS = new Set([
  'de', 'du', 'des', 'le', 'la', 'les', 'un', 'une', 'et', 'au', 'aux', 'en',
  'sur', 'pour', 'par', 'dans', 'ski', 'nordique', 'stade', 'tuffes',
  'premanon', 'france', 'jura', 'edition', 'competition', 'course', 'open',
  'challenge', 'tour', 'epreuve',
]);

function titleTokens(title) {
  const norm = (title || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ');
  return new Set(norm.split(/\s+/).filter(t => t.length >= 3 && !STOPWORDS.has(t)));
}

function jaccard(a, b) {
  if (!a.size || !b.size) return 0;
  let inter = 0;
  a.forEach(t => { if (b.has(t)) inter += 1; });
  return inter / (a.size + b.size - inter);
}

function sameEvent(a, b) {
  const da = Date.parse(`${a.date_start}T12:00:00`);
  const db = Date.parse(`${b.date_start}T12:00:00`);
  if (isNaN(da) || isNaN(db)) return false;
  if (Math.abs(da - db) > 86400000) return false;
  return jaccard(titleTokens(a.title), titleTokens(b.title)) >= 0.5;
}

// Champs que le seed peut compléter sur une ligne Supabase incomplète
// (typiquement quand la migration level/organizer/is_highlight n'est pas passée).
const SEED_FILLABLE = ['level', 'organizer', 'is_highlight', 'date_tbd', 'notes', 'date_end', 'source_name', 'source_url'];

// Ajoute les événements confirmés à la main (events-seed.js) que Supabase ne
// renvoie pas encore — même titre ± 1 jour = doublon : on garde la ligne
// Supabase, enrichie des champs que le seed est seul à connaître.
function mergeSeed(events) {
  const seed = window.STT_SEED_EVENTS || [];
  const extra = [];
  seed.forEach((s) => {
    const match = events.find(e => sameEvent(e, s));
    if (!match) {
      extra.push(s);
      return;
    }
    SEED_FILLABLE.forEach((field) => {
      if (match[field] === undefined || match[field] === null || match[field] === '') {
        match[field] = s[field];
      }
    });
  });
  if (!extra.length) return events;
  return events.concat(extra).sort((a, b) => (a.date_start < b.date_start ? -1 : 1));
}

(async function loadData() {
  const headers = {
    apikey: SUPABASE_ANON_KEY,
    Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
  };
  const eventsUrl = (columns) =>
    `${SUPABASE_URL}/rest/v1/events?status=eq.published&order=date_start.asc&select=${columns}`;
  const articlesUrl = `${SUPABASE_URL}/rest/v1/press_articles?status=eq.published&order=published_at.desc.nullslast&limit=24&select=id,title,url,source_name,published_at,summary,image_url,sport_tags,mentions_tuffes`;

  try {
    let [evRes, artRes] = await Promise.all([
      fetch(eventsUrl(EVENT_COLUMNS_EXTENDED), { headers }),
      fetch(articlesUrl, { headers }),
    ]);
    if (!evRes.ok) {
      console.warn('STT: colonnes étendues indisponibles, repli sur le schéma de base');
      evRes = await fetch(eventsUrl(EVENT_COLUMNS_BASE), { headers });
    }
    if (!evRes.ok) throw new Error(`events HTTP ${evRes.status}`);
    if (!artRes.ok) throw new Error(`articles HTTP ${artRes.status}`);
    window.STT_EVENTS = await evRes.json();
    window.STT_ARTICLES = await artRes.json();
  } catch (err) {
    window.STT_ERROR = err.message || 'Erreur de chargement';
    console.error('STT data load failed:', err);
  } finally {
    // Même en cas d'erreur réseau, les événements confirmés restent affichés.
    window.STT_EVENTS = mergeSeed(window.STT_EVENTS || []);
    window.STT_LOADING = false;
    window.dispatchEvent(new CustomEvent('stt-data-ready'));
  }
})();
