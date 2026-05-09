/* global React, ReactDOM */
const { useState, useMemo } = React;

const DISCIPLINES = [
  { key: 'all', label: 'Toutes disciplines' },
  { key: 'biathlon', label: 'Biathlon' },
  { key: 'fond', label: 'Ski de fond' },
  { key: 'combine', label: 'Combiné' },
  { key: 'saut', label: 'Saut à ski' },
  { key: 'para', label: 'Para' },
];

function articleMatchesDiscipline(a, key) {
  if (key === 'all') return true;
  const tags = (a.sport_tags || []).map(t => String(t).toLowerCase());
  return tags.includes(key);
}

function ArticleCard({ a }) {
  const Tag = a.url ? 'a' : 'div';
  const linkProps = a.url ? { href: a.url, target: '_blank', rel: 'noopener noreferrer' } : {};
  return (
    <Tag className={'news-item' + (a.image_url ? '' : ' no-photo')} {...linkProps}>
      {a.image_url && (
        <div className="thumb has-photo">
          <img src={a.image_url} alt="" className="photo-img" />
        </div>
      )}
      <div className="meta">
        <span className="source">{a.source_name}</span>
        <span>·</span>
        <span>{window.fmtArticleDate(a.published_at)}</span>
        {a.mentions_tuffes && <><span>·</span><span style={{ color: 'var(--accent)' }}>Mentionne les Tuffes</span></>}
      </div>
      <h3>{a.title}</h3>
      <p>{a.summary}</p>
    </Tag>
  );
}

function ArticlesPage() {
  const [, forceRender] = React.useReducer(x => x + 1, 0);
  React.useEffect(() => {
    const handler = () => forceRender();
    if (window.STT_LOADING) {
      window.addEventListener('stt-data-ready', handler);
      return () => window.removeEventListener('stt-data-ready', handler);
    }
    forceRender();
  }, []);

  const articles = window.STT_ARTICLES || [];
  const [discipline, setDiscipline] = useState('all');
  const [tuffesOnly, setTuffesOnly] = useState(false);
  const [source, setSource] = useState('all');

  const sources = useMemo(() => {
    const set = new Set(articles.map(a => a.source_name).filter(Boolean));
    return ['all', ...Array.from(set).sort()];
  }, [articles]);

  const filtered = useMemo(() => {
    return articles.filter(a => {
      if (!articleMatchesDiscipline(a, discipline)) return false;
      if (tuffesOnly && !a.mentions_tuffes) return false;
      if (source !== 'all' && a.source_name !== source) return false;
      return true;
    });
  }, [articles, discipline, tuffesOnly, source]);

  return (
    <>
      <window.Disclaimer />
      <window.Nav active="articles" />

      <section className="page-hero">
        <div className="page-hero-eyebrow">Actualités · Veille presse</div>
        <h1 className="page-hero-title">Actualités <em>nordiques</em></h1>
        <p className="page-hero-deck">
          Les articles publiés dans la presse française et locale autour du stade des Tuffes :
          biathlon, ski de fond, combiné nordique, saut à ski. Agrégés depuis NordicMag, FFS,
          Ski-Nordique et les sites des clubs jurassiens.
        </p>
      </section>

      <section className="section">
        <div className="filters">
          {DISCIPLINES.map(d => (
            <button key={d.key} className={'filter-btn' + (discipline === d.key ? ' active' : '')} onClick={() => setDiscipline(d.key)}>{d.label}</button>
          ))}
          <button className={'filter-btn' + (tuffesOnly ? ' active' : '')} onClick={() => setTuffesOnly(t => !t)}>Tuffes uniquement</button>
        </div>

        {sources.length > 2 && (
          <div className="filters" style={{ marginBottom: 32 }}>
            {sources.map(s => (
              <button key={s} className={'filter-btn' + (source === s ? ' active' : '')} onClick={() => setSource(s)}>
                {s === 'all' ? 'Toutes sources' : s}
              </button>
            ))}
          </div>
        )}

        <div className="articles-grid">
          {filtered.map(a => <ArticleCard key={a.id || a.url} a={a} />)}
        </div>

        {filtered.length === 0 && (
          <div style={{ padding: '60px 0', textAlign: 'center', fontFamily: 'var(--sans)', color: 'var(--muted)', fontStyle: 'italic' }}>
            Aucun article ne correspond à ces filtres.
          </div>
        )}
      </section>

      <window.Footer />
    </>
  );
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<ArticlesPage />);
