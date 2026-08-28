/* global React, ReactDOM */
const { useState, useEffect, useMemo } = React;

const MONTHS_SHORT = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Juin', 'Juil', 'Août', 'Sep', 'Oct', 'Nov', 'Déc'];
const MONTHS_LONG = ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre'];

// Saison courante — à mettre à jour une fois par an (voir aussi les <title>/meta
// des pages HTML et le libellé de venir.jsx).
const SEASON = { long: '2026 – 2027', short: '26–27' };

const SPORTS = [
  { key: 'all', label: 'Toutes disciplines' },
  { key: 'Biathlon', label: 'Biathlon' },
  { key: 'Ski de fond', label: 'Ski de fond' },
  { key: 'Combiné nordique', label: 'Combiné' },
  { key: 'Saut à ski', label: 'Saut à ski' },
  { key: 'Para', label: 'Para' },
  { key: 'Cyclisme', label: 'Cyclisme' },
];

const LEVELS = [
  { key: 'all', label: 'Tous niveaux' },
  { key: 'International', label: 'International' },
  { key: 'National', label: 'National' },
  { key: 'Régional', label: 'Régional' },
  // Evenements sans niveau de competition : entrainements de club, symposiums,
  // journees portes ouvertes. Sans cette entree, eventLevel() ne reconnaissait
  // pas la valeur et retombait sur la deduction par titre, qui leur collait un
  // badge « Régional » faux.
  { key: 'Autre', label: 'Autre' },
];

// `level` est renseigné pour les épreuves vérifiées à la main ; pour les
// événements simplement scrapés on le déduit du titre et des notes (les codes
// FFS « -NA » valent national, « -CR »/« -SR » régional).
const RE_INTERNATIONAL = /coupe du monde|world cup|tour de ski|\bfis\b|\bibu\b|international|inter-nations/i;
const RE_NATIONAL = /national|championnat de france|france\b|samse|grand prix|\bffs-[a-z]+-na\b/i;

const LEVEL_SLUG = { International: 'intl', National: 'nat', 'Régional': 'reg', Autre: 'autre' };

// Libellé du badge « phare » : on précise Coupe du monde quand c'en est une.
function highlightLabel(ev) {
  const isWorldCup = /coupe du monde|world cup|tour de ski/i.test(`${ev.title || ''} ${ev.notes || ''}`);
  return isWorldCup ? '★ Événement phare · Coupe du monde' : '★ Événement phare';
}

function eventLevel(ev) {
  if (ev.level) {
    // « National / Inter-nations » et autres libellés composés → 1re valeur connue.
    const found = LEVELS.slice(1).find(l => ev.level.toLowerCase().includes(l.key.toLowerCase()));
    if (found) return found.key;
  }
  const haystack = `${ev.title || ''} ${ev.notes || ''}`;
  if (RE_INTERNATIONAL.test(haystack)) return 'International';
  if (RE_NATIONAL.test(haystack)) return 'National';
  return 'Régional';
}

const today = () => {
  const d = new Date();
  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
};
const parseDate = (s) => s ? new Date(s + 'T12:00:00') : null;
const daysBetween = (a, b) => Math.round((a - b) / 86400000);
const dateStatus = (ev) => {
  const t = today();
  const start = parseDate(ev.date_start);
  const end = ev.date_end ? parseDate(ev.date_end) : start;
  if (!start) return 'future';
  if (end < t) return 'past';
  return daysBetween(start, t) <= 14 ? 'soon' : 'future';
};

function fmtRange(ev) {
  const s = parseDate(ev.date_start);
  if (!s) return '—';
  // Épreuve annoncée pour un mois sans jour publié : on ne montre pas de jour.
  if (ev.date_tbd) return `${MONTHS_LONG[s.getMonth()]} ${s.getFullYear()} · date à confirmer`;
  if (ev.date_end && ev.date_end !== ev.date_start) {
    const e = parseDate(ev.date_end);
    if (s.getMonth() === e.getMonth()) return `${s.getDate()}–${e.getDate()} ${MONTHS_LONG[s.getMonth()]} ${s.getFullYear()}`;
    return `${s.getDate()} ${MONTHS_LONG[s.getMonth()]} – ${e.getDate()} ${MONTHS_LONG[e.getMonth()]} ${e.getFullYear()}`;
  }
  return `${s.getDate()} ${MONTHS_LONG[s.getMonth()]} ${s.getFullYear()}`;
}

function truncate(text, max) {
  if (!text || text.length <= max) return text;
  return text.slice(0, text.lastIndexOf(' ', max) > 0 ? text.lastIndexOf(' ', max) : max).trim() + '…';
}

function fmtArticleDate(s) {
  if (!s) return '';
  // published_at peut être une date (YYYY-MM-DD) OU un timestamp ISO complet.
  // parseDate() ajoutait 'T12:00:00' même aux timestamps complets → Invalid Date
  // → affichait « NaN undefined NaN ». On n'ajoute l'heure que pour une date nue.
  const d = new Date(s + (s.length === 10 ? 'T12:00:00' : ''));
  if (isNaN(d)) return '';
  return `${d.getDate()} ${MONTHS_LONG[d.getMonth()]} ${d.getFullYear()}`;
}

function Disclaimer() {
  return (
    <div className="disclaimer">
      Site indépendant — sans lien avec le CNSNMM ou l'ENSM. Données agrégées depuis les sources publiques.&nbsp;
      <a href="https://cnsnmm.sports.gouv.fr/" target="_blank" rel="noopener">Site officiel</a>
    </div>
  );
}

function Nav({ theme, onToggleTheme }) {
  return (
    <nav className="nav">
      <div className="brand">
        <span className="brand-name">Les Tuffes</span>
        <span className="brand-sub">Stade nordique · Prémanon</span>
      </div>
      <div className="nav-links">
        <a className="nav-link active">Calendrier</a>
        <a className="nav-link" href="articles.html">Actualités</a>
        <a className="nav-link" href="venir-aux-tuffes.html">Venir aux Tuffes</a>
        <a className="nav-link" href="a-propos.html">À propos</a>
      </div>
      <div className="nav-right">
        <a className="btn-esf" href="https://www.esf-lesrousses.com/nordique/montagne-experiences/visite-stade-tuffes-raquettes/" target="_blank" rel="noopener">Visiter avec l'ESF</a>
        <button className="icon-btn" onClick={onToggleTheme} aria-label="Changer le thème">
          {theme === 'dark' ? (
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>
          ) : (
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>
          )}
        </button>
      </div>
    </nav>
  );
}

function Hero({ next, onSelect }) {
  return (
    <section className="hero">
      <div className="hero-photo has-photo">
        <img src="photos/hero-range-sun.jpg" alt="Stade nordique des Tuffes — pas de tir au lever du soleil" className="photo-img" />
        <span className="ph-corner">Prémanon · Jura · 1020 m</span>
      </div>
      <div className="hero-content">
        <div>
          <div className="hero-eyebrow">Saison {SEASON.long}</div>
          <h1 className="hero-title">Le calendrier des compétitions du <em>stade nordique</em> des Tuffes.</h1>
          <p className="hero-deck">Une saison entière de ski de fond, biathlon, combiné nordique et saut à ski, réunie en un seul endroit. Tout ce qui se passe à Prémanon, en un coup d'œil.</p>
          <p className="hero-deck" style={{ marginTop: 16, fontSize: 14, color: 'var(--accent)', fontWeight: 500 }}>
            ⚠ Calendrier mis à jour automatiquement à partir d'informations trouvées en ligne ou d'annonces directes. Ce n'est pas une source officielle — vérifiez toujours auprès des organisateurs avant de vous déplacer.
          </p>
        </div>
        {next && (
          <div className="hero-next">
            <div className="hero-next-eyebrow">Prochain événement</div>
            {next.is_highlight && <div className="event-flag">{highlightLabel(next)}</div>}
            <div className="hero-next-date">{fmtRange(next)}</div>
            <div className="hero-next-title">{next.title}</div>
            <div className="hero-next-meta">
              <span><span className="dot"></span>{next.sport}</span>
              {next.public_access === true && <span>· Public admis</span>}
              {next.has_catering === true && <span>· Restauration sur place</span>}
            </div>
            <a className="hero-next-link" onClick={() => onSelect(next)} style={{ cursor: 'pointer' }}>Voir les détails →</a>
          </div>
        )}
      </div>
    </section>
  );
}

function EventRow({ ev, onClick }) {
  const status = dateStatus(ev);
  const s = parseDate(ev.date_start);
  const e = ev.date_end ? parseDate(ev.date_end) : null;
  let dayDisplay = String(s.getDate()).padStart(2, '0');
  if (e && e.getMonth() === s.getMonth() && e.getDate() !== s.getDate()) {
    dayDisplay = `${dayDisplay}–${String(e.getDate()).padStart(2, '0')}`;
  }
  const level = eventLevel(ev);
  return (
    <div className={'event-row' + (ev.is_highlight ? ' highlight' : '')} onClick={onClick}>
      <div className={'event-date' + (status === 'past' ? ' past' : '')}>
        {ev.date_tbd ? <span className="tbd">Date à confirmer</span> : dayDisplay}
        <span className="month">{MONTHS_SHORT[s.getMonth()]} {s.getFullYear()}</span>
      </div>
      <div className="event-main">
        <div className="event-discipline">
          {ev.sport}
          <span className={'event-level lvl-' + LEVEL_SLUG[level]}>{level}</span>
        </div>
        {ev.is_highlight && <div className="event-flag">{highlightLabel(ev)}</div>}
        <h3 className="event-title">{ev.title}</h3>
        <div className="event-info">
          {ev.public_access === true && <span><span className="dot public"></span>Public admis</span>}
          {ev.public_access === false && <span><span className="dot restricted"></span>Accès restreint</span>}
          {ev.has_catering === true && <span>Restauration</span>}
          {ev.organizer && <span>Org. {ev.organizer}</span>}
          {ev.notes && <span style={{ opacity: 0.7 }}>· {truncate(ev.notes, 120)}</span>}
        </div>
      </div>
      <div className="event-arrow">→</div>
    </div>
  );
}

function Events({ events, filter, level, onSelect }) {
  const filtered = events.filter(e =>
    (filter === 'all' || e.sport === filter) &&
    (level === 'all' || eventLevel(e) === level)
  );
  const upcoming = filtered.filter(e => dateStatus(e) !== 'past').sort((a, b) => new Date(a.date_start) - new Date(b.date_start));
  const past = filtered.filter(e => dateStatus(e) === 'past').sort((a, b) => new Date(b.date_start) - new Date(a.date_start));
  const [openPast, setOpenPast] = useState(false);
  return (
    <>
      <div className="events">
        {upcoming.map(ev => <EventRow key={ev.id} ev={ev} onClick={() => onSelect(ev)} />)}
        {upcoming.length === 0 && (
          <div style={{ padding: '40px 0', textAlign: 'center', fontFamily: 'var(--serif)', color: 'var(--muted)', fontStyle: 'italic' }}>
            Aucune compétition à venir pour cette sélection.
          </div>
        )}
      </div>
      {past.length > 0 && (
        <>
          <button
            type="button"
            className={'past-toggle' + (openPast ? ' open' : '')}
            onClick={() => setOpenPast(o => !o)}
            aria-expanded={openPast}
          >
            <span>Compétitions passées · {past.length}</span>
            <span className="chevron">⌄</span>
          </button>
          {openPast && (
            <div className="events">
              {past.map(ev => <EventRow key={ev.id} ev={ev} onClick={() => onSelect(ev)} />)}
            </div>
          )}
        </>
      )}
    </>
  );
}

function NewsSection({ articles }) {
  const visible = articles.slice(0, 4);
  return (
    <section className="section">
      <div className="section-head">
        <div>
          <div className="section-eyebrow">Actualités</div>
          <h2 className="section-title">Dans la <em>presse</em></h2>
        </div>
        <div className="section-meta">
          Articles agrégés depuis NordicMag, FFS,<br/>Ski-Nordique et clubs locaux
        </div>
      </div>
      <div className="news-grid">
        {visible.map((a) => {
          const Tag = a.url ? 'a' : 'div';
          const linkProps = a.url ? { href: a.url, target: '_blank', rel: 'noopener noreferrer' } : {};
          return (
            <Tag key={a.id} className={'news-item' + (a.image_url ? '' : ' no-photo')} {...linkProps}>
              {a.image_url && (
                <div className="thumb has-photo">
                  <img src={a.image_url} alt="" className="photo-img" />
                </div>
              )}
              <div className="meta">
                <span className="source">{a.source_name}</span>
                <span>·</span>
                <span>{fmtArticleDate(a.published_at)}</span>
              </div>
              <h3>{a.title}</h3>
              <p>{a.summary}</p>
            </Tag>
          );
        })}
      </div>
      <div className="news-link">
        <a href="articles.html">Voir toutes les actualités →</a>
      </div>
    </section>
  );
}

function Venir() {
  return (
    <section className="venir">
      <div className="venir-text">
        <div className="eyebrow">Infos pratiques</div>
        <h2>Venir aux <em>Tuffes</em></h2>
        <p>Accès, parking, hébergement, restauration et itinéraires pour préparer sereinement votre venue à Prémanon, dans le Jura.</p>
        <div className="venir-coords">
          <div className="venir-coord"><div className="lbl">Altitude</div><div className="val">1020 m</div></div>
          <div className="venir-coord"><div className="lbl">Commune</div><div className="val">Prémanon</div></div>
          <div className="venir-coord"><div className="lbl">Massif</div><div className="val">Jura</div></div>
        </div>
        <a className="venir-link" href="venir-aux-tuffes.html">Préparer ma venue →</a>
      </div>
      <div className="venir-photo has-photo">
        <img src="photos/aerial.jpg" alt="Vue aérienne du stade nordique des Tuffes" className="photo-img" />
      </div>
    </section>
  );
}

function Sponsor() {
  return (
    <section className="sponsor">
      <div className="sponsor-photo sponsor-logo-frame">
        <img src="photos/cinqcibles-logo.jpeg" alt="Cinq Cibles — la marque lifestyle du biathlon" className="sponsor-logo-img" />
        <span className="ad-stamp">Partenaire</span>
      </div>
      <div className="sponsor-content">
        <div className="sponsor-eyebrow">Cinq Cibles · Partenaire de la saison</div>
        <h2 className="sponsor-title">La marque <em>lifestyle</em> du biathlon.</h2>
        <p className="sponsor-deck">
          T-shirts, sweats, accessoires et collections en collaboration directe avec des athlètes —
          pensés pour celles et ceux qui vivent ce sport intensément. Une partie des ventes
          alimente un fonds qui soutient les clubs et les athlètes du biathlon :
          en achetant, tu deviens leur sponsor.
        </p>
        <div className="sponsor-pillars">
          <div className="sponsor-pillar"><span className="num">Brodé</span><span className="lbl">à 800 m du pas de tir du stade des Tuffes, Prémanon</span></div>
          <div className="sponsor-pillar"><span className="num">Créé</span><span className="lbl">aux Rousses, Jura</span></div>
          <div className="sponsor-pillar"><span className="num">Sur commande</span><span className="lbl">slow fashion · zéro surstock</span></div>
        </div>
        <a className="sponsor-cta" href="https://www.cinqcibles.fr" target="_blank" rel="noopener">Découvrir la boutique →</a>
      </div>
    </section>
  );
}

function Footer() {
  return (
    <footer className="footer">
      <div className="footer-grid">
        <div className="footer-brand">
          <h4>Les Tuffes</h4>
          <p>Site indépendant qui rassemble le calendrier des compétitions et la veille presse pour le stade nordique de Prémanon. Sans lien avec le CNSNMM ou l'ENSM.</p>
        </div>
        <div className="footer-col"><h5>Navigation</h5><ul>
          <li><a href="index.html">Calendrier</a></li>
          <li><a href="articles.html">Actualités</a></li>
          <li><a href="venir-aux-tuffes.html">Venir aux Tuffes</a></li>
          <li><a href="a-propos.html">À propos</a></li>
        </ul></div>
        <div className="footer-col"><h5>Disciplines</h5><ul>
          <li>Biathlon</li>
          <li>Ski de fond</li>
          <li>Combiné nordique</li>
          <li>Saut à ski</li>
          <li>Para-nordique</li>
        </ul></div>
        <div className="footer-col"><h5>Contribuer</h5><ul>
          <li><a href="#annoncer" onClick={(e) => {
            e.preventDefault();
            if (typeof window.openSubmitEvent === 'function') window.openSubmitEvent();
            else { console.error('[Tuffes] submit-event.js non chargé'); alert('Le formulaire n\'est pas disponible. Réessayez dans quelques secondes.'); }
          }}>Annoncer un événement</a></li>
          <li><a href="#contact" onClick={(e) => {
            e.preventDefault();
            if (typeof window.openContact === 'function') window.openContact('Signalement d\'erreur');
            else { console.error('[Tuffes] contact.js non chargé — impossible d\'ouvrir le modal'); alert('Le formulaire de contact n\'est pas disponible. Réessayez dans quelques secondes ou écrivez à cinqcibles@gmail.com'); }
          }}>Signaler une erreur</a></li>
          <li><a href="#contact" onClick={(e) => {
            e.preventDefault();
            if (typeof window.openContact === 'function') window.openContact('Devenir partenaire');
            else { console.error('[Tuffes] contact.js non chargé'); alert('Le formulaire de contact n\'est pas disponible. Réessayez ou écrivez à cinqcibles@gmail.com'); }
          }}>Devenir partenaire</a></li>
        </ul></div>
      </div>
      <div className="footer-bottom">
        <span>© 2026 — Les Tuffes · Site indépendant</span>
        <span><a href="mentions-legales.html">Mentions légales · Confidentialité</a></span>
      </div>
    </footer>
  );
}

function EventModal({ event, onClose }) {
  if (!event) return null;
  const level = eventLevel(event);
  // Toutes les sources listées : source principale + sources ajoutées par la
  // fusion de doublons côté scraper (additional_sources).
  const sources = [
    { source_name: event.source_name, source_url: event.source_url },
    ...(Array.isArray(event.additional_sources) ? event.additional_sources : []),
  ].filter(s => s && (s.source_name || s.source_url));
  return (
    <div className="overlay open" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <div className="modal-photo has-photo">
          <img src={event.sport && event.sport.toLowerCase().includes('biathlon') ? 'photos/biathlon-shooting.jpg' : 'photos/range-blue-sky.jpg'} alt="" className="photo-img" />
          <button className="modal-close" onClick={onClose}>×</button>
        </div>
        <div className="modal-body">
          <div className="modal-eyebrow">
            {event.sport}
            <span className={'event-level lvl-' + LEVEL_SLUG[level]}>{level}</span>
          </div>
          {event.is_highlight && <div className="event-flag modal-flag">{highlightLabel(event)}</div>}
          <h3 className="modal-title">{event.title}</h3>
          <div className="modal-rows">
            <div className="modal-row"><div className="lbl">Dates</div><div className="val">{fmtRange(event)}</div></div>
            <div className="modal-row"><div className="lbl">Lieu</div><div className="val">Stade nordique des Tuffes — Prémanon, Jura</div></div>
            <div className="modal-row"><div className="lbl">Niveau</div><div className="val">{event.level || level}</div></div>
            {event.organizer && <div className="modal-row"><div className="lbl">Organisation</div><div className="val">{event.organizer}</div></div>}
            <div className="modal-row"><div className="lbl">Public</div><div className="val">{event.public_access === true ? 'Admis · accès libre' : event.public_access === false ? 'Restreint · sur invitation' : 'À confirmer'}</div></div>
            <div className="modal-row"><div className="lbl">Restauration</div><div className="val">{event.has_catering === true ? 'Sur place' : event.has_catering === false ? 'Aucune' : 'À confirmer'}</div></div>
            {event.notes && <div className="modal-row"><div className="lbl">Type d'épreuve</div><div className="val">{event.notes}</div></div>}
          </div>
          <div className="modal-source">
            <div style={{ marginBottom: 8 }}>
              <strong style={{ color: 'var(--text)', fontStyle: 'normal', fontFamily: 'var(--cond)', fontSize: 11, letterSpacing: '0.18em', textTransform: 'uppercase', fontWeight: 600 }}>Source</strong>
            </div>
            {event.source_url ? (
              <>
                <p style={{ marginBottom: 8 }}>
                  Information récupérée depuis{' '}
                  {sources.map((src, i) => (
                    <React.Fragment key={(src.source_url || src.source_name) + i}>
                      {i > 0 && ', '}
                      {src.source_url ? (
                        <a href={src.source_url} target="_blank" rel="noopener" style={{ color: 'var(--accent)', borderBottom: '1px solid var(--accent)', fontStyle: 'normal' }}>{src.source_name || 'la source'}</a>
                      ) : (src.source_name)}
                    </React.Fragment>
                  ))}.
                </p>
                <a className="modal-cta" href={event.source_url} target="_blank" rel="noopener">
                  Infos pratiques & billetterie →
                </a>
              </>
            ) : (
              <p style={{ marginBottom: 8 }}>
                Information : {event.source_name || 'Communauté'}.
                {event.notes && event.notes.startsWith('FFS') && (
                  <> Calendrier officiel FFS : <a href="https://www.ffs.fr/calendrier" target="_blank" rel="noopener" style={{ color: 'var(--accent)', borderBottom: '1px solid var(--accent)', fontStyle: 'normal' }}>ffs.fr/calendrier</a>.</>
                )}
              </p>
            )}
            <p>⚠ Ce calendrier n'est pas officiel. Vérifiez auprès de l'organisateur avant de vous déplacer.</p>
          </div>
        </div>
      </div>
    </div>
  );
}

function App() {
  const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{ "theme": "light" }/*EDITMODE-END*/;

  const [tweaks, setTweak] = window.useTweaks(TWEAK_DEFAULTS);
  const [filter, setFilter] = useState('all');
  const [level, setLevel] = useState('all');
  const [selected, setSelected] = useState(null);
  const [, forceRender] = React.useReducer(x => x + 1, 0);

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', tweaks.theme || 'light');
  }, [tweaks.theme]);

  useEffect(() => {
    const handler = () => forceRender();
    if (window.STT_LOADING) {
      window.addEventListener('stt-data-ready', handler);
      return () => window.removeEventListener('stt-data-ready', handler);
    }
    // Data already arrived before this effect ran — force a render to read it.
    forceRender();
  }, []);

  // Ouvre le modal de soumission si l'URL est /#annoncer (depuis n'importe quelle page)
  useEffect(() => {
    const checkHash = () => {
      if (window.location.hash === '#annoncer' && typeof window.openSubmitEvent === 'function') {
        window.openSubmitEvent();
        history.replaceState(null, '', window.location.pathname + window.location.search);
      }
    };
    checkHash();
    window.addEventListener('hashchange', checkHash);
    return () => window.removeEventListener('hashchange', checkHash);
  }, []);

  const events = window.STT_EVENTS || [];
  const articles = window.STT_ARTICLES || [];
  const upcoming = events.filter(e => dateStatus(e) !== 'past').sort((a, b) => new Date(a.date_start) - new Date(b.date_start));
  const next = upcoming[0];

  const toggleTheme = () => setTweak('theme', tweaks.theme === 'light' ? 'dark' : 'light');

  const TweaksUI = window.TweaksPanel;

  return (
    <>
      <Disclaimer />
      <Nav theme={tweaks.theme} onToggleTheme={toggleTheme} />
      <Hero next={next} onSelect={setSelected} />

      <section className="section" data-screen-label="Calendrier">
        <div className="section-head">
          <div>
            <div className="section-eyebrow">Calendrier · Saison {SEASON.short}</div>
            <h2 className="section-title">Les <em>compétitions</em><br/>aux Tuffes</h2>
          </div>
          <div className="section-meta">
            {upcoming.length} épreuve{upcoming.length > 1 ? 's' : ''} à venir<br/>
            Dernière mise à jour : aujourd'hui
          </div>
        </div>
        <div className="filter-groups">
          <div className="filter-group">
            <span className="filter-label">Discipline</span>
            <div className="filters" role="group" aria-label="Filtrer par discipline">
              {SPORTS.map(s => (
                <button
                  key={s.key}
                  type="button"
                  className={'filter-btn' + (filter === s.key ? ' active' : '')}
                  aria-pressed={filter === s.key}
                  onClick={() => setFilter(s.key)}
                >{s.label}</button>
              ))}
            </div>
          </div>
          <div className="filter-group">
            <span className="filter-label">Niveau</span>
            <div className="filters" role="group" aria-label="Filtrer par niveau">
              {LEVELS.map(l => (
                <button
                  key={l.key}
                  type="button"
                  className={'filter-btn' + (level === l.key ? ' active' : '')}
                  aria-pressed={level === l.key}
                  onClick={() => setLevel(l.key)}
                >{l.label}</button>
              ))}
            </div>
          </div>
        </div>
        {window.STT_ERROR && (
          <div className="fld-error" style={{ marginBottom: 24 }}>
            Impossible de charger le calendrier complet depuis Supabase ({window.STT_ERROR}).
            Seuls les événements confirmés à la main sont affichés — rechargez la page pour réessayer.
          </div>
        )}
        <Events events={events} filter={filter} level={level} onSelect={setSelected} />

        <div className="cta-annoncer">
          <div className="cta-annoncer-content">
            <div className="cta-annoncer-eyebrow">Contribuer · Communauté</div>
            <h3 className="cta-annoncer-title">Une compétition <em>manquante</em> ?</h3>
            <p>
              Vous organisez un événement aux Tuffes — course, stage, démo, animation — ou
              vous en repérez un qui n'apparaît pas dans le calendrier ? Soumettez-le en
              une minute. Examen avant publication.
            </p>
          </div>
          <button className="cta-annoncer-btn" onClick={() => window.openSubmitEvent()}>
            Annoncer un événement →
          </button>
        </div>
      </section>

      <NewsSection articles={articles} />
      <Venir />
      <Sponsor />
      <Footer />

      <EventModal event={selected} onClose={() => setSelected(null)} />

      <TweaksUI title="Tweaks">
        <window.TweakSection label="Apparence" />
        <window.TweakRadio label="Thème" value={tweaks.theme} options={['light', 'dark']} onChange={v => setTweak('theme', v)} />
      </TweaksUI>
    </>
  );
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<App />);
