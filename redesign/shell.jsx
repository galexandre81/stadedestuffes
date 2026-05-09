// Shared header + footer for sub-pages.
// Pages render <Disclaimer />, <Nav active="..." />, page content, then <Footer />.

const MONTHS_LONG_SHELL = ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre'];

window.fmtArticleDate = function (s) {
  if (!s) return '';
  const d = new Date(s + (s.length === 10 ? 'T12:00:00' : ''));
  if (isNaN(d)) return '';
  return `${d.getDate()} ${MONTHS_LONG_SHELL[d.getMonth()]} ${d.getFullYear()}`;
};

window.Disclaimer = function () {
  return (
    <div className="disclaimer">
      Site indépendant — sans lien avec le CNSNMM ou l'ENSM. Données agrégées depuis les sources publiques.&nbsp;
      <a href="https://cnsnmm.sports.gouv.fr/" target="_blank" rel="noopener">Site officiel</a>
    </div>
  );
};

window.Nav = function ({ active }) {
  const link = (key, href, label) => (
    <a className={'nav-link' + (active === key ? ' active' : '')} href={href}>{label}</a>
  );
  return (
    <nav className="nav">
      <div className="brand">
        <a href="index.html" style={{ display: 'flex', alignItems: 'baseline', gap: 12 }}>
          <span className="brand-name">Les Tuffes</span>
          <span className="brand-sub">Stade nordique · Prémanon</span>
        </a>
      </div>
      <div className="nav-links">
        {link('calendrier', 'index.html', 'Calendrier')}
        {link('articles', 'articles.html', 'Actualités')}
        {link('venir', 'venir-aux-tuffes.html', 'Venir aux Tuffes')}
        {link('apropos', 'a-propos.html', 'À propos')}
      </div>
      <div className="nav-right">
        <a className="btn-esf" href="https://www.esf-lesrousses.com/nordique/montagne-experiences/visite-stade-tuffes-raquettes/" target="_blank" rel="noopener">Visiter avec l'ESF</a>
      </div>
    </nav>
  );
};

window.Footer = function () {
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
        <div className="footer-col"><h5>Disciplines</h5><ul><li>Biathlon</li><li>Ski de fond</li><li>Combiné nordique</li><li>Saut à ski</li><li>Para-nordique</li></ul></div>
        <div className="footer-col"><h5>Contribuer</h5><ul>
          <li><a href="#annoncer" onClick={(e) => { e.preventDefault(); if (window.openSubmitEvent) window.openSubmitEvent(); else window.location.href = 'index.html#annoncer'; }}>Annoncer un événement</a></li>
          <li><a href="#contact" onClick={(e) => { e.preventDefault(); window.openContact('Signalement d\'erreur'); }}>Signaler une erreur</a></li>
          <li><a href="#contact" onClick={(e) => { e.preventDefault(); window.openContact('Devenir partenaire'); }}>Devenir partenaire</a></li>
          <li><a href="mentions-legales.html">Mentions légales</a></li>
        </ul></div>
      </div>
      <div className="footer-bottom">
        <span>© 2026 — Les Tuffes · Site indépendant</span>
        <span><a href="mentions-legales.html">Mentions légales · Confidentialité</a></span>
      </div>
    </footer>
  );
};
