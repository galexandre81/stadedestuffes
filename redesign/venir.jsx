/* global React, ReactDOM */
const { useState } = React;

const DISTANCES = [
  { city: 'Genève (CH)', time: '45 min', detail: 'Par l\'A40 puis col de la Faucille ou route des Rousses. Frontière à passer.' },
  { city: 'Aéroport de Genève (CH)', time: '< 50 min', detail: 'Aéroport international le plus proche. Voitures de location sur place, accès direct par autoroute suisse jusqu\'à Nyon puis frontière de la Cure.' },
  { city: 'Lausanne (CH)', time: '55 min', detail: 'Par Nyon et la frontière de la Cure (col de la Givrine).' },
  { city: 'Lons-le-Saunier', time: '1 h', detail: 'Par Champagnole et Morez.' },
  { city: 'Lyon', time: '2 h', detail: 'Par l\'A42 puis A40 jusqu\'à Bellegarde, ensuite col de la Faucille.' },
  { city: 'Paris', time: '4 h 30', detail: 'Par l\'A6 jusqu\'à Mâcon ou Bourg-en-Bresse, puis A40 et col de la Faucille.' },
];

const HEBERGEMENTS = [
  { lieu: 'Les Rousses', detail: 'Station principale du massif, hôtels et locations toutes catégories.' },
  { lieu: 'Prémanon', detail: 'Village du stade, gîtes et chambres d\'hôtes à proximité immédiate.' },
  { lieu: 'Bois d\'Amont', detail: 'Village voisin, ambiance plus calme, locations familiales.' },
  { lieu: 'Lamoura', detail: 'Village d\'altitude, accès direct aux pistes du Risoux.' },
  { lieu: 'La Cure (Suisse)', detail: 'Côté suisse de la frontière, à 20 min à pied du stade. Pratique pour les visiteurs venant de l\'arc lémanique en train.' },
];

const FAQ = [
  {
    q: 'Le stade est-il accessible librement ?',
    a: 'Le stade nordique des Tuffes est la propriété du CNSNMM (Centre National de Ski Nordique et de Moyenne Montagne). L\'accès n\'est pas libre en permanence : il dépend de l\'occupation du site (entraînements des équipes nationales, stages fermés, compétitions). Le CNSNMM ouvre régulièrement des créneaux au grand public — historiquement les mardis et vendredis en fin de journée — et l\'accès aux abords est libre l\'été tant que les pistes ne sont pas enneigées. Pour une venue ponctuelle, contactez le CNSNMM (03 84 60 78 37) ou consultez les annonces de la station des Rousses.',
  },
  {
    q: 'Y a-t-il une billetterie pour les compétitions ?',
    a: 'Pour la majorité des épreuves, l\'entrée est gratuite. La billetterie ne concerne que les grands événements internationaux (Coupes du Monde). Vérifiez auprès de la source officielle de chaque compétition.',
  },
  {
    q: 'Peut-on s\'entraîner sur le stade en dehors des compétitions ?',
    a: 'L\'accès aux installations techniques (pas de tir, tremplins) est strictement réservé aux licenciés via leur club et aux stages encadrés par le CNSNMM. Pour le ski de fond sur le domaine, les pistes sont accessibles via le forfait nordique de l\'Espace Loisirs des Rousses.',
  },
  {
    q: 'Quelles activités l\'été ?',
    a: 'Randonnée et VTT sur les sentiers du massif jurassien (le stade lui-même est traversé par plusieurs itinéraires). Le stade héberge des compétitions de ski-roues et de saut à ski d\'été (Coupe Continentale, Coupe d\'Europe) ainsi que les stages nationaux d\'été du CNSNMM.',
  },
  {
    q: 'Comment venir depuis la Suisse ?',
    a: 'Le stade se trouve à 20 minutes à pied de la gare de La Cure (canton de Vaud), terminus de la ligne CFF NStCM depuis Nyon. C\'est l\'option la plus simple si vous arrivez en train depuis Genève ou Lausanne. En voiture, comptez moins d\'une heure depuis l\'aéroport de Genève par Nyon et le col de la Givrine.',
  },
];

function FAQItem({ q, a }) {
  const [open, setOpen] = useState(false);
  return (
    <li style={{ display: 'block', padding: 0 }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
          width: '100%', padding: '20px 0', textAlign: 'left',
          fontFamily: 'var(--cond)', fontSize: 17, fontWeight: 600, letterSpacing: 0,
          color: 'var(--text)', cursor: 'pointer',
        }}
      >
        <span>{q}</span>
        <span style={{ color: 'var(--accent)', fontFamily: 'var(--display)', fontSize: 24, transform: open ? 'rotate(45deg)' : 'none', transition: 'transform .2s' }}>+</span>
      </button>
      {open && (
        <p style={{ padding: '0 0 24px', fontFamily: 'var(--sans)', fontSize: 15, lineHeight: 1.65, color: 'var(--text-2)', fontWeight: 300, maxWidth: 720 }}>
          {a}
        </p>
      )}
    </li>
  );
}

function VenirPage() {
  return (
    <>
      <window.Disclaimer />
      <window.Nav active="venir" />

      <section className="page-hero">
        <div className="page-hero-eyebrow">Infos pratiques · Saison 2026–27</div>
        <h1 className="page-hero-title">Venir aux <em>Tuffes</em></h1>
        <p className="page-hero-deck">
          Adresse, accès, parking et hébergements pour préparer sereinement votre venue
          au stade nordique de Prémanon, dans le massif du Jura — à la frontière franco-suisse.
        </p>
      </section>

      <section className="section">
        <div className="section-head">
          <div>
            <div className="section-eyebrow">Localisation</div>
            <h2 className="section-title">Adresse <em>et accès</em></h2>
          </div>
          <div className="section-meta">Stade nordique des Tuffes<br/>Prémanon, Jura</div>
        </div>

        <div className="info-grid">
          <div className="info-card">
            <span className="lbl">Adresse</span>
            <span className="val">730–732 Route<br/>des Tremplins</span>
            <span className="desc">39220 Prémanon, France</span>
          </div>
          <div className="info-card">
            <span className="lbl">Coordonnées GPS</span>
            <span className="val">46.4510° N<br/>6.0689° E</span>
            <span className="desc">Entre Prémanon et Les Rousses, à la frontière franco-suisse.</span>
          </div>
          <div className="info-card">
            <span className="lbl">Altitude</span>
            <span className="val">~ 1 020 m</span>
            <span className="desc">Site exposé, prévoyez vêtements chauds même au printemps.</span>
          </div>
          <div className="info-card">
            <span className="lbl">Massif · Frontière</span>
            <span className="val">Jura · CH/FR</span>
            <span className="desc">Massif jurassien, frontière avec le canton de Vaud (La Cure).</span>
          </div>
        </div>

        <div style={{ marginTop: 48, position: 'relative', borderRadius: 4, overflow: 'hidden', border: '1px solid var(--line-soft)', background: 'var(--bg-2)' }}>
          <iframe
            title="Carte Google Maps · Stade nordique des Tuffes"
            src="https://maps.google.com/maps?q=Stade+Nordique+des+Tuffes,+730+Route+des+Tremplins,+39220+Pr%C3%A9manon&output=embed"
            width="100%"
            height="420"
            style={{ border: 0, display: 'block' }}
            loading="lazy"
            referrerPolicy="no-referrer-when-downgrade"
            allowFullScreen
          />
          <div style={{ padding: '12px 16px', borderTop: '1px solid var(--line-soft)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 12, fontFamily: 'var(--cond)', fontSize: 12, letterSpacing: '0.12em', textTransform: 'uppercase', color: 'var(--muted)' }}>
            <span>Calculer un itinéraire :</span>
            <a href="https://www.google.com/maps/dir/?api=1&destination=Stade+Nordique+des+Tuffes,+730+Route+des+Tremplins,+39220+Pr%C3%A9manon" target="_blank" rel="noopener" style={{ color: 'var(--accent)', borderBottom: '1px solid var(--accent)', paddingBottom: 2, fontWeight: 600 }}>Ouvrir dans Google Maps →</a>
          </div>
        </div>
      </section>

      <section className="section" style={{ paddingTop: 0 }}>
        <div className="section-head">
          <div>
            <div className="section-eyebrow">Transports</div>
            <h2 className="section-title">En <em>voiture</em></h2>
          </div>
          <div className="section-meta">Temps de trajet<br/>indicatifs</div>
        </div>

        <ul className="info-list">
          {DISTANCES.map(d => (
            <li key={d.city}>
              <span className="key">{d.city}<small>~ {d.time}</small></span>
              <span>{d.detail}</span>
            </li>
          ))}
        </ul>

        <div className="section-head" style={{ marginTop: 64 }}>
          <div>
            <div className="section-eyebrow">Train · Avion · Navettes</div>
            <h2 className="section-title">Transports <em>en commun</em></h2>
          </div>
        </div>
        <ul className="info-list">
          <li>
            <span className="key">Gare de La Cure (CH)<small>20 min à pied · option recommandée</small></span>
            <span>Côté suisse, canton de Vaud. Terminus de la ligne CFF NStCM depuis Nyon (correspondance directe avec les trains depuis Genève et Lausanne). De la gare, comptez 20 minutes de marche pour rejoindre le stade.</span>
          </li>
          <li>
            <span className="key">Aéroport de Genève (CH)<small>&lt; 50 min en voiture</small></span>
            <span>Aéroport international le plus proche. Voitures de location sur place. Bus / train possible jusqu'à Nyon, puis CFF NStCM jusqu'à La Cure.</span>
          </li>
          <li>
            <span className="key">Gares françaises<small>Morez, Saint-Claude</small></span>
            <span>Plus éloignées que La Cure. Liaison locale en bus ou taxi nécessaire pour rejoindre Prémanon.</span>
          </li>
          <li>
            <span className="key">Navettes événements<small>Coupes du monde, Championnats</small></span>
            <span>Navettes mises en place depuis Les Rousses ou les parkings relais lors des grandes compétitions. Détails communiqués sur la page de chaque événement.</span>
          </li>
        </ul>
      </section>

      <section className="section" style={{ paddingTop: 0 }}>
        <div className="section-head">
          <div>
            <div className="section-eyebrow">Stationnement</div>
            <h2 className="section-title"><em>Parking</em></h2>
          </div>
        </div>
        <ul className="info-list">
          <li>
            <span className="key">Parking principal P1</span>
            <span>Environ 100 places, situé 250 m avant l'entrée du stade sur la Route des Tremplins, en face des bâtiments techniques Sogestar. Accès libre pour l'entraînement et les compétitions régionales.</span>
          </li>
          <li>
            <span className="key">Grands événements</span>
            <span>Lors des Coupes du Monde et Championnats de France, un dispositif spécifique est mis en place : parkings relais et navettes organisées. Suivez les consignes de chaque épreuve.</span>
          </li>
        </ul>
      </section>

      <section className="section" style={{ paddingTop: 0 }}>
        <div className="section-head">
          <div>
            <div className="section-eyebrow">Où dormir</div>
            <h2 className="section-title"><em>Hébergements</em></h2>
          </div>
          <div className="section-meta">Athlètes en stage : hébergement<br/>directement au CNSNMM</div>
        </div>
        <ul className="info-list">
          {HEBERGEMENTS.map(h => (
            <li key={h.lieu}>
              <span className="key">{h.lieu}</span>
              <span>{h.detail}</span>
            </li>
          ))}
        </ul>
      </section>

      <section className="section" style={{ paddingTop: 0 }}>
        <div className="section-head">
          <div>
            <div className="section-eyebrow">Sur place</div>
            <h2 className="section-title"><em>Restauration</em></h2>
          </div>
        </div>
        <ul className="info-list">
          <li>
            <span className="key">Grands événements</span>
            <span>Food trucks, snack-bars et stands partenaires sont déployés lors des compétitions internationales et nationales.</span>
          </li>
          <li>
            <span className="key">Compétitions régionales</span>
            <span>Selon l'événement. Vérifiez le détail sur la fiche de chaque épreuve dans le calendrier.</span>
          </li>
        </ul>
      </section>

      <section className="section" style={{ paddingTop: 0 }}>
        <div className="section-head">
          <div>
            <div className="section-eyebrow">Foire aux questions</div>
            <h2 className="section-title">Questions <em>fréquentes</em></h2>
          </div>
        </div>
        <ul className="info-list" style={{ borderTop: 0 }}>
          {FAQ.map((f, i) => <FAQItem key={i} q={f.q} a={f.a} />)}
        </ul>
      </section>

      <window.Footer />
    </>
  );
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<VenirPage />);
