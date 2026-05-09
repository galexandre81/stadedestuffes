/* global React */
// Modal "Annoncer un événement" — public form, anyone can submit.
// Inserts into Supabase events table with status='pending'
// (RLS enforces this), then sends a notification email via FormSubmit.

const SPORT_OPTIONS = ['Ski de fond', 'Biathlon', 'Combiné nordique', 'Saut à ski', 'Para', 'Autre / Plusieurs'];

function radioToBool(v) {
  if (v === 'true') return true;
  if (v === 'false') return false;
  return null;
}

function SubmitEventModal({ open, onClose }) {
  const [form, setForm] = React.useState({
    title: '', date_start: '', date_end: '', sport: '',
    public_access: '', has_catering: '', source_url: '', email: '', notes: '',
    honeypot: '',
  });
  const [state, setState] = React.useState('idle'); // idle | sending | success | error
  const [error, setError] = React.useState('');

  React.useEffect(() => {
    if (!open) {
      setForm({
        title: '', date_start: '', date_end: '', sport: '',
        public_access: '', has_catering: '', source_url: '', email: '', notes: '',
        honeypot: '',
      });
      setState('idle');
      setError('');
    }
  }, [open]);

  if (!open) return null;

  const set = (k) => (e) => setForm(f => ({ ...f, [k]: e.target.value }));

  async function submit(e) {
    e.preventDefault();
    if (form.honeypot) return;
    if (!form.title.trim() || !form.sport || !form.date_start) {
      setError('Merci de remplir le nom, la discipline et la date de début.');
      return;
    }
    setState('sending');
    setError('');

    const cfg = window.STT_CONFIG;
    const payload = {
      title:         form.title.trim().slice(0, 255),
      sport:         form.sport,
      date_start:    form.date_start,
      date_end:      form.date_end || null,
      public_access: radioToBool(form.public_access),
      has_catering:  radioToBool(form.has_catering),
      source_url:    form.source_url.trim() || null,
      notes:         form.notes.trim() || null,
      source_name:   'Communauté',
      status:        'pending',
    };

    try {
      const supRes = await fetch(`${cfg.SUPABASE_URL}/rest/v1/events`, {
        method: 'POST',
        headers: {
          apikey: cfg.SUPABASE_ANON_KEY,
          Authorization: `Bearer ${cfg.SUPABASE_ANON_KEY}`,
          'Content-Type': 'application/json',
          Prefer: 'return=minimal',
        },
        body: JSON.stringify(payload),
      });
      if (!supRes.ok) throw new Error(`Supabase HTTP ${supRes.status}`);

      // Email de notif (best-effort, n'empêche pas le succès si ça échoue)
      fetch(`https://formsubmit.co/ajax/${cfg.FORMSUBMIT_EMAIL}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({
          _subject: `🎿 Nouvel événement à valider — ${payload.title}`,
          _template: 'table',
          'Événement':     payload.title,
          'Discipline':    payload.sport,
          'Date début':    payload.date_start,
          'Date fin':      payload.date_end || '—',
          'Public':        payload.public_access === true ? 'Oui' : payload.public_access === false ? 'Non' : 'Inconnu',
          'Restauration':  payload.has_catering === true ? 'Oui' : payload.has_catering === false ? 'Non' : 'Inconnu',
          'Source':        payload.source_url || '—',
          'Notes':         payload.notes || '—',
          'Email contact': form.email || '—',
        }),
      }).catch(() => {});

      setState('success');
    } catch (err) {
      setState('error');
      setError(err.message || 'Erreur inconnue');
    }
  }

  return (
    <div className="overlay open" onClick={onClose}>
      <div className="modal submit-modal" onClick={e => e.stopPropagation()}>
        <button className="modal-close submit-close" onClick={onClose} aria-label="Fermer">×</button>

        <div className="submit-body">
          {state === 'success' ? (
            <div className="submit-success">
              <div className="modal-eyebrow">Merci !</div>
              <h3 className="modal-title">Proposition <em>envoyée</em></h3>
              <p>Votre événement a été ajouté à la file de validation. Il sera examiné rapidement avant publication.</p>
              <button className="sponsor-cta" onClick={onClose} style={{ marginTop: 24, alignSelf: 'flex-start' }}>Fermer</button>
            </div>
          ) : (
            <form onSubmit={submit} noValidate>
              <div className="modal-eyebrow">Contribution communautaire</div>
              <h3 className="modal-title">Annoncer un <em>événement</em></h3>
              <p style={{ fontFamily: 'var(--sans)', fontSize: 14, color: 'var(--muted)', marginBottom: 24 }}>
                Les informations seront examinées par un modérateur avant publication sur le calendrier public.
              </p>

              <input type="text" tabIndex="-1" autoComplete="off" aria-hidden="true"
                value={form.honeypot} onChange={set('honeypot')}
                style={{ position: 'absolute', left: -9999, width: 1, height: 1, opacity: 0 }} />

              <div className="fld">
                <label>Nom de l'événement <span className="req">*</span></label>
                <input type="text" value={form.title} onChange={set('title')} placeholder="ex. Coupe de France FFS — Sprint" required />
              </div>

              <div className="fld-row">
                <div className="fld">
                  <label>Date de début <span className="req">*</span></label>
                  <input type="date" value={form.date_start} onChange={set('date_start')} required />
                </div>
                <div className="fld">
                  <label>Date de fin</label>
                  <input type="date" value={form.date_end} onChange={set('date_end')} />
                </div>
              </div>

              <div className="fld">
                <label>Discipline <span className="req">*</span></label>
                <select value={form.sport} onChange={set('sport')} required>
                  <option value="">— Sélectionner —</option>
                  {SPORT_OPTIONS.map(s => <option key={s} value={s}>{s}</option>)}
                </select>
              </div>

              <div className="fld-row">
                <div className="fld">
                  <label>Ouverture au public</label>
                  <select value={form.public_access} onChange={set('public_access')}>
                    <option value="">— Inconnu —</option>
                    <option value="true">Oui, public admis</option>
                    <option value="false">Non, accès restreint</option>
                  </select>
                </div>
                <div className="fld">
                  <label>Restauration sur place</label>
                  <select value={form.has_catering} onChange={set('has_catering')}>
                    <option value="">— Inconnu —</option>
                    <option value="true">Oui</option>
                    <option value="false">Non</option>
                  </select>
                </div>
              </div>

              <div className="fld">
                <label>Lien web (source officielle)</label>
                <input type="url" value={form.source_url} onChange={set('source_url')} placeholder="https://…" />
              </div>

              <div className="fld">
                <label>Votre adresse e-mail</label>
                <input type="email" value={form.email} onChange={set('email')} placeholder="votre@email.fr" />
                <span className="fld-hint">Pour qu'on vous recontacte si besoin. Ne sera jamais visible sur le site.</span>
              </div>

              <div className="fld">
                <label>Autres informations</label>
                <textarea rows="3" value={form.notes} onChange={set('notes')} placeholder="Horaires, inscriptions, remarques…" />
              </div>

              {error && <div className="fld-error">{error}</div>}

              <div className="fld-actions">
                <button type="button" className="filter-btn" onClick={onClose}>Annuler</button>
                <button type="submit" className="sponsor-cta" disabled={state === 'sending'}>
                  {state === 'sending' ? 'Envoi…' : 'Envoyer →'}
                </button>
              </div>
            </form>
          )}
        </div>
      </div>
    </div>
  );
}

window.SubmitEventModal = SubmitEventModal;
