/* global React, ReactDOM */
// Generic contact modal — opens via:
//   window.dispatchEvent(new CustomEvent('open-contact', { detail: { subject: '...' } }))
// or:
//   window.openContact('Mon sujet')
//
// Sends through FormSubmit (no email client required on the visitor's side).
// Mounted as its own React root, available on every page that loads contact.js.

const { useState, useEffect } = React;

function ContactModal() {
  const [open, setOpen] = useState(false);
  const [subject, setSubject] = useState('');
  const [form, setForm] = useState({ name: '', email: '', message: '', honeypot: '' });
  const [state, setState] = useState('idle'); // idle | sending | success | error
  const [error, setError] = useState('');

  useEffect(() => {
    const handler = (e) => {
      setSubject(e.detail?.subject || 'Contact');
      setForm({ name: '', email: '', message: '', honeypot: '' });
      setState('idle');
      setError('');
      setOpen(true);
    };
    window.addEventListener('open-contact', handler);
    return () => window.removeEventListener('open-contact', handler);
  }, []);

  const set = (k) => (e) => setForm(f => ({ ...f, [k]: e.target.value }));

  async function submit(e) {
    e.preventDefault();
    if (form.honeypot) return;
    if (!form.email.trim() || !form.message.trim()) {
      setError('Email et message sont obligatoires.');
      return;
    }
    setState('sending');
    setError('');

    const cfg = window.STT_CONFIG;
    try {
      const res = await fetch(`https://formsubmit.co/ajax/${cfg.FORMSUBMIT_EMAIL}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({
          _subject: `[Tuffes] ${subject} — ${form.name || form.email}`,
          _template: 'box',
          _replyto: form.email.trim(),
          Sujet: subject,
          Nom: form.name.trim() || '—',
          Email: form.email.trim(),
          Message: form.message.trim(),
        }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setState('success');
    } catch (err) {
      setState('error');
      setError(err.message || 'Envoi impossible.');
    }
  }

  if (!open) return null;

  return (
    <div className="overlay open" onClick={() => setOpen(false)} role="dialog" aria-modal="true" aria-label="Nous écrire">
      <div className="modal submit-modal" onClick={e => e.stopPropagation()}>
        <button type="button" className="modal-close submit-close" onClick={() => setOpen(false)} aria-label="Fermer">×</button>
        <div className="submit-body">
          {state === 'success' ? (
            <>
              <div className="modal-eyebrow">Merci !</div>
              <h3 className="modal-title">Message <em>envoyé</em></h3>
              <p style={{ fontFamily: 'var(--sans)', fontSize: 14.5, color: 'var(--text-2)', marginBottom: 24, fontWeight: 300, lineHeight: 1.6 }}>
                Votre message est bien parti. Vous recevrez une réponse à <strong>{form.email}</strong> sous 48 h.
              </p>
              <button className="sponsor-cta" onClick={() => setOpen(false)} style={{ alignSelf: 'flex-start' }}>Fermer</button>
            </>
          ) : (
            <form onSubmit={submit} noValidate>
              <div className="modal-eyebrow">{subject}</div>
              <h3 className="modal-title">Nous <em>écrire</em></h3>
              <p style={{ fontFamily: 'var(--sans)', fontSize: 14, color: 'var(--muted)', marginBottom: 20 }}>
                Message envoyé directement à l'équipe du site. Réponse à votre adresse e-mail.
              </p>

              <input type="text" tabIndex="-1" autoComplete="off" aria-hidden="true"
                value={form.honeypot} onChange={set('honeypot')}
                style={{ position: 'absolute', left: -9999, width: 1, height: 1, opacity: 0 }} />

              <div className="fld">
                <label>Votre nom</label>
                <input type="text" value={form.name} onChange={set('name')} placeholder="Optionnel" />
              </div>

              <div className="fld">
                <label>Votre email <span className="req">*</span></label>
                <input type="email" value={form.email} onChange={set('email')} placeholder="votre@email.fr" required />
                <span className="fld-hint">Pour qu'on puisse vous répondre.</span>
              </div>

              <div className="fld">
                <label>Message <span className="req">*</span></label>
                <textarea rows="5" value={form.message} onChange={set('message')} placeholder="Votre message…" required />
              </div>

              {error && <div className="fld-error">{error}</div>}

              <div className="fld-actions">
                <button type="button" className="filter-btn" onClick={() => setOpen(false)}>Annuler</button>
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

// Expose the global trigger BEFORE mounting so early clicks can't TypeError.
window.openContact = (subj) => window.dispatchEvent(new CustomEvent('open-contact', { detail: { subject: subj } }));

// Mount on its own root so it works on every page that loads this script.
const contactRoot = document.createElement('div');
contactRoot.id = 'contact-root';
document.body.appendChild(contactRoot);
ReactDOM.createRoot(contactRoot).render(<ContactModal />);
