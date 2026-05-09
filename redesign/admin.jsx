/* global React, ReactDOM */
// Admin page — Supabase magic-link auth + event moderation.
// Roles enforced server-side via RLS (see migration_auth.sql).

const { useState, useEffect, useMemo, useCallback } = React;

const SPORTS = ['Ski de fond', 'Biathlon', 'Combiné nordique', 'Saut à ski', 'Para', 'Autre / Plusieurs'];

const sb = window.supabase.createClient(
  window.STT_CONFIG.SUPABASE_URL,
  window.STT_CONFIG.SUPABASE_ANON_KEY,
);

const MONTHS_LONG = ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre'];
function fmtDate(s) {
  if (!s) return '—';
  const d = new Date(s + (s.length === 10 ? 'T12:00:00' : ''));
  if (isNaN(d)) return s;
  return `${d.getDate()} ${MONTHS_LONG[d.getMonth()]} ${d.getFullYear()}`;
}
function fmtRange(ev) {
  if (!ev.date_end || ev.date_end === ev.date_start) return fmtDate(ev.date_start);
  const a = fmtDate(ev.date_start), b = fmtDate(ev.date_end);
  return `${a} → ${b}`;
}

function LoginPanel() {
  const [email, setEmail] = useState('');
  const [code, setCode] = useState('');
  const [state, setState] = useState('idle'); // idle | sending | sent | verifying | error
  const [error, setError] = useState('');

  async function sendLink(e) {
    e.preventDefault();
    if (!email.trim()) return;
    setState('sending');
    setError('');
    const { error: err } = await sb.auth.signInWithOtp({
      email: email.trim(),
      options: { emailRedirectTo: window.location.href.split('#')[0] },
    });
    if (err) {
      setError(err.message);
      setState('error');
    } else {
      setState('sent');
    }
  }

  async function verifyCode(e) {
    e.preventDefault();
    if (!email.trim() || !code.trim()) return;
    setState('verifying');
    setError('');
    const { error: err } = await sb.auth.verifyOtp({
      email: email.trim(),
      token: code.trim(),
      type: 'email',
    });
    if (err) {
      setError(err.message);
      setState('sent'); // back to sent state so they can retry
    }
    // success: onAuthStateChange in AdminApp picks up the new session
  }

  return (
    <div className="admin-login">
      <div className="admin-login-card">
        <div className="page-hero-eyebrow">Espace privé · Stade des Tuffes</div>
        <h1 className="admin-login-title">Connexion <em>admin</em></h1>
        <p style={{ fontFamily: 'var(--sans)', fontSize: 14, color: 'var(--text-2)', marginBottom: 24, fontWeight: 300 }}>
          Entrez votre email. Vous recevrez un mail contenant un lien magique <strong>et</strong> un code à 6 chiffres — utilisez l'un ou l'autre.
        </p>

        {state === 'sent' || state === 'verifying' ? (
          <>
            <div className="admin-login-success">
              <strong>Email envoyé !</strong> Sur <em>{email}</em>. Vérifiez aussi vos spams.
            </div>

            <form onSubmit={verifyCode} className="admin-login-form" style={{ marginTop: 20 }}>
              <label style={{ fontFamily: 'var(--cond)', fontSize: 11, fontWeight: 600, letterSpacing: '0.18em', textTransform: 'uppercase', color: 'var(--muted)' }}>
                Code à 6 chiffres
              </label>
              <input type="text" inputMode="numeric" pattern="[0-9]*" maxLength="6"
                value={code} onChange={e => setCode(e.target.value.replace(/\D/g, ''))}
                placeholder="123456" autoFocus
                style={{ letterSpacing: '0.3em', fontSize: 18, textAlign: 'center', fontFamily: 'var(--mono)' }} />
              <button type="submit" className="sponsor-cta" disabled={state === 'verifying' || code.length < 6}>
                {state === 'verifying' ? 'Vérification…' : 'Valider le code →'}
              </button>
              {error && <div className="fld-error">{error}</div>}
            </form>

            <p style={{ marginTop: 16, fontFamily: 'var(--sans)', fontSize: 12.5, color: 'var(--muted)', fontWeight: 300 }}>
              Le lien magique fonctionne aussi : clique-le directement dans le mail. Le code reste valable ~1h.
            </p>
          </>
        ) : (
          <form onSubmit={sendLink} className="admin-login-form">
            <input type="email" value={email} onChange={e => setEmail(e.target.value)}
              placeholder="votre@email.fr" required autoFocus />
            <button type="submit" className="sponsor-cta" disabled={state === 'sending'}>
              {state === 'sending' ? 'Envoi…' : 'Recevoir le lien →'}
            </button>
            {error && <div className="fld-error">{error}</div>}
          </form>
        )}

        <p style={{ marginTop: 24, fontFamily: 'var(--cond)', fontSize: 11, letterSpacing: '0.12em', textTransform: 'uppercase', color: 'var(--muted)' }}>
          <a href="index.html" style={{ borderBottom: '1px solid var(--line)' }}>← Retour au site</a>
        </p>
      </div>
    </div>
  );
}

function EventCard({ ev, role, onPublish, onUnpublish, onDelete }) {
  const isPending = ev.status === 'pending';
  return (
    <div className={'admin-card admin-card-' + ev.status}>
      <div className="admin-card-head">
        <div className="admin-card-status">
          {isPending ? <span className="badge-pending">● En attente</span> : <span className="badge-published">✓ Publié</span>}
        </div>
        <div className="admin-card-source">
          {ev.source_name || '—'}
          {ev.source_url && <> · <a href={ev.source_url} target="_blank" rel="noopener">source ↗</a></>}
        </div>
      </div>
      <h3 className="admin-card-title">{ev.title}</h3>
      <div className="admin-card-meta">
        <span><strong>Discipline :</strong> {ev.sport || '—'}</span>
        <span><strong>Dates :</strong> {fmtRange(ev)}</span>
        <span><strong>Public :</strong> {ev.public_access === true ? 'Oui' : ev.public_access === false ? 'Non' : '?'}</span>
        <span><strong>Restauration :</strong> {ev.has_catering === true ? 'Oui' : ev.has_catering === false ? 'Non' : '?'}</span>
      </div>
      {ev.notes && <div className="admin-card-notes">{ev.notes}</div>}
      <div className="admin-card-actions">
        {isPending ? (
          <button className="btn-publish" onClick={() => onPublish(ev.id)}>✓ Publier</button>
        ) : (
          <button className="btn-unpublish" onClick={() => onUnpublish(ev.id)}>↩ Dépublier</button>
        )}
        {role === 'admin' && (
          <button className="btn-reject" onClick={() => onDelete(ev.id, ev.title)}>🗑 Supprimer</button>
        )}
      </div>
    </div>
  );
}

function NewEventForm({ onCreated }) {
  const [open, setOpen] = useState(false);
  const [form, setForm] = useState({
    title: '', sport: '', date_start: '', date_end: '',
    public_access: '', has_catering: '', source_url: '', notes: '',
  });
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState('');

  const set = (k) => (e) => setForm(f => ({ ...f, [k]: e.target.value }));

  async function submit(e) {
    e.preventDefault();
    if (!form.title || !form.sport || !form.date_start) {
      setErr('Nom, discipline et date de début requis.');
      return;
    }
    setBusy(true);
    setErr('');
    const payload = {
      title: form.title.trim(),
      sport: form.sport,
      date_start: form.date_start,
      date_end: form.date_end || null,
      public_access: form.public_access === 'true' ? true : form.public_access === 'false' ? false : null,
      has_catering: form.has_catering === 'true' ? true : form.has_catering === 'false' ? false : null,
      source_url: form.source_url.trim() || null,
      notes: form.notes.trim() || null,
      source_name: 'Direct (admin)',
      status: 'published',
    };
    const { error } = await sb.from('events').insert([payload]);
    setBusy(false);
    if (error) { setErr(error.message); return; }
    setForm({ title: '', sport: '', date_start: '', date_end: '', public_access: '', has_catering: '', source_url: '', notes: '' });
    setOpen(false);
    onCreated();
  }

  if (!open) {
    return (
      <button className="sponsor-cta" onClick={() => setOpen(true)} style={{ marginBottom: 32 }}>
        + Publier un nouvel événement
      </button>
    );
  }

  return (
    <form onSubmit={submit} className="admin-new-form">
      <div className="fld">
        <label>Nom de l'événement *</label>
        <input type="text" value={form.title} onChange={set('title')} required />
      </div>
      <div className="fld-row">
        <div className="fld">
          <label>Discipline *</label>
          <select value={form.sport} onChange={set('sport')} required>
            <option value="">—</option>
            {SPORTS.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
        <div className="fld">
          <label>Date début *</label>
          <input type="date" value={form.date_start} onChange={set('date_start')} required />
        </div>
        <div className="fld">
          <label>Date fin</label>
          <input type="date" value={form.date_end} onChange={set('date_end')} />
        </div>
      </div>
      <div className="fld-row">
        <div className="fld">
          <label>Public</label>
          <select value={form.public_access} onChange={set('public_access')}>
            <option value="">— Inconnu —</option><option value="true">Oui</option><option value="false">Non</option>
          </select>
        </div>
        <div className="fld">
          <label>Restauration</label>
          <select value={form.has_catering} onChange={set('has_catering')}>
            <option value="">— Inconnu —</option><option value="true">Oui</option><option value="false">Non</option>
          </select>
        </div>
      </div>
      <div className="fld">
        <label>Lien source (URL officielle)</label>
        <input type="url" value={form.source_url} onChange={set('source_url')} />
      </div>
      <div className="fld">
        <label>Notes</label>
        <textarea rows="2" value={form.notes} onChange={set('notes')} />
      </div>
      {err && <div className="fld-error">{err}</div>}
      <div className="fld-actions">
        <button type="button" className="filter-btn" onClick={() => setOpen(false)}>Annuler</button>
        <button type="submit" className="sponsor-cta" disabled={busy}>{busy ? 'Envoi…' : 'Publier →'}</button>
      </div>
    </form>
  );
}

function AdminPanel({ session }) {
  const [role, setRole] = useState(null);
  const [events, setEvents] = useState([]);
  const [tab, setTab] = useState('pending');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const load = useCallback(async () => {
    setLoading(true);
    const { data: profile, error: profErr } = await sb.from('profiles').select('role').eq('id', session.user.id).single();
    if (profErr) { setError('Profil introuvable. As-tu lancé la migration SQL ?'); setLoading(false); return; }
    setRole(profile.role);

    if (profile.role === 'guest') {
      setLoading(false);
      return;
    }

    const { data: evs, error: evErr } = await sb.from('events')
      .select('*')
      .order('date_start', { ascending: false });
    if (evErr) { setError(evErr.message); setLoading(false); return; }
    setEvents(evs || []);
    setLoading(false);
  }, [session.user.id]);

  useEffect(() => { load(); }, [load]);

  async function onPublish(id) {
    await sb.from('events').update({ status: 'published' }).eq('id', id);
    load();
  }
  async function onUnpublish(id) {
    await sb.from('events').update({ status: 'pending' }).eq('id', id);
    load();
  }
  async function onDelete(id, title) {
    if (!window.confirm(`Supprimer définitivement « ${title} » ?`)) return;
    await sb.from('events').delete().eq('id', id);
    load();
  }
  async function logout() {
    await sb.auth.signOut();
    window.location.reload();
  }

  const pending = useMemo(() => events.filter(e => e.status === 'pending'), [events]);
  const published = useMemo(() => events.filter(e => e.status === 'published'), [events]);
  const visible = tab === 'pending' ? pending : published;

  if (loading) {
    return <div className="admin-loading">Chargement…</div>;
  }

  if (role === 'guest') {
    return (
      <div className="admin-wrap">
        <div className="admin-topbar">
          <div className="admin-brand">Les Tuffes · Admin</div>
          <div className="admin-user">
            <span>{session.user.email}</span>
            <button className="filter-btn" onClick={logout}>Déconnexion</button>
          </div>
        </div>
        <div style={{ padding: 64, maxWidth: 600, margin: '0 auto', textAlign: 'center' }}>
          <div className="page-hero-eyebrow">Compte en attente</div>
          <h2 className="section-title" style={{ fontSize: 48, marginBottom: 16 }}>En attente <em>d'approbation</em></h2>
          <p style={{ fontFamily: 'var(--sans)', color: 'var(--text-2)', fontSize: 16, lineHeight: 1.6 }}>
            Votre compte est créé mais n'a pas encore les droits pour modérer.
            Contactez Guillaume (cinqcibles@gmail.com) pour qu'il vous attribue le rôle approprié.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="admin-wrap">
      <div className="admin-topbar">
        <div className="admin-brand">Les Tuffes · Admin <span className="admin-role">{role}</span></div>
        <div className="admin-user">
          <span>{session.user.email}</span>
          <button className="filter-btn" onClick={logout}>Déconnexion</button>
        </div>
      </div>

      <div style={{ padding: '40px 32px', maxWidth: 1100, margin: '0 auto' }}>
        <NewEventForm onCreated={load} />

        <div className="admin-tabs">
          <button className={'admin-tab' + (tab === 'pending' ? ' active' : '')} onClick={() => setTab('pending')}>
            En attente <span className="admin-tab-count">{pending.length}</span>
          </button>
          <button className={'admin-tab' + (tab === 'published' ? ' active' : '')} onClick={() => setTab('published')}>
            Publiés <span className="admin-tab-count">{published.length}</span>
          </button>
        </div>

        {error && <div className="fld-error">{error}</div>}

        {visible.length === 0 ? (
          <div style={{ padding: 60, textAlign: 'center', fontFamily: 'var(--sans)', color: 'var(--muted)', fontStyle: 'italic' }}>
            Aucun événement {tab === 'pending' ? 'en attente' : 'publié'}.
          </div>
        ) : (
          <div className="admin-cards">
            {visible.map(ev => (
              <EventCard key={ev.id} ev={ev} role={role}
                onPublish={onPublish} onUnpublish={onUnpublish} onDelete={onDelete} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function AdminApp() {
  const [session, setSession] = useState(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    sb.auth.getSession().then(({ data }) => {
      setSession(data.session || null);
      setReady(true);
    });
    const { data: sub } = sb.auth.onAuthStateChange((_event, sess) => {
      setSession(sess || null);
    });
    return () => sub.subscription.unsubscribe();
  }, []);

  if (!ready) return <div className="admin-loading">Chargement…</div>;
  if (!session) return <LoginPanel />;
  return <AdminPanel session={session} />;
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<AdminApp />);
