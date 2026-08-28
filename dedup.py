"""dedup.py - Helpers de déduplication d'événements."""
import re
import unicodedata
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)

STOPWORDS = {
    "de", "du", "des", "le", "la", "les", "un", "une", "et", "à", "au", "aux",
    "en", "sur", "pour", "par", "dans", "ski", "nordique", "stade", "tuffes",
    "prémanon", "premanon", "france", "jura", "edition", "édition",
    "competition", "compétition", "course", "open", "challenge", "tour",
    "épreuve", "epreuve",
}

MOIS_FR = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11,
    "décembre": 12, "decembre": 12,
}

RE_DATE_FR_RANGE = re.compile(
    r"(?:du\s+)?(\d{1,2})\s+(?:au\s+(\d{1,2})\s+)?"
    r"(janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre)"
    r"\s+(\d{4})",
    re.IGNORECASE,
)
RE_DATE_NUM = re.compile(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b")


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def signature_tokens(title: str) -> set:
    norm = normalize_text(title)
    tokens = norm.split()
    return {t for t in tokens if len(t) >= 3 and t not in STOPWORDS}


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def parse_french_date(text: str):
    """Returns (date_start, date_end) tuple, both YYYY-MM-DD or (None, None)."""
    if not text:
        return None, None
    text_lower = text.lower()
    m = RE_DATE_FR_RANGE.search(text_lower)
    if m:
        d1, d2, mois_name, annee = m.group(1), m.group(2), m.group(3), m.group(4)
        if mois_name not in MOIS_FR:
            return None, None
        mois = MOIS_FR[mois_name]
        date_start = f"{annee}-{mois:02d}-{int(d1):02d}"
        date_end = f"{annee}-{mois:02d}-{int(d2):02d}" if d2 else None
        return date_start, date_end
    m = RE_DATE_NUM.search(text_lower)
    if m:
        d, mois, annee = m.group(1), m.group(2), m.group(3)
        try:
            return f"{annee}-{int(mois):02d}-{int(d):02d}", None
        except ValueError:
            return None, None
    return None, None


def is_future_date(date_str: str, tolerate_today: bool = True) -> bool:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return False
    today = datetime.now().date()
    return d >= today if tolerate_today else d > today


BASE_SELECT_FIELDS = (
    "id", "title", "date_start", "date_end", "source_name", "source_url",
    "notes", "has_catering", "public_access", "sport", "additional_sources",
    "status",
)

# Champs recopiés depuis la nouvelle ligne quand l'existante ne les a pas.
BASE_FILL_FIELDS = ("notes", "has_catering", "public_access", "date_end")


# Fenetre de lecture SQL. Large a dessein : un evenement deja en base peut
# avoir commence plusieurs jours avant le candidat et courir encore (le Tour
# de Ski va du 1er au 3 janvier). On ratisse large ici, puis on filtre
# finement par chevauchement d'intervalles cote Python, ce qui conserve la
# selectivite d'origine pour les evenements sans date_end.
SEARCH_WINDOW_DAYS = 15


def _as_date(value):
    """'YYYY-MM-DD' -> date, ou None si illisible."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def event_interval(row: dict):
    """Retourne (start, end) en dates. end retombe sur start si absent ou incoherent."""
    start = _as_date(row.get("date_start"))
    if start is None:
        return None, None
    end = _as_date(row.get("date_end")) or start
    return start, max(start, end)


def intervals_overlap(a, b, slack_days: int = 1) -> bool:
    """
    Chevauchement de deux intervalles inclusifs, avec tolerance en jours.

    Le slack de 1 jour preserve le comportement historique : deux entrees
    consecutives (J et J+1) sans date_end restent considerees comme le meme
    evenement etale sur deux jours.
    """
    (a_start, a_end), (b_start, b_end) = a, b
    if a_start is None or b_start is None:
        return False
    slack = timedelta(days=slack_days)
    return a_start - slack <= b_end and b_start - slack <= a_end


def sports_conflict(a, b) -> bool:
    """
    True si les deux sports sont connus ET differents.

    Garde-fou indispensable : aux Tuffes, les Championnats de France de
    biathlon et ceux de ski de fond portent le titre exact et se suivent d'un
    jour. STOPWORDS avale "france" et "tuffes", les deux titres se reduisent au
    seul token "championnats" et la similarite vaut 1.0. Sans ce test, la
    deduplication ferait disparaitre une des deux disciplines du calendrier.
    Un sport inconnu d'un cote ne bloque pas la fusion.
    """
    a = (a or "").strip().lower()
    b = (b or "").strip().lower()
    return bool(a and b and a != b)


# Sources faisant autorite sur les donnees de calendrier. La FFS publie le
# calendrier officiel ; tout le reste (sites de clubs, agregateurs du type
# Ski-Nordique.net, saisies manuelles) en est une copie, parfois figee.
AUTHORITATIVE_SOURCES = ("FFS calendrier",)

# Fenetre de recherche d'un evenement de titre proche mais hors fusion.
# Sert uniquement a alerter sur un decalage de calendrier, jamais a fusionner.
SHIFT_ALERT_WINDOW_DAYS = 45
SHIFT_ALERT_THRESHOLD = 0.7


def source_outranks(new_source, existing_source) -> bool:
    """True si la nouvelle source fait autorite et pas l'existante."""
    return (new_source in AUTHORITATIVE_SOURCES
            and existing_source not in AUTHORITATIVE_SOURCES)


def warn_if_date_shift(sb, candidate_row: dict) -> None:
    """
    Signale qu'un evenement sur le point d'etre insere ressemble fortement a un
    evenement deja en base, situe quelques semaines plus loin.

    C'est la vraie signature d'un decalage de calendrier. On n'ecrase pas la
    date existante et on ne fusionne pas : le calendrier FFS liste chaque
    journee d'une epreuve comme une entree distincte, donc laisser une source
    imposer sa date ferait deriver un evenement de trois jours vers son dernier
    jour a chaque passage. Un decalage reel se tranche a la main.
    """
    cand = event_interval(candidate_row)
    if cand[0] is None:
        return
    candidate_tokens = signature_tokens(candidate_row.get("title", ""))
    if not candidate_tokens:
        return
    window = timedelta(days=SHIFT_ALERT_WINDOW_DAYS)
    try:
        result = sb.table("events").select(
            "id, title, date_start, date_end, sport, source_name"
        ).gte("date_start", (cand[0] - window).isoformat()) \
         .lte("date_start", (cand[1] + window).isoformat()).execute()
    except Exception:
        return
    for existing in result.data or []:
        if sports_conflict(candidate_row.get("sport"), existing.get("sport")):
            continue
        if intervals_overlap(cand, event_interval(existing)):
            continue  # deja traite par la deduplication
        if jaccard(candidate_tokens, signature_tokens(existing.get("title", ""))) < SHIFT_ALERT_THRESHOLD:
            continue
        log.warning(
            "   /!\\ decalage possible : '%s' (%s, source %s) ressemble a "
            "l'event #%s '%s' du %s. Rien n'a ete fusionne, a trancher a la main.",
            (candidate_row.get("title") or "")[:50], cand[0],
            candidate_row.get("source_name"),
            existing.get("id"), (existing.get("title") or "")[:50],
            existing.get("date_start"),
        )
        return


def find_duplicate(sb, candidate_row: dict, similarity_threshold: float = 0.5,
                   extra_fields: tuple = ()):
    cand = event_interval(candidate_row)
    if cand[0] is None:
        return None
    window = timedelta(days=SEARCH_WINDOW_DAYS)
    d_min = (cand[0] - window).isoformat()
    d_max = (cand[1] + window).isoformat()
    select_fields = ", ".join(BASE_SELECT_FIELDS + tuple(extra_fields))
    try:
        result = sb.table("events").select(
            select_fields
        ).gte("date_start", d_min).lte("date_start", d_max).execute()
    except Exception as exc:
        log.warning("dedup: erreur lecture events : %s", exc)
        return None
    if not result.data:
        return None
    candidate_tokens = signature_tokens(candidate_row.get("title", ""))
    if not candidate_tokens:
        return None
    best_match, best_score = None, 0.0
    # A score egal, on prefere une ligne publiee. Un doublon mis de cote en
    # 'pending' ne doit pas capter l'enrichissement a la place de la ligne
    # reellement affichee sur le site.
    candidates = sorted(result.data, key=lambda r: r.get("status") != "published")
    for existing in candidates:
        if sports_conflict(candidate_row.get("sport"), existing.get("sport")):
            continue
        if not intervals_overlap(cand, event_interval(existing)):
            continue
        existing_tokens = signature_tokens(existing.get("title", ""))
        score = jaccard(candidate_tokens, existing_tokens)
        if score > best_score:
            best_score = score
            best_match = existing
    return best_match if best_score >= similarity_threshold else None


def merge_into_existing(sb, existing: dict, new_row: dict,
                       fill_fields: tuple = BASE_FILL_FIELDS) -> bool:
    updates = {}
    new_source_name = new_row.get("source_name")
    new_source_url = new_row.get("source_url")
    existing_main_source = existing.get("source_name")
    existing_extras = existing.get("additional_sources") or []
    already_listed = (
        new_source_name == existing_main_source
        or any(s.get("source_name") == new_source_name for s in existing_extras)
    )
    if new_source_name and not already_listed:
        updates["additional_sources"] = existing_extras + [
            {"source_name": new_source_name, "source_url": new_source_url}
        ]
    for field in fill_fields:
        if existing.get(field) in (None, "") and new_row.get(field) not in (None, ""):
            updates[field] = new_row[field]

    # La source de reference corrige le sport sans discuter : il est derive du
    # code d'epreuve (FFS-BIATH-NA -> Biathlon), donc plus fiable qu'une
    # deduction faite sur un titre libre. Elle n'impose rien d'autre : notes,
    # organizer et level restent ceux de la fiche la plus riche, et les dates
    # ne sont jamais ecrasees (voir warn_if_date_shift).
    if source_outranks(new_source_name, existing_main_source):
        new_sport = new_row.get("sport")
        if new_sport and existing.get("sport") != new_sport:
            updates["sport"] = new_sport

    # Elargissement de la fenetre de l'evenement. On n'ecrase jamais une date
    # par une date plus etroite : on ne fait qu'etendre l'intervalle pour
    # couvrir ce que la nouvelle source apporte. Sans ca, une source qui decrit
    # le Tour de Ski par sa date de fin laisserait l'evenement affiche sur le
    # seul 3 janvier.
    cand_start, cand_end = event_interval(new_row)
    exi_start, exi_end = event_interval(existing)
    if cand_start is not None and exi_start is not None:
        if cand_start < exi_start:
            updates["date_start"] = cand_start.isoformat()
        if cand_end > exi_end:
            updates["date_end"] = cand_end.isoformat()

    if not updates:
        return False
    try:
        sb.table("events").update(updates).eq("id", existing["id"]).execute()
        log.info("   ⤴ enrichi event #%s avec %d champs depuis '%s'",
                 existing["id"], len(updates), new_source_name)
        return True
    except Exception as exc:
        log.error("   ✗ enrichissement échoué pour event #%s : %s", existing["id"], exc)
        return False


def upsert_event(sb, row: dict, extra_fields: tuple = ()) -> str:
    """Returns 'added', 'merged', 'unchanged' or 'error'.

    `extra_fields` : colonnes optionnelles (level, organizer, is_highlight…) à
    lire sur la ligne existante et à compléter si elle ne les a pas encore.
    """
    try:
        existing = find_duplicate(sb, row, extra_fields=extra_fields)
        if existing:
            changed = merge_into_existing(
                sb, existing, row, fill_fields=BASE_FILL_FIELDS + tuple(extra_fields)
            )
            return "merged" if changed else "unchanged"
        warn_if_date_shift(sb, row)
        sb.table("events").insert(row).execute()
        return "added"
    except Exception as exc:
        log.error("   ✗ upsert event ÉCHOUÉ pour '%s' : %s",
                  row.get("title", "?")[:60], exc)
        return "error"
