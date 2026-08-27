// Événements confirmés de la saison 2026-2027 aux Tuffes.
//
// Ces épreuves sont vérifiées à la main auprès des sources officielles
// (voir `source_url`). Elles sont également poussées dans Supabase par
// `scrape_events.py` (liste CONFIRMED_EVENTS) : le seed ci-dessous n'est là que
// pour que le calendrier reste juste même si le scraper n'a pas encore tourné
// ou si Supabase est injoignable. data.js fusionne les deux sans doublon.
//
// Format identique aux lignes de la table `events` : les champs supplémentaires
// (level, organizer, is_highlight) sont facultatifs côté Supabase.

window.STT_SEED_EVENTS = [
  {
    id: 'seed-sprint-classique-tuffes-2026',
    title: 'Sprint Classique Les Tuffes',
    sport: 'Ski de fond',
    date_start: '2026-10-24',
    date_end: null,
    level: 'Régional',
    organizer: 'Haut-Jura Ski',
    public_access: true,
    notes: "Sprint classique ouvert des U11 aux Seniors, annoncé au calendrier de saison de Haut-Jura Ski. Format et modalités communiqués ultérieurement par le club.",
    source_name: 'Haut-Jura Ski — saison 2026/2027',
    source_url: 'https://www.hautjuraski.fr/evenements',
  },
  {
    id: 'seed-cyclo-haut-jura-2026',
    title: 'Cyclo Haut-Jura',
    sport: 'Cyclisme',
    date_start: '2026-07-05',
    date_end: null,
    level: 'Régional',
    organizer: 'Jura Ski Events',
    public_access: true,
    has_catering: true,
    notes: "Départs et arrivées depuis l'esplanade du stade des Tuffes, sur des parcours de 65 km (940 m D+) et 100 km (1620 m D+).",
    source_name: 'Vélo-Cyclosport — agenda cyclosportives',
    source_url: 'https://www.velo-cyclosport.com/agenda/index.php?month=7',
  },
  {
    id: 'seed-samse-biathlon-etape-2-2026',
    title: 'SAMSE National Tour Biathlon — Étape 2',
    sport: 'Biathlon',
    date_start: '2026-12-19',
    date_end: '2026-12-20',
    level: 'National',
    organizer: 'FFS & Ski Club du Grandvaux',
    public_access: true,
    has_catering: true,
    notes: "Première étape sur neige de la saison pour les U17, avec également les U19, U21 et Seniors. Épreuves : individuel, sprint et mass-start.",
    source_name: 'Ski-Nordique.net — calendrier national FFS',
    source_url: 'https://www.ski-nordique.net/biathlon-decouvrez-le-calendrier-des-epreuves-nationales-pour-la-saison-2026-2027.6749329-72348.html',
  },
  {
    id: 'seed-tour-de-ski-2027',
    title: 'FIS Tour de Ski 2027 — Coupe du monde',
    sport: 'Ski de fond',
    date_start: '2027-01-01',
    date_end: '2027-01-03',
    level: 'International',
    organizer: 'FIS & Jura Ski Events',
    is_highlight: true,
    public_access: true,
    has_catering: true,
    notes: "Étape inaugurale du Tour de Ski FIS en France, sur trois jours : sprint classique 1,3 km le 1er janvier, mass-start classique 20 km le 2, poursuite libre 15 km le 3 — femmes et hommes. Village partenaires, navettes et pack VIP.",
    source_name: 'World Cup Station des Rousses',
    source_url: 'https://www.worldcupstationdesrousses.fr/tour-de-ski-2027-les-rousses/',
  },
  {
    id: 'seed-samse-biathlon-etape-6-2027',
    title: 'SAMSE National Tour Biathlon — Étape 6',
    sport: 'Biathlon',
    date_start: '2027-02-27',
    date_end: '2027-02-28',
    level: 'National',
    organizer: 'FFS & ESSS Montbenoît',
    public_access: true,
    has_catering: true,
    notes: "Sixième étape du circuit national (U19 à Seniors), avec la participation exceptionnelle de 70 biathlètes suisses. Épreuves : sprint, individuel et mass-start.",
    source_name: 'Ski-Nordique.net — calendrier national FFS',
    source_url: 'https://www.ski-nordique.net/biathlon-decouvrez-le-calendrier-des-epreuves-nationales-pour-la-saison-2026-2027.6749329-72348.html',
  },
  {
    // « Janvier 2027 » et départ aux Tuffes sont confirmés, le jour ne l'est pas :
    // date_tbd affiche « date à confirmer » au lieu d'un jour inventé, et
    // date_start pointe la fin du mois pour que l'épreuve reste « à venir »
    // pendant tout janvier.
    id: 'seed-transju-jeunes-2027',
    title: "La Transju'Jeunes",
    sport: 'Ski de fond',
    date_start: '2027-01-31',
    date_end: null,
    date_tbd: true,
    level: 'Régional',
    organizer: "La Transju' / Trans'Organisation",
    public_access: true,
    notes: "Course jeunes au départ du stade nordique des Tuffes, environ 2 000 participants annoncés. Mois et lieu confirmés, jour exact encore non publié par l'organisation.",
    source_name: "La Transju' — Transju'Jeunes",
    source_url: 'https://www.latransju.com/la-transjeunes/',
  },
];
