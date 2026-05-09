/* global React, ReactDOM */

function AProposPage() {
  return (
    <>
      <window.Disclaimer />
      <window.Nav active="apropos" />

      <section className="page-hero">
        <div className="page-hero-eyebrow">À propos du site</div>
        <h1 className="page-hero-title">Un site <em>pour les fans</em></h1>
        <p className="page-hero-deck">
          stadedestuffes.fr est un site éditorial indépendant qui rassemble en un seul
          endroit le calendrier des compétitions et la veille presse autour du stade
          nordique de Prémanon. Sans lien officiel avec le CNSNMM ou l'ENSM.
        </p>
      </section>

      <article className="longform">
        <h2>Pourquoi ce site</h2>
        <p>
          Le stade nordique des Tuffes est l'un des hauts lieux du ski nordique français :
          centre d'entraînement national, base d'accueil des équipes de France, théâtre
          de Coupes du Monde et de Championnats. Pourtant, il n'existait pas de calendrier
          centralisé, accessible aux supporters, présentant l'ensemble des épreuves de
          la saison toutes disciplines confondues.
        </p>
        <p>
          Ce site comble ce manque. Il agrège automatiquement les informations publiques
          publiées par les fédérations (FFS, FIS, IBU), les clubs et la presse spécialisée,
          puis les présente de façon lisible. L'objectif : permettre à toute personne
          curieuse — supporter, parent d'athlète, simple promeneur — de savoir ce qui
          se passe aux Tuffes en un coup d'œil.
        </p>

        <h2>Ce qu'il fait — et ce qu'il ne fait pas</h2>
        <p>
          <strong>Le site fait :</strong> compilation et mise en forme des dates connues,
          regroupement des articles de presse mentionnant le stade, infos pratiques pour
          venir sur place.
        </p>
        <p>
          <strong>Le site ne fait pas :</strong> billetterie, réservation, contact direct
          avec les organisateurs, source officielle. <strong>Avant tout déplacement,
          vérifiez auprès de l'organisateur de l'épreuve.</strong>
        </p>

        <h2>Comment fonctionne le calendrier</h2>
        <p>
          Les événements sont collectés via plusieurs scrapers automatiques qui
          interrogent quotidiennement le calendrier FFS, le calendrier FIS, et les sites
          des clubs jurassiens. Chaque entrée est dédupliquée et associée à sa source.
          La fiche d'une compétition affiche toujours le lien vers cette source — c'est
          elle qui fait foi.
        </p>
        <p>
          Si vous repérez une erreur, une compétition manquante ou une donnée obsolète :
          écrivez à <a href="mailto:cinqcibles@gmail.com">cinqcibles@gmail.com</a>. Les
          corrections sont apportées rapidement.
        </p>

        <h2>Cinq Cibles, partenaire de la saison</h2>
        <p>
          Le site est soutenu par <strong>Cinq Cibles</strong>, marque française dédiée
          aux fans de biathlon. Vêtements, accessoires et objets pensés pour celles et
          ceux qui vivent ce sport intensément. Une partie des bénéfices finance
          directement les athlètes français.
        </p>

        <h2>Contribuer</h2>
        <p>
          Vous organisez une compétition aux Tuffes ? Vous avez repéré une information
          incomplète ? Vous voulez devenir partenaire ? Toutes les contributions sont
          les bienvenues — écrivez à <a href="mailto:cinqcibles@gmail.com">cinqcibles@gmail.com</a>.
        </p>

        <h2>Le stade en quelques chiffres</h2>
        <p>
          Construit en 1969 et rénové à plusieurs reprises, le stade des Tuffes est la
          propriété du <strong>Centre National de Ski Nordique et de Moyenne Montagne
          (CNSNMM)</strong>, antenne de l'École Nationale des Sports de Montagne (ENSM).
          Il accueille des compétitions de ski de fond, biathlon, combiné nordique et
          saut à ski, et a notamment hébergé une étape de Coupe du Monde de biathlon en
          janvier 2024.
        </p>
      </article>

      <window.Footer />
    </>
  );
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<AProposPage />);
