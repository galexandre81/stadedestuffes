/* global React, ReactDOM */

function MentionsPage() {
  return (
    <>
      <window.Disclaimer />
      <window.Nav active={null} />

      <section className="page-hero">
        <div className="page-hero-eyebrow">Informations légales</div>
        <h1 className="page-hero-title">Mentions <em>légales</em></h1>
        <p className="page-hero-deck">
          Site éditorial indépendant, sans lien officiel avec le CNSNMM ou l'ENSM.
          Ces mentions précisent l'éditeur, l'hébergeur et le traitement des données.
        </p>
      </section>

      <article className="longform">
        <h2>Éditeur</h2>
        <p>
          Ce site est édité à titre personnel par <strong>Guillaume Alexandre</strong>,
          dans le cadre d'une initiative indépendante et bénévole. Il n'a aucun lien
          officiel avec le Centre National de Ski Nordique et de Moyenne Montagne
          (CNSNMM), l'École Nationale des Sports de Montagne (ENSM), ni avec les
          fédérations ou collectivités locales.
        </p>
        <p>
          Contact : <a href="mailto:cinqcibles@gmail.com">cinqcibles@gmail.com</a>
        </p>

        <h2>Hébergement</h2>
        <p>
          Le site est hébergé par <strong>GitHub Pages</strong>, service de Microsoft Corporation,
          88 Colin P. Kelly Jr. Street, San Francisco, CA 94107, États-Unis.
        </p>

        <h2>Nom de domaine</h2>
        <p>
          Le domaine <strong>stadedestuffes.fr</strong> est enregistré auprès de
          <strong> OVH SAS</strong>, 2 rue Kellermann, 59100 Roubaix, France.
        </p>

        <h2>Données personnelles · RGPD</h2>
        <p>
          Les adresses e-mail collectées via les formulaires de contact ou de signalement
          sont utilisées uniquement pour traiter la demande concernée. Elles ne sont
          jamais cédées ni partagées avec des tiers à des fins commerciales.
        </p>
        <p>
          Conformément au Règlement Général sur la Protection des Données, vous disposez
          d'un droit d'accès, de rectification et de suppression de vos données. Pour
          l'exercer, écrivez à <a href="mailto:cinqcibles@gmail.com">cinqcibles@gmail.com</a>.
        </p>

        <h2>Cookies</h2>
        <p>
          Le site n'utilise aucun cookie de mesure d'audience ni de cookie publicitaire.
          Le seul stockage local est celui de la préférence de thème (clair/sombre),
          conservée dans le <code>localStorage</code> de votre navigateur. Vous pouvez
          le supprimer à tout moment depuis les réglages du navigateur.
        </p>

        <h2>Propriété intellectuelle</h2>
        <p>
          Le travail éditorial — textes, mise en page, code source — est la propriété
          de Guillaume Alexandre. Les informations sur les compétitions sont issues de
          sources publiques officielles, citées dans chaque fiche du calendrier. Les
          marques, logos et photographies de tiers restent la propriété de leurs
          titulaires respectifs.
        </p>
        <p>
          Toute reproduction du contenu original doit faire l'objet d'une autorisation
          préalable.
        </p>

        <h2>Limitation de responsabilité</h2>
        <p>
          Les informations publiées sont vérifiées au mieux mais peuvent comporter des
          erreurs ou être obsolètes. Avant de vous déplacer pour une compétition,
          vérifiez systématiquement auprès de la source officielle indiquée sur la
          fiche de l'épreuve.
        </p>

        <h2>Signaler une erreur</h2>
        <p>
          Si vous constatez une information inexacte, écrivez à
          <a href="mailto:cinqcibles@gmail.com"> cinqcibles@gmail.com</a> en précisant
          la page concernée. La correction sera apportée dans les meilleurs délais.
        </p>
      </article>

      <window.Footer />
    </>
  );
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<MentionsPage />);
