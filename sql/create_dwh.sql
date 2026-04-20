-- =============================================================
--  sql/create_dwh.sql
--  Script complet de création du Data Warehouse Mexora Analytics
--  À exécuter dans pgAdmin > Query Tool
-- =============================================================
--
--  ORDRE D'EXÉCUTION :
--    1. Créer les schémas
--    2. Créer les tables de dimensions
--    3. Créer la table de faits
--    4. Créer les index
--    5. Créer les vues matérialisées
--
--  AVANT D'EXÉCUTER :
--    - Crée d'abord la base de données "mexora_dwh" dans pgAdmin
--    - Connecte-toi à cette base
--    - Colle tout ce script dans Query Tool et clique Run
-- =============================================================


-- =============================================================
-- ÉTAPE 0 : Supprimer l'ancien schéma si besoin (RESET COMPLET)
-- Décommente ces lignes si tu veux tout recommencer à zéro
-- =============================================================
-- DROP SCHEMA IF EXISTS dwh_mexora       CASCADE;
-- DROP SCHEMA IF EXISTS staging_mexora   CASCADE;
-- DROP SCHEMA IF EXISTS reporting_mexora CASCADE;


-- =============================================================
-- ÉTAPE 1 : Créer les 3 schémas
-- =============================================================

CREATE SCHEMA IF NOT EXISTS staging_mexora;
    COMMENT ON SCHEMA staging_mexora   IS 'Zone de transit des données brutes avant transformation';

CREATE SCHEMA IF NOT EXISTS dwh_mexora;
    COMMENT ON SCHEMA dwh_mexora       IS 'Data Warehouse : dimensions et faits du schéma en étoile';

CREATE SCHEMA IF NOT EXISTS reporting_mexora;
    COMMENT ON SCHEMA reporting_mexora IS 'Vues matérialisées pour les dashboards analytiques';


-- =============================================================
-- ÉTAPE 2 : Tables de DIMENSIONS
-- =============================================================

-- -------------------------------------------------------------
-- DIM_TEMPS : calendrier complet avec jours fériés marocains
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh_mexora.dim_temps (
    id_date          INTEGER      PRIMARY KEY,           -- format YYYYMMDD  ex: 20241115
    jour             SMALLINT     NOT NULL CHECK (jour     BETWEEN 1 AND 31),
    mois             SMALLINT     NOT NULL CHECK (mois     BETWEEN 1 AND 12),
    trimestre        SMALLINT     NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    annee            SMALLINT     NOT NULL,
    semaine          SMALLINT,
    libelle_jour     VARCHAR(20),                        -- "Monday", "Tuesday"...
    libelle_mois     VARCHAR(20),                        -- "January", "February"...
    est_weekend      BOOLEAN      DEFAULT FALSE,
    est_ferie_maroc  BOOLEAN      DEFAULT FALSE,
    periode_ramadan  BOOLEAN      DEFAULT FALSE
);
COMMENT ON TABLE dwh_mexora.dim_temps IS 'Dimension temporelle : chaque ligne = 1 jour';


-- -------------------------------------------------------------
-- DIM_PRODUIT : avec colonnes SCD Type 2
-- SCD = Slowly Changing Dimension
-- Type 2 = on conserve l'historique des changements
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh_mexora.dim_produit (
    id_produit_sk    SERIAL       PRIMARY KEY,           -- Surrogate Key (clé artificielle auto-incrémentée)
    id_produit_nk    VARCHAR(20)  NOT NULL,              -- Natural Key  (clé source, ex: "P001")
    nom_produit      VARCHAR(200) NOT NULL,
    categorie        VARCHAR(100),
    sous_categorie   VARCHAR(100),
    marque           VARCHAR(100),
    fournisseur      VARCHAR(100),
    prix_standard    DECIMAL(10,2),
    origine_pays     VARCHAR(50),
    -- Colonnes SCD Type 2
    date_debut       DATE         NOT NULL DEFAULT CURRENT_DATE,
    date_fin         DATE         NOT NULL DEFAULT '9999-12-31',
    est_actif        BOOLEAN      NOT NULL DEFAULT TRUE
);
COMMENT ON TABLE dwh_mexora.dim_produit IS 'Dimension produit avec historique SCD Type 2';
COMMENT ON COLUMN dwh_mexora.dim_produit.id_produit_sk IS 'Clé artificielle (surrogate key)';
COMMENT ON COLUMN dwh_mexora.dim_produit.id_produit_nk IS 'Clé naturelle issue du système source';
COMMENT ON COLUMN dwh_mexora.dim_produit.date_fin      IS '9999-12-31 = ligne encore active';


-- -------------------------------------------------------------
-- DIM_CLIENT : avec SCD Type 2 sur le segment
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh_mexora.dim_client (
    id_client_sk     SERIAL       PRIMARY KEY,
    id_client_nk     VARCHAR(20)  NOT NULL,
    nom_complet      VARCHAR(200),
    tranche_age      VARCHAR(10),                        -- "<18", "18-24", "25-34"...
    sexe             CHAR(1),                            -- "m", "f"
    ville            VARCHAR(100),
    region_admin     VARCHAR(100),
    segment_client   VARCHAR(20)  CHECK (segment_client IN ('Gold','Silver','Bronze')),
    canal_acquisition VARCHAR(50),
    -- SCD Type 2
    date_debut       DATE         NOT NULL DEFAULT CURRENT_DATE,
    date_fin         DATE         NOT NULL DEFAULT '9999-12-31',
    est_actif        BOOLEAN      NOT NULL DEFAULT TRUE
);
COMMENT ON TABLE dwh_mexora.dim_client IS 'Dimension client avec segment Gold/Silver/Bronze';


-- -------------------------------------------------------------
-- DIM_REGION : référentiel géographique du Maroc
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh_mexora.dim_region (
    id_region        SERIAL       PRIMARY KEY,
    ville            VARCHAR(100) NOT NULL,
    province         VARCHAR(100),
    region_admin     VARCHAR(100),
    zone_geo         VARCHAR(50),                        -- "Nord", "Centre", "Sud", "Est"
    pays             VARCHAR(50)  DEFAULT 'Maroc'
);
COMMENT ON TABLE dwh_mexora.dim_region IS 'Dimension géographique : villes et régions du Maroc';


-- -------------------------------------------------------------
-- DIM_LIVREUR
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh_mexora.dim_livreur (
    id_livreur       SERIAL       PRIMARY KEY,
    id_livreur_nk    VARCHAR(20),
    nom_livreur      VARCHAR(100),
    type_transport   VARCHAR(50),                        -- "Moto", "Camionnette", "Vélo"
    zone_couverture  VARCHAR(100)
);
COMMENT ON TABLE dwh_mexora.dim_livreur IS 'Dimension livreurs Mexora';


-- =============================================================
-- ÉTAPE 3 : Table de FAITS
-- =============================================================

CREATE TABLE IF NOT EXISTS dwh_mexora.fait_ventes (
    id_vente                BIGSERIAL    PRIMARY KEY,

    -- Clés étrangères vers les dimensions
    id_date                 INTEGER      NOT NULL REFERENCES dwh_mexora.dim_temps(id_date),
    id_produit              INTEGER      NOT NULL REFERENCES dwh_mexora.dim_produit(id_produit_sk),
    id_client               INTEGER      NOT NULL REFERENCES dwh_mexora.dim_client(id_client_sk),
    id_region               INTEGER      NOT NULL REFERENCES dwh_mexora.dim_region(id_region),
    id_livreur              INTEGER               REFERENCES dwh_mexora.dim_livreur(id_livreur),

    -- Mesures ADDITIVES (on peut faire SUM sur n'importe quelle dimension)
    quantite_vendue         INTEGER      NOT NULL CHECK (quantite_vendue > 0),
    montant_ht              DECIMAL(12,2) NOT NULL,
    montant_ttc             DECIMAL(12,2) NOT NULL,
    cout_livraison          DECIMAL(8,2),

    -- Mesure SEMI-ADDITIVE (sommer dans le temps n'a pas de sens)
    delai_livraison_jours   SMALLINT,

    -- Mesure NON-ADDITIVE (taux = à recalculer, ne jamais sommer)
    remise_pct              DECIMAL(5,2) DEFAULT 0,

    -- Métadonnées ETL
    date_chargement         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    statut_commande         VARCHAR(20)  CHECK (statut_commande IN ('livré','annulé','en_cours','retourné','inconnu'))
);
COMMENT ON TABLE dwh_mexora.fait_ventes IS 'Table de faits : une ligne = une commande. Granularité : commande × produit';
COMMENT ON COLUMN dwh_mexora.fait_ventes.montant_ht  IS 'Mesure additive : peut être sommée sur toutes les dimensions';
COMMENT ON COLUMN dwh_mexora.fait_ventes.remise_pct  IS 'Mesure non-additive : NE PAS sommer, recalculer par requête';


-- =============================================================
-- ÉTAPE 4 : INDEX pour accélérer les requêtes analytiques
-- =============================================================

-- Index simples sur les clés étrangères (jointures)
CREATE INDEX IF NOT EXISTS idx_fv_date     ON dwh_mexora.fait_ventes(id_date);
CREATE INDEX IF NOT EXISTS idx_fv_produit  ON dwh_mexora.fait_ventes(id_produit);
CREATE INDEX IF NOT EXISTS idx_fv_client   ON dwh_mexora.fait_ventes(id_client);
CREATE INDEX IF NOT EXISTS idx_fv_region   ON dwh_mexora.fait_ventes(id_region);
CREATE INDEX IF NOT EXISTS idx_fv_livreur  ON dwh_mexora.fait_ventes(id_livreur);

-- Index composites pour les requêtes analytiques fréquentes
CREATE INDEX IF NOT EXISTS idx_fv_date_region
    ON dwh_mexora.fait_ventes(id_date, id_region)
    INCLUDE (montant_ttc, quantite_vendue);

-- Index partiel : uniquement les commandes livrées (les + requêtées)
CREATE INDEX IF NOT EXISTS idx_fv_livre
    ON dwh_mexora.fait_ventes(statut_commande)
    WHERE statut_commande = 'livré';


-- =============================================================
-- ÉTAPE 5 : VUES MATÉRIALISÉES (3 obligatoires)
-- Une vue matérialisée = résultat pré-calculé stocké sur disque
-- → Réponse en millisecondes au lieu de secondes
-- =============================================================

-- -------------------------------------------------------------
-- Vue 1 : CA mensuel par région et catégorie
-- Répond à : "Quelle région génère le plus de CA ce mois-ci ?"
-- -------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS reporting_mexora.mv_ca_mensuel AS
SELECT
    t.annee,
    t.mois,
    t.libelle_mois,
    t.periode_ramadan,
    r.region_admin,
    r.zone_geo,
    p.categorie,
    SUM(f.montant_ttc)              AS ca_ttc,
    SUM(f.montant_ht)               AS ca_ht,
    COUNT(DISTINCT f.id_client)     AS nb_clients_actifs,
    SUM(f.quantite_vendue)          AS volume_vendu,
    AVG(f.montant_ttc)              AS panier_moyen,
    COUNT(DISTINCT f.id_vente)      AS nb_commandes
FROM      dwh_mexora.fait_ventes f
JOIN      dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
JOIN      dwh_mexora.dim_region  r ON f.id_region  = r.id_region
JOIN      dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
WHERE     f.statut_commande = 'livré'
GROUP BY  t.annee, t.mois, t.libelle_mois, t.periode_ramadan,
          r.region_admin, r.zone_geo, p.categorie
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_mv_ca_annee    ON reporting_mexora.mv_ca_mensuel(annee, mois);
CREATE INDEX IF NOT EXISTS idx_mv_ca_region   ON reporting_mexora.mv_ca_mensuel(region_admin);
CREATE INDEX IF NOT EXISTS idx_mv_ca_categorie ON reporting_mexora.mv_ca_mensuel(categorie);


-- -------------------------------------------------------------
-- Vue 2 : Top produits par trimestre
-- Répond à : "Quels sont les 10 produits les plus vendus à Tanger ce trimestre ?"
-- -------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS reporting_mexora.mv_top_produits AS
SELECT
    t.annee,
    t.trimestre,
    p.nom_produit,
    p.categorie,
    p.marque,
    SUM(f.quantite_vendue)          AS qte_totale,
    SUM(f.montant_ttc)              AS ca_total,
    COUNT(DISTINCT f.id_client)     AS nb_clients_distincts,
    RANK() OVER (
        PARTITION BY t.annee, t.trimestre, p.categorie
        ORDER BY SUM(f.montant_ttc) DESC
    )                               AS rang_dans_categorie
FROM      dwh_mexora.fait_ventes f
JOIN      dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
JOIN      dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
WHERE     f.statut_commande = 'livré'
GROUP BY  t.annee, t.trimestre, p.nom_produit, p.categorie, p.marque
WITH DATA;


-- -------------------------------------------------------------
-- Vue 3 : Performance des livreurs (taux de retard)
-- Répond à : "Quel livreur a le plus de retards ?"
-- -------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS reporting_mexora.mv_performance_livreurs AS
SELECT
    l.nom_livreur,
    l.zone_couverture,
    t.annee,
    t.mois,
    COUNT(*)                                                         AS nb_livraisons,
    AVG(f.delai_livraison_jours)                                     AS delai_moyen_jours,
    COUNT(*) FILTER (WHERE f.delai_livraison_jours > 3)              AS nb_livraisons_retard,
    ROUND(
        COUNT(*) FILTER (WHERE f.delai_livraison_jours > 3) * 100.0
        / NULLIF(COUNT(*), 0),
    2)                                                               AS taux_retard_pct
FROM      dwh_mexora.fait_ventes f
JOIN      dwh_mexora.dim_livreur l ON f.id_livreur = l.id_livreur
JOIN      dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
WHERE     f.statut_commande IN ('livré','retourné')
  AND     f.delai_livraison_jours IS NOT NULL
GROUP BY  l.nom_livreur, l.zone_couverture, t.annee, t.mois
WITH DATA;


-- =============================================================
-- VÉRIFICATION FINALE
-- =============================================================
-- Affiche toutes les tables créées dans dwh_mexora
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS taille
FROM pg_tables
WHERE schemaname IN ('dwh_mexora','reporting_mexora','staging_mexora')
ORDER BY schemaname, tablename;
