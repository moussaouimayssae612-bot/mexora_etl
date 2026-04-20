-- =============================================================
--  sql/requetes_analytiques.sql  (VERSION CORRIGÉE)
--  Les 5 requêtes analytiques obligatoires du projet
-- =============================================================


-- =============================================================
-- QUESTION 1A : CA total par région (classement)
-- =============================================================
SELECT
    r.region_admin,
    r.zone_geo,
    SUM(f.montant_ttc)          AS ca_total_ttc,
    COUNT(DISTINCT f.id_client) AS nb_clients,
    SUM(f.quantite_vendue)      AS volume_total
FROM      dwh_mexora.fait_ventes f
JOIN      dwh_mexora.dim_region  r ON f.id_region = r.id_region
JOIN      dwh_mexora.dim_temps   t ON f.id_date   = t.id_date
WHERE     f.statut_commande = 'livré'
GROUP BY  r.region_admin, r.zone_geo
ORDER BY  ca_total_ttc DESC;


-- =============================================================
-- QUESTION 1B : Évolution mensuelle CA par région (N vs N-1)
-- =============================================================
SELECT
    t.annee,
    t.mois,
    t.libelle_mois,
    r.region_admin,
    SUM(f.montant_ttc)  AS ca_ttc,
    LAG(SUM(f.montant_ttc)) OVER (
        PARTITION BY r.region_admin, t.mois
        ORDER BY t.annee
    )                   AS ca_annee_precedente,
    ROUND(
        (SUM(f.montant_ttc)
         - LAG(SUM(f.montant_ttc)) OVER (
               PARTITION BY r.region_admin, t.mois ORDER BY t.annee)
        ) * 100.0
        / NULLIF(
            LAG(SUM(f.montant_ttc)) OVER (
                PARTITION BY r.region_admin, t.mois ORDER BY t.annee),
          0),
    2)                  AS evolution_pct
FROM      dwh_mexora.fait_ventes f
JOIN      dwh_mexora.dim_temps   t ON f.id_date   = t.id_date
JOIN      dwh_mexora.dim_region  r ON f.id_region = r.id_region
WHERE     f.statut_commande = 'livré'
GROUP BY  t.annee, t.mois, t.libelle_mois, r.region_admin
ORDER BY  r.region_admin, t.annee, t.mois;


-- =============================================================
-- QUESTION 2 : Top 10 produits à Tanger par trimestre
--
-- RÈGLE POSTGRESQL : les fonctions de fenêtre (RANK, ROW_NUMBER...)
-- ne sont pas autorisées dans HAVING.
-- Solution : mettre RANK() dans une sous-requête,
--            puis filtrer avec WHERE à l'extérieur.
-- =============================================================
SELECT *
FROM (
    SELECT
        t.annee,
        t.trimestre,
        p.nom_produit,
        p.categorie,
        p.marque,
        SUM(f.quantite_vendue)      AS qte_vendue,
        SUM(f.montant_ttc)          AS ca_total,
        RANK() OVER (
            PARTITION BY t.annee, t.trimestre
            ORDER BY SUM(f.montant_ttc) DESC
        )                           AS rang
    FROM      dwh_mexora.fait_ventes f
    JOIN      dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
    JOIN      dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
    JOIN      dwh_mexora.dim_region  r ON f.id_region  = r.id_region
    WHERE     f.statut_commande = 'livré'
      AND     r.ville = 'Tanger'
    GROUP BY  t.annee, t.trimestre, p.nom_produit, p.categorie, p.marque
) sous_requete
WHERE rang <= 10
ORDER BY annee, trimestre, rang;


-- =============================================================
-- QUESTION 3 : Segment client avec le panier moyen le plus élevé
-- =============================================================
SELECT
    c.segment_client,
    COUNT(DISTINCT f.id_client)                     AS nb_clients,
    COUNT(DISTINCT f.id_vente)                      AS nb_commandes,
    ROUND(SUM(f.montant_ttc), 2)                    AS ca_total,
    ROUND(AVG(f.montant_ttc), 2)                    AS panier_moyen,
    ROUND(
        SUM(f.montant_ttc) * 100.0
        / SUM(SUM(f.montant_ttc)) OVER (),
    2)                                              AS part_ca_pct
FROM      dwh_mexora.fait_ventes f
JOIN      dwh_mexora.dim_client  c ON f.id_client = c.id_client_sk
WHERE     f.statut_commande = 'livré'
GROUP BY  c.segment_client
ORDER BY  panier_moyen DESC;


-- =============================================================
-- QUESTION 4 : Taux de retour par catégorie
--              Alerte rouge > 5%, orange 3-5%, vert < 3%
-- =============================================================
SELECT
    p.categorie,
    COUNT(*) FILTER (WHERE f.statut_commande = 'livré')    AS nb_livrees,
    COUNT(*) FILTER (WHERE f.statut_commande = 'retourné') AS nb_retournees,
    ROUND(
        COUNT(*) FILTER (WHERE f.statut_commande = 'retourné') * 100.0
        / NULLIF(
            COUNT(*) FILTER (WHERE f.statut_commande IN ('livré','retourné')),
          0),
    2)                                                     AS taux_retour_pct,
    CASE
        WHEN COUNT(*) FILTER (WHERE f.statut_commande = 'retourné') * 100.0
             / NULLIF(COUNT(*) FILTER (WHERE f.statut_commande IN ('livré','retourné')), 0)
             > 5  THEN 'ALERTE ROUGE'
        WHEN COUNT(*) FILTER (WHERE f.statut_commande = 'retourné') * 100.0
             / NULLIF(COUNT(*) FILTER (WHERE f.statut_commande IN ('livré','retourné')), 0)
             > 3  THEN 'ATTENTION ORANGE'
        ELSE           'OK VERT'
    END                                                    AS niveau_alerte
FROM      dwh_mexora.fait_ventes f
JOIN      dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
WHERE     f.statut_commande IN ('livré','retourné')
GROUP BY  p.categorie
ORDER BY  taux_retour_pct DESC;


-- =============================================================
-- QUESTION 5A : CA alimentation — Ramadan vs Hors Ramadan
-- =============================================================
SELECT
    t.annee,
    CASE WHEN t.periode_ramadan THEN 'Pendant Ramadan'
         ELSE 'Hors Ramadan' END        AS periode,
    COUNT(DISTINCT t.id_date)           AS nb_jours,
    ROUND(SUM(f.montant_ttc), 2)        AS ca_total,
    ROUND(
        SUM(f.montant_ttc)
        / NULLIF(COUNT(DISTINCT t.id_date), 0),
    2)                                  AS ca_moyen_journalier,
    SUM(f.quantite_vendue)              AS volume_total
FROM      dwh_mexora.fait_ventes f
JOIN      dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
JOIN      dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
WHERE     f.statut_commande = 'livré'
  AND     p.categorie       = 'Alimentation'
GROUP BY  t.annee, t.periode_ramadan
ORDER BY  t.annee, t.periode_ramadan;


-- =============================================================
-- QUESTION 5B : Indice de performance Ramadan
--               (indice > 1 = surperforme la moyenne)
-- =============================================================
WITH stats AS (
    SELECT
        t.annee,
        t.periode_ramadan,
        SUM(f.montant_ttc) / NULLIF(COUNT(DISTINCT t.id_date), 0) AS ca_par_jour
    FROM      dwh_mexora.fait_ventes f
    JOIN      dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
    JOIN      dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
    WHERE     f.statut_commande = 'livré'
      AND     p.categorie = 'Alimentation'
    GROUP BY  t.annee, t.periode_ramadan
),
moyenne AS (
    SELECT annee, AVG(ca_par_jour) AS ca_moyen_global
    FROM   stats
    GROUP BY annee
)
SELECT
    s.annee,
    CASE WHEN s.periode_ramadan THEN 'Pendant Ramadan'
         ELSE 'Hors Ramadan' END            AS periode,
    ROUND(s.ca_par_jour, 2)                 AS ca_journalier,
    ROUND(m.ca_moyen_global, 2)             AS ca_journalier_moyen,
    ROUND(s.ca_par_jour / NULLIF(m.ca_moyen_global, 0), 3) AS indice_performance
FROM      stats   s
JOIN      moyenne m ON s.annee = m.annee
ORDER BY  s.annee, s.periode_ramadan;


-- =============================================================
-- BONUS : Top 10 meilleurs clients à Tanger ce trimestre
-- =============================================================
SELECT
    c.nom_complet,
    c.segment_client,
    COUNT(DISTINCT f.id_vente)   AS nb_commandes,
    ROUND(SUM(f.montant_ttc), 2) AS ca_total,
    ROUND(AVG(f.montant_ttc), 2) AS panier_moyen
FROM      dwh_mexora.fait_ventes f
JOIN      dwh_mexora.dim_client  c ON f.id_client = c.id_client_sk
JOIN      dwh_mexora.dim_region  r ON f.id_region = r.id_region
JOIN      dwh_mexora.dim_temps   t ON f.id_date   = t.id_date
WHERE     f.statut_commande = 'livré'
  AND     r.ville           = 'Tanger'
  AND     t.annee           = EXTRACT(YEAR    FROM CURRENT_DATE)
  AND     t.trimestre       = EXTRACT(QUARTER FROM CURRENT_DATE)
GROUP BY  c.nom_complet, c.segment_client
ORDER BY  ca_total DESC
LIMIT 10;