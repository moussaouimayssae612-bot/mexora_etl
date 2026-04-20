-- =============================================================
--  sql/check_integrity.sql
--  Vérifications d'intégrité du Data Warehouse Mexora
--  À exécuter après le chargement des données (python main.py --pg)
-- =============================================================


-- =============================================================
-- TEST 1 : Nombre de lignes dans chaque table
-- =============================================================
SELECT 'dim_temps'    AS table_name, COUNT(*) AS nb_lignes FROM dwh_mexora.dim_temps
UNION ALL
SELECT 'dim_produit',  COUNT(*) FROM dwh_mexora.dim_produit
UNION ALL
SELECT 'dim_client',   COUNT(*) FROM dwh_mexora.dim_client
UNION ALL
SELECT 'dim_region',   COUNT(*) FROM dwh_mexora.dim_region
UNION ALL
SELECT 'dim_livreur',  COUNT(*) FROM dwh_mexora.dim_livreur
UNION ALL
SELECT 'fait_ventes',  COUNT(*) FROM dwh_mexora.fait_ventes
ORDER BY table_name;


-- =============================================================
-- TEST 2 : Vérifier qu'il n'y a pas de clés étrangères orphelines
-- (lignes dans fait_ventes sans correspondance dans les dimensions)
-- =============================================================

-- Orphelins sur id_date
SELECT 'ORPHELINS id_date' AS test, COUNT(*) AS nb_problemes
FROM dwh_mexora.fait_ventes f
LEFT JOIN dwh_mexora.dim_temps t ON f.id_date = t.id_date
WHERE t.id_date IS NULL;

-- Orphelins sur id_produit
SELECT 'ORPHELINS id_produit' AS test, COUNT(*) AS nb_problemes
FROM dwh_mexora.fait_ventes f
LEFT JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
WHERE p.id_produit_sk IS NULL;

-- Orphelins sur id_client
SELECT 'ORPHELINS id_client' AS test, COUNT(*) AS nb_problemes
FROM dwh_mexora.fait_ventes f
LEFT JOIN dwh_mexora.dim_client c ON f.id_client = c.id_client_sk
WHERE c.id_client_sk IS NULL;

-- Orphelins sur id_region
SELECT 'ORPHELINS id_region' AS test, COUNT(*) AS nb_problemes
FROM dwh_mexora.fait_ventes f
LEFT JOIN dwh_mexora.dim_region r ON f.id_region = r.id_region
WHERE r.id_region IS NULL;


-- =============================================================
-- TEST 3 : Vérifier les valeurs NULL dans les mesures obligatoires
-- =============================================================
SELECT
    'NULL montant_ttc'  AS test, COUNT(*) FROM dwh_mexora.fait_ventes WHERE montant_ttc IS NULL
UNION ALL
SELECT
    'NULL montant_ht',           COUNT(*) FROM dwh_mexora.fait_ventes WHERE montant_ht IS NULL
UNION ALL
SELECT
    'NULL quantite_vendue',      COUNT(*) FROM dwh_mexora.fait_ventes WHERE quantite_vendue IS NULL
UNION ALL
SELECT
    'Quantites negatives',       COUNT(*) FROM dwh_mexora.fait_ventes WHERE quantite_vendue <= 0
UNION ALL
SELECT
    'Prix negatifs',             COUNT(*) FROM dwh_mexora.fait_ventes WHERE montant_ttc < 0;


-- =============================================================
-- TEST 4 : Vérifier les segments clients
-- =============================================================
SELECT
    segment_client,
    COUNT(*)          AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM dwh_mexora.dim_client
GROUP BY segment_client
ORDER BY nb_clients DESC;


-- =============================================================
-- TEST 5 : Distribution des statuts de commandes
-- =============================================================
SELECT
    statut_commande,
    COUNT(*)  AS nb_commandes,
    ROUND(SUM(montant_ttc), 2) AS ca_total
FROM dwh_mexora.fait_ventes
GROUP BY statut_commande
ORDER BY nb_commandes DESC;


-- =============================================================
-- TEST 6 : Vérifier la dimension temps
-- (jours fériés et Ramadan bien présents)
-- =============================================================
SELECT
    annee,
    COUNT(*) FILTER (WHERE est_ferie_maroc) AS nb_feries,
    COUNT(*) FILTER (WHERE periode_ramadan) AS nb_jours_ramadan,
    COUNT(*) FILTER (WHERE est_weekend)     AS nb_weekends
FROM dwh_mexora.dim_temps
GROUP BY annee
ORDER BY annee;


-- =============================================================
-- TEST 7 : Vérifier SCD Type 2 sur dim_produit
-- =============================================================
SELECT
    est_actif,
    COUNT(*) AS nb_produits,
    MIN(date_debut) AS premiere_date,
    MAX(date_fin)   AS derniere_date
FROM dwh_mexora.dim_produit
GROUP BY est_actif;


-- =============================================================
-- RÉSUMÉ : Afficher un verdict global
-- =============================================================
SELECT
    CASE
        WHEN (
            SELECT COUNT(*) FROM dwh_mexora.fait_ventes f
            LEFT JOIN dwh_mexora.dim_temps t ON f.id_date = t.id_date
            WHERE t.id_date IS NULL
        ) = 0
        AND (
            SELECT COUNT(*) FROM dwh_mexora.fait_ventes
            WHERE montant_ttc IS NULL OR montant_ttc < 0
        ) = 0
        THEN '✅ INTÉGRITÉ OK — Le DWH est propre'
        ELSE '❌ PROBLÈMES DÉTECTÉS — Vérifier les tests ci-dessus'
    END AS verdict;
