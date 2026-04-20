#!/usr/bin/env python3
# =============================================================
#  main.py
#  Point d'entrée unique du pipeline ETL Mexora Analytics
# =============================================================
#
#  Usage :
#    python main.py         → mode démo (sauvegarde en CSV, sans PostgreSQL)
#    python main.py --pg    → mode production (charge dans PostgreSQL)
#
#  Structure du pipeline :
#    EXTRACT   → lit les 4 fichiers sources bruts
#    TRANSFORM → nettoie + construit les 5 dimensions + la table de faits
#    LOAD      → insère dans PostgreSQL (ou CSV en mode démo)
#
# =============================================================

import sys
import logging
from datetime import datetime
from pathlib import Path

# Ajouter le dossier racine au chemin Python pour les imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

# ── Imports internes ──────────────────────────────────────────
from utils.logger import get_logger
from config.settings import SOURCE_FILES, DB_URL

from extract.extractor import (
    extract_commandes,
    extract_produits,
    extract_clients,
    extract_regions,
)
from transform.clean_commandes import transform_commandes
from transform.clean_clients   import transform_clients, calculer_segments_clients
from transform.clean_produits  import transform_produits
from transform.build_dimensions import (
    build_dim_temps,
    build_dim_produit,
    build_dim_client,
    build_dim_region,
    build_dim_livreur,
    build_fait_ventes,
)
from load.loader import charger_vers_csv, charger_dimension, charger_faits

# Démarrer le logger
logger = get_logger("mexora_etl")


def run_pipeline(use_postgres: bool = False):
    """
    Lance le pipeline complet Extract → Transform → Load.
    Retourne un dictionnaire avec toutes les tables construites.
    """
    start = datetime.now()

    logger.info("=" * 60)
    logger.info("  DÉMARRAGE PIPELINE ETL MEXORA ANALYTICS")
    logger.info(f"  Mode : {'PostgreSQL' if use_postgres else 'Démo CSV'}")
    logger.info("=" * 60)

    try:
        # =====================================================
        # PHASE 1 — EXTRACT
        # =====================================================
        logger.info("--- PHASE EXTRACT ---")

        df_commandes_raw = extract_commandes(SOURCE_FILES["commandes"])
        df_produits_raw  = extract_produits(SOURCE_FILES["produits"])
        df_clients_raw   = extract_clients(SOURCE_FILES["clients"])
        df_regions       = extract_regions(SOURCE_FILES["regions"])

        # =====================================================
        # PHASE 2 — TRANSFORM
        # =====================================================
        logger.info("--- PHASE TRANSFORM ---")

        # Nettoyage des données sources
        df_commandes = transform_commandes(df_commandes_raw, df_regions)
        df_clients   = transform_clients(df_clients_raw, df_regions)
        df_produits  = transform_produits(df_produits_raw)

        # Calcul de la segmentation client
        df_segments  = calculer_segments_clients(df_commandes)

        # Construction des 5 dimensions
        logger.info("  Construction des dimensions...")
        dim_temps   = build_dim_temps()
        dim_produit = build_dim_produit(df_produits)
        dim_client  = build_dim_client(df_clients, df_segments)
        dim_region  = build_dim_region(df_regions)
        dim_livreur = build_dim_livreur(df_commandes)

        # Construction de la table de faits
        logger.info("  Construction de la table de faits...")
        fait_ventes = build_fait_ventes(
            df_commandes, dim_temps, dim_client,
            dim_produit,  dim_region, dim_livreur,
        )

        # =====================================================
        # PHASE 3 — LOAD
        # =====================================================
        logger.info("--- PHASE LOAD ---")

        dimensions = {
            "dim_temps"   : dim_temps,
            "dim_produit" : dim_produit,
            "dim_client"  : dim_client,
            "dim_region"  : dim_region,
            "dim_livreur" : dim_livreur,
        }

        if use_postgres:
            # Mode PostgreSQL (nécessite pgAdmin + DB créée)
            from load.loader import get_engine
            engine = get_engine(DB_URL)
            for nom, df in dimensions.items():
                charger_dimension(df, nom, engine)
            charger_faits(fait_ventes, engine)
        else:
            # Mode démo CSV (fonctionne sans PostgreSQL)
            output_dir = Path(__file__).parent / "output_dwh"
            charger_vers_csv(dimensions, fait_ventes, output_dir)

        # =====================================================
        # RÉSUMÉ FINAL
        # =====================================================
        duree = (datetime.now() - start).seconds
        logger.info("=" * 60)
        logger.info(f"  PIPELINE TERMINÉ EN {duree} seconde(s)")
        logger.info("  Lignes dans chaque table :")
        for nom, df in dimensions.items():
            logger.info(f"    {nom:<20} : {len(df):>8,} lignes")
        logger.info(f"    {'fait_ventes':<20} : {len(fait_ventes):>8,} lignes")
        logger.info("=" * 60)

        return {**dimensions, "fait_ventes": fait_ventes}

    except Exception as e:
        logger.error(f"ERREUR PIPELINE : {e}", exc_info=True)
        raise


if __name__ == "__main__":
    use_pg = "--pg" in sys.argv
    run_pipeline(use_postgres=use_pg)
