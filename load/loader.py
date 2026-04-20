# =============================================================
#  load/loader.py
#  Phase LOAD : charge les données dans PostgreSQL (ou CSV en mode démo)
# =============================================================
#
#  Stratégie corrigée :
#    - On utilise TRUNCATE ... RESTART IDENTITY CASCADE
#      au lieu de DROP TABLE (qui échoue à cause des clés étrangères
#      et des vues matérialisées qui dépendent des dimensions)
#    - Ordre de chargement : dimensions d'abord, faits ensuite
#
# =============================================================

import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import text

logger = logging.getLogger("mexora_etl")


def get_engine(db_url: str):
    """
    Crée la connexion SQLAlchemy vers PostgreSQL.
    Lève une erreur claire si la connexion échoue.
    """
    try:
        from sqlalchemy import create_engine
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("[LOAD] Connexion PostgreSQL établie avec succès")
        return engine
    except Exception as e:
        logger.error(f"[LOAD] Impossible de se connecter à PostgreSQL : {e}")
        logger.error("[LOAD] Vérifie DB_CONFIG dans config/settings.py")
        raise


def _truncate_cascade(engine, schema: str, table_name: str) -> None:
    """
    Vide une table avec TRUNCATE ... RESTART IDENTITY CASCADE.
    - RESTART IDENTITY : remet les séquences (SERIAL) à 1
    - CASCADE          : vide aussi les tables dépendantes (fait_ventes)
                         et invalide les vues matérialisées
    C'est la bonne façon de vider une table qui a des dépendances.
    """
    sql = f'TRUNCATE TABLE {schema}.{table_name} RESTART IDENTITY CASCADE;'
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.debug(f"[LOAD] TRUNCATE CASCADE sur {schema}.{table_name} OK")


def charger_dimension(
    df         : pd.DataFrame,
    table_name : str,
    engine,
    schema     : str = "dwh_mexora",
) -> None:
    """
    Charge une table de dimension dans PostgreSQL.

    Stratégie :
      1. TRUNCATE ... CASCADE  → vide proprement (respecte les FK et vues)
      2. INSERT via pandas      → insère les nouvelles données

    Pourquoi pas if_exists='replace' ?
    → pandas fait un DROP TABLE qui échoue si des FK ou vues en dépendent.
    → TRUNCATE CASCADE est la solution correcte pour un DWH.
    """
    try:
        # Étape 1 : vider proprement
        _truncate_cascade(engine, schema, table_name)

        # Étape 2 : insérer les données (append = juste INSERT, pas de DROP)
        df.to_sql(
            name      = table_name,
            con       = engine,
            schema    = schema,
            if_exists = "append",   # table existe déjà → juste INSERT
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        logger.info(f"[LOAD] {schema}.{table_name} → {len(df):,} lignes chargées")
    except Exception as e:
        logger.error(f"[LOAD] Erreur lors du chargement de {table_name} : {e}")
        raise


def charger_faits(
    df         : pd.DataFrame,
    engine,
    schema     : str = "dwh_mexora",
    table_name : str = "fait_ventes",
) -> None:
    """
    Charge la table de faits dans PostgreSQL.
    Même stratégie : TRUNCATE CASCADE puis INSERT par blocs de 5000 lignes.
    """
    try:
        # Vider la table de faits
        _truncate_cascade(engine, schema, table_name)

        # Insérer par blocs
        total      = 0
        chunk_size = 5000
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            chunk.to_sql(
                name      = table_name,
                con       = engine,
                schema    = schema,
                if_exists = "append",
                index     = False,
                method    = "multi",
            )
            total += len(chunk)
            logger.debug(f"[LOAD] Chunk {i // chunk_size + 1} → {total:,} lignes insérées")

        logger.info(f"[LOAD] {schema}.{table_name} → {total:,} lignes chargées")
    except Exception as e:
        logger.error(f"[LOAD] Erreur lors du chargement de fait_ventes : {e}")
        raise


def charger_vers_csv(
    dimensions : dict,
    faits      : pd.DataFrame,
    output_dir,
) -> None:
    """
    Mode DÉMO sans PostgreSQL.
    Sauvegarde toutes les tables dans des fichiers CSV.
    Utile pour tester le pipeline sans base de données.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    for nom, df in dimensions.items():
        chemin = output_dir / f"{nom}.csv"
        df.to_csv(chemin, index=False, encoding="utf-8")
        logger.info(f"[LOAD-CSV] {nom} → {chemin.name} ({len(df):,} lignes)")

    chemin_faits = output_dir / "fait_ventes.csv"
    faits.to_csv(chemin_faits, index=False, encoding="utf-8")
    logger.info(f"[LOAD-CSV] fait_ventes → {chemin_faits.name} ({len(faits):,} lignes)")
    logger.info(f"[LOAD-CSV] Toutes les tables sauvegardées dans : {output_dir}/")