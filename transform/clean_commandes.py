# =============================================================
#  transform/clean_commandes.py
#  Nettoyage et standardisation des commandes Mexora
# =============================================================
#
#  7 règles appliquées (R1 à R7) :
#    R1 - Suppression des doublons sur id_commande
#    R2 - Standardisation des dates (format mixte → YYYY-MM-DD)
#    R3 - Harmonisation des noms de villes via le référentiel
#    R4 - Standardisation des statuts (OK→en_cours, DONE→livré, etc.)
#    R5 - Suppression des lignes avec quantite <= 0
#    R6 - Suppression des lignes avec prix_unitaire = 0 (commandes test)
#    R7 - Remplacement des id_livreur manquants par "-1"
#
# =============================================================

import logging
import pandas as pd
from config.settings import MAPPING_STATUTS, TVA_MAROC

logger = logging.getLogger("mexora_etl")


def _construire_mapping_villes(df_regions: pd.DataFrame) -> dict:
    """
    Construit un dictionnaire :
        "tanger"   → "Tanger"
        "tng"      → "Tanger"
        "TANGER"   → "Tanger"   etc.
    depuis le référentiel officiel regions_maroc.csv
    """
    mapping = {}
    for _, row in df_regions.iterrows():
        nom_std = row["nom_ville_standard"]
        code    = row["code_ville"]
        # On ajoute toutes les variantes possibles (en minuscules comme clé)
        for variante in [nom_std, nom_std.upper(), code, code.lower()]:
            mapping[variante.strip().lower()] = nom_std
    return mapping


def transform_commandes(df: pd.DataFrame, df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Applique les 7 règles de nettoyage sur les commandes.
    Retourne un DataFrame propre avec les colonnes montant_ht et montant_ttc calculées.
    """
    df = df.copy()
    n_initial = len(df)
    logger.info(f"[TRANSFORM commandes] Début : {n_initial:,} lignes")

    # ---------------------------------------------------------
    # R1 - Suppression des doublons sur id_commande
    # ---------------------------------------------------------
    n_avant = len(df)
    df = df.drop_duplicates(subset=["id_commande"], keep="last")
    n_suppr = n_avant - len(df)
    logger.info(f"[R1] Doublons supprimés : {n_suppr} lignes")

    # ---------------------------------------------------------
    # R2 - Standardisation des dates (formats mixtes)
    # Ex : "15/11/2024", "2024-11-15", "Nov 15 2024" → datetime
    # ---------------------------------------------------------
    df["date_commande"] = pd.to_datetime(
        df["date_commande"],
        format="mixed",      # accepte tous les formats
        dayfirst=True,       # DD/MM/YYYY en priorité
        errors="coerce",     # dates invalides → NaT
    )
    n_dates_invalides = df["date_commande"].isna().sum()
    df = df.dropna(subset=["date_commande"])
    logger.info(f"[R2] Dates invalides supprimées : {n_dates_invalides}")

    # Date de livraison (optionnelle, peut être vide)
    df["date_livraison"] = pd.to_datetime(
        df["date_livraison"].replace("", pd.NaT),
        errors="coerce",
    )

    # ---------------------------------------------------------
    # R3 - Harmonisation des noms de villes
    # Ex : "tanger", "TNG", "TANGER", "Tnja" → "Tanger"
    # ---------------------------------------------------------
    mapping_villes = _construire_mapping_villes(df_regions)
    df["ville_livraison_clean"] = df["ville_livraison"].str.strip().str.lower()
    df["ville_livraison"] = df["ville_livraison_clean"].map(mapping_villes).fillna("Non renseignée")
    df = df.drop(columns=["ville_livraison_clean"])
    n_non_mappes = (df["ville_livraison"] == "Non renseignée").sum()
    logger.info(f"[R3] Villes non reconnues → 'Non renseignée' : {n_non_mappes}")

    # ---------------------------------------------------------
    # R4 - Standardisation des statuts
    # Ex : "OK"→"en_cours", "DONE"→"livré", "KO"→"annulé"
    # ---------------------------------------------------------
    df["statut"] = df["statut"].str.strip().map(MAPPING_STATUTS)
    n_inconnus = df["statut"].isna().sum()
    df["statut"] = df["statut"].fillna("inconnu")
    logger.warning(f"[R4] Statuts non reconnus → 'inconnu' : {n_inconnus}")

    # ---------------------------------------------------------
    # R5 - Suppression quantite <= 0
    # ---------------------------------------------------------
    df["quantite"] = pd.to_numeric(df["quantite"], errors="coerce")
    n_avant = len(df)
    df = df[df["quantite"] > 0]
    logger.info(f"[R5] Quantités <= 0 supprimées : {n_avant - len(df)}")

    # ---------------------------------------------------------
    # R6 - Suppression prix_unitaire = 0 (commandes test)
    # ---------------------------------------------------------
    df["prix_unitaire"] = pd.to_numeric(df["prix_unitaire"], errors="coerce")
    n_avant = len(df)
    df = df[df["prix_unitaire"] > 0]
    logger.info(f"[R6] Prix nuls (commandes test) supprimés : {n_avant - len(df)}")

    # ---------------------------------------------------------
    # R7 - Livreurs manquants → "-1" (livreur inconnu)
    # ---------------------------------------------------------
    n_manquants = (df["id_livreur"].isna() | (df["id_livreur"] == "")).sum()
    df["id_livreur"] = df["id_livreur"].replace("", "-1").fillna("-1")
    logger.info(f"[R7] Livreurs manquants remplacés par '-1' : {n_manquants}")

    # ---------------------------------------------------------
    # Calcul des montants HT et TTC
    # ---------------------------------------------------------
    df["montant_ht"]  = (df["quantite"] * df["prix_unitaire"]).round(2)
    df["montant_ttc"] = (df["montant_ht"] * (1 + TVA_MAROC)).round(2)

    # Calcul du délai de livraison en jours
    df["delai_livraison_jours"] = (df["date_livraison"] - df["date_commande"]).dt.days

    n_final = len(df)
    logger.info(
        f"[TRANSFORM commandes] Fin : {n_initial:,} → {n_final:,} lignes "
        f"(supprimées : {n_initial - n_final:,})"
    )
    return df
