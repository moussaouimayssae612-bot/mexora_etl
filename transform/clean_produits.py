# =============================================================
#  transform/clean_produits.py
#  Nettoyage des produits Mexora
# =============================================================
#
#  Règles appliquées :
#    R1 - Normalisation de la casse des catégories
#         "electronique" / "ELECTRONIQUE" → "Electronique"
#    R2 - Prix catalogue NULL → remplacé par la médiane de la catégorie
#    R3 - Produits inactifs (actif=false) : conservés en SCD Type 2
#         (on NE les supprime PAS, on les marque est_actif=False)
#
# =============================================================

import logging
import pandas as pd

logger = logging.getLogger("mexora_etl")

# Catégories valides et leurs variantes
NORMALISATION_CATEGORIES = {
    "electronique"  : "Electronique",
    "électronique"  : "Electronique",
    "electronics"   : "Electronique",
    "mode"          : "Mode",
    "fashion"       : "Mode",
    "alimentation"  : "Alimentation",
    "food"          : "Alimentation",
    "accessoires"   : "Accessoires",
}


def transform_produits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les données produits.
    Retourne un DataFrame propre.
    """
    df = df.copy()
    n_initial = len(df)
    logger.info(f"[TRANSFORM produits] Début : {n_initial:,} lignes")

    # ---------------------------------------------------------
    # R1 - Normalisation de la casse des catégories
    # ---------------------------------------------------------
    df["categorie_clean"] = df["categorie"].str.strip().str.lower()
    df["categorie"] = df["categorie_clean"].map(NORMALISATION_CATEGORIES)

    # Si la catégorie n'est pas dans le mapping → garder la version .title()
    masque_non_mappe = df["categorie"].isna()
    df.loc[masque_non_mappe, "categorie"] = df.loc[masque_non_mappe, "categorie_clean"].str.title()
    n_normalises = (~masque_non_mappe).sum()
    df = df.drop(columns=["categorie_clean"])
    logger.info(f"[R1] Catégories normalisées : {n_normalises}")

    # ---------------------------------------------------------
    # R2 - Prix NULL → médiane de la catégorie
    # ---------------------------------------------------------
    df["prix_catalogue"] = pd.to_numeric(df["prix_catalogue"], errors="coerce")
    n_prix_manquants = df["prix_catalogue"].isna().sum()
    if n_prix_manquants > 0:
        mediane_par_cat = df.groupby("categorie")["prix_catalogue"].transform("median")
        df["prix_catalogue"] = df["prix_catalogue"].fillna(mediane_par_cat)
    logger.info(f"[R2] Prix manquants remplacés par médiane catégorie : {n_prix_manquants}")

    # ---------------------------------------------------------
    # R3 - Gestion des produits inactifs (SCD Type 2)
    # On convertit "True"/"False" string → booléen Python
    # Les inactifs sont conservés avec est_actif=False
    # ---------------------------------------------------------
    df["actif"] = df["actif"].map({
        "True": True, "False": False, "true": True, "false": False,
        "1": True, "0": False,
    }).fillna(True)

    n_inactifs = (~df["actif"]).sum()
    logger.info(
        f"[R3] Produits inactifs conservés (SCD Type 2) : {n_inactifs} "
        f"(ils seront marqués est_actif=False dans dim_produit)"
    )

    logger.info(f"[TRANSFORM produits] Fin : {len(df):,} lignes")
    return df
