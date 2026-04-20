# =============================================================
#  transform/clean_clients.py
#  Nettoyage des clients + calcul des segments Gold/Silver/Bronze
# =============================================================
#
#  Règles appliquées :
#    R1 - Déduplication sur email normalisé
#    R2 - Standardisation du sexe → m / f / inconnu
#    R3 - Validation des dates de naissance (âge entre 16 et 100 ans)
#    R4 - Calcul de la tranche d'âge
#    R5 - Validation du format email (regex)
#    R6 - Harmonisation des villes (même référentiel que commandes)
#
# =============================================================

import re
import logging
import pandas as pd
from datetime import date, timedelta
from config.settings import MAPPING_SEXE, SEGMENT_GOLD, SEGMENT_SILVER

logger = logging.getLogger("mexora_etl")

# Regex pour valider un email
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def transform_clients(df: pd.DataFrame, df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les données clients.
    Retourne un DataFrame propre avec les champs normalisés.
    """
    df = df.copy()
    n_initial = len(df)
    logger.info(f"[TRANSFORM clients] Début : {n_initial:,} lignes")

    # ---------------------------------------------------------
    # R1 - Déduplication sur email normalisé
    # On garde la ligne avec la date d'inscription la plus récente
    # ---------------------------------------------------------
    df["email_norm"] = df["email"].str.lower().str.strip()
    df["date_inscription"] = pd.to_datetime(df["date_inscription"], errors="coerce")
    n_avant = len(df)
    df = df.sort_values("date_inscription").drop_duplicates(
        subset=["email_norm"], keep="last"
    )
    logger.info(f"[R1] Doublons email supprimés : {n_avant - len(df)}")

    # ---------------------------------------------------------
    # R2 - Standardisation du sexe
    # "m", "h", "1", "Homme", "male" → "m"
    # "f", "0", "Femme", "female"    → "f"
    # tout le reste                   → "inconnu"
    # ---------------------------------------------------------
    df["sexe"] = (
        df["sexe"].str.lower().str.strip()
        .map(MAPPING_SEXE)
        .fillna("inconnu")
    )
    n_inconnus = (df["sexe"] == "inconnu").sum()
    logger.info(f"[R2] Sexe non reconnu → 'inconnu' : {n_inconnus}")

    # ---------------------------------------------------------
    # R3 - Validation des dates de naissance
    # Âge valide : entre 16 et 100 ans
    # ---------------------------------------------------------
    aujourd_hui = pd.Timestamp(date.today())
    df["date_naissance"] = pd.to_datetime(df["date_naissance"], errors="coerce")
    df["age"] = ((aujourd_hui - df["date_naissance"]).dt.days // 365).astype("Int64")

    masque_invalide = (df["age"] < 16) | (df["age"] > 100)
    n_invalides = masque_invalide.sum()
    df.loc[masque_invalide, ["date_naissance", "age"]] = [pd.NaT, pd.NA]
    logger.info(f"[R3] Dates de naissance invalides (âge hors 16-100 ans) : {n_invalides}")

    # ---------------------------------------------------------
    # R4 - Calcul de la tranche d'âge
    # ---------------------------------------------------------
    df["tranche_age"] = pd.cut(
        df["age"].fillna(0).astype(int),
        bins   = [0,  18,  25,  35,  45,  55,  65, 200],
        labels = ["<18","18-24","25-34","35-44","45-54","55-64","65+"],
    ).astype(str).replace("nan", "Inconnue")

    # ---------------------------------------------------------
    # R5 - Validation du format email
    # Les emails invalides sont mis à None
    # ---------------------------------------------------------
    masque_email_invalide = ~df["email"].str.match(EMAIL_REGEX, na=False)
    n_emails_invalides = masque_email_invalide.sum()
    df.loc[masque_email_invalide, "email"] = None
    logger.info(f"[R5] Emails invalides → None : {n_emails_invalides}")

    # ---------------------------------------------------------
    # R6 - Harmonisation des villes
    # ---------------------------------------------------------
    mapping_villes = {}
    for _, row in df_regions.iterrows():
        nom_std = row["nom_ville_standard"]
        code    = row["code_ville"]
        for v in [nom_std, nom_std.upper(), code, code.lower()]:
            mapping_villes[v.strip().lower()] = nom_std

    df["ville"] = (
        df["ville"].str.strip().str.lower()
        .map(mapping_villes)
        .fillna("Non renseignée")
    )

    # Construction du nom complet
    df["nom_complet"] = (
        df["prenom"].str.strip() + " " + df["nom"].str.strip()
    ).str.title()

    n_final = len(df)
    logger.info(
        f"[TRANSFORM clients] Fin : {n_initial:,} → {n_final:,} lignes "
        f"(supprimées : {n_initial - n_final:,})"
    )
    return df


def calculer_segments_clients(df_commandes: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le segment de chaque client selon son CA des 12 derniers mois :
      Gold   : CA >= 15 000 MAD
      Silver : CA >=  5 000 MAD
      Bronze : CA <   5 000 MAD

    Seules les commandes avec statut='livré' sont prises en compte.
    """
    date_limite = pd.Timestamp(date.today() - timedelta(days=365))

    df_recents = df_commandes[
        (df_commandes["date_commande"] >= date_limite) &
        (df_commandes["statut"] == "livré")
    ].copy()

    if df_recents.empty:
        logger.warning("[SEGMENT] Aucune commande récente → tous les clients classés Bronze")
        ids = df_commandes["id_client"].unique()
        return pd.DataFrame({
            "id_client"      : ids,
            "segment_client" : "Bronze",
            "ca_12m"         : 0.0,
        })

    # Calcul du CA par client
    ca = (
        df_recents.groupby("id_client")["montant_ttc"]
        .sum()
        .reset_index()
        .rename(columns={"montant_ttc": "ca_12m"})
    )

    def segmenter(ca_val):
        if ca_val >= SEGMENT_GOLD:   return "Gold"
        if ca_val >= SEGMENT_SILVER: return "Silver"
        return "Bronze"

    ca["segment_client"] = ca["ca_12m"].apply(segmenter)

    logger.info(
        f"[SEGMENT] Gold={( ca['segment_client']=='Gold').sum()}  "
        f"Silver={(ca['segment_client']=='Silver').sum()}  "
        f"Bronze={(ca['segment_client']=='Bronze').sum()}"
    )
    return ca[["id_client", "segment_client", "ca_12m"]]
