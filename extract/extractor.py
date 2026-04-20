# =============================================================
#  extract/extractor.py
#  Phase EXTRACT : lit les données brutes sans les modifier
# =============================================================
#
#  RÈGLE D'OR : aucune transformation ici.
#  Tout est lu en string (dtype=str) pour éviter
#  les conversions automatiques de Python qui cachent les erreurs.
#
# =============================================================

import json
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger("mexora_etl")


# -------------------------------------------------------------
# 1. Commandes (CSV)
# -------------------------------------------------------------
def extract_commandes(filepath) -> pd.DataFrame:
    """
    Lit le fichier commandes_mexora.csv et retourne un DataFrame brut.

    Colonnes attendues :
        id_commande, id_client, id_produit, date_commande,
        quantite, prix_unitaire, statut, ville_livraison,
        mode_paiement, id_livreur, date_livraison
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Fichier introuvable : {filepath}")

    df = pd.read_csv(
        filepath,
        encoding="utf-8",
        dtype=str,              # tout en texte → pas de conversion automatique
        keep_default_na=False,  # garder les cellules vides comme ""
    )
    logger.info(f"[EXTRACT] commandes : {len(df):,} lignes lues depuis {filepath.name}")
    return df


# -------------------------------------------------------------
# 2. Produits (JSON)
# -------------------------------------------------------------
def extract_produits(filepath) -> pd.DataFrame:
    """
    Lit le fichier produits_mexora.json.
    Structure attendue : { "produits": [ {...}, {...} ] }
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Fichier introuvable : {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data["produits"]).astype(str)
    logger.info(f"[EXTRACT] produits : {len(df):,} produits lus depuis {filepath.name}")
    return df


# -------------------------------------------------------------
# 3. Clients (CSV)
# -------------------------------------------------------------
def extract_clients(filepath) -> pd.DataFrame:
    """
    Lit le fichier clients_mexora.csv et retourne un DataFrame brut.

    Colonnes attendues :
        id_client, nom, prenom, email, date_naissance,
        sexe, ville, telephone, date_inscription, canal_acquisition
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Fichier introuvable : {filepath}")

    df = pd.read_csv(
        filepath,
        encoding="utf-8",
        dtype=str,
        keep_default_na=False,
    )
    logger.info(f"[EXTRACT] clients : {len(df):,} lignes lues depuis {filepath.name}")
    return df


# -------------------------------------------------------------
# 4. Régions (CSV référentiel propre)
# -------------------------------------------------------------
def extract_regions(filepath) -> pd.DataFrame:
    """
    Lit le fichier regions_maroc.csv.
    C'est le référentiel officiel, il est propre.
    Il servira à harmoniser les noms de villes dans les autres fichiers.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Fichier introuvable : {filepath}")

    df = pd.read_csv(filepath, encoding="utf-8", dtype=str)
    logger.info(f"[EXTRACT] régions : {len(df):,} villes dans le référentiel")
    return df
