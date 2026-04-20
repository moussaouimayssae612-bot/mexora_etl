# =============================================================
#  transform/build_dimensions.py
#  Construction des 5 dimensions + la table de faits
# =============================================================
#
#  Dimensions construites :
#    - dim_temps    : calendrier complet avec jours fériés et Ramadan
#    - dim_produit  : produits avec SCD Type 2
#    - dim_client   : clients avec segment Gold/Silver/Bronze
#    - dim_region   : référentiel géographique du Maroc
#    - dim_livreur  : livreurs inférés depuis les commandes
#
#  Table de faits :
#    - fait_ventes  : une ligne = une commande passée
#
# =============================================================

import logging
import pandas as pd
from datetime import date
from config.settings import (
    DIM_TEMPS_DEBUT, DIM_TEMPS_FIN,
    FERIES_MAROC, RAMADAN_PERIODES,
)

logger = logging.getLogger("mexora_etl")


# =============================================================
# 1. DIM_TEMPS
# =============================================================
def build_dim_temps(date_debut: str = DIM_TEMPS_DEBUT,
                    date_fin:   str = DIM_TEMPS_FIN) -> pd.DataFrame:
    """
    Génère la dimension temporelle complète.
    Chaque ligne = 1 jour.
    Inclut : jours fériés marocains + périodes Ramadan.
    """
    dates = pd.date_range(start=date_debut, end=date_fin, freq="D")

    df = pd.DataFrame({
        "id_date"        : dates.strftime("%Y%m%d").astype(int),  # ex: 20241115
        "jour"           : dates.day,
        "mois"           : dates.month,
        "trimestre"      : dates.quarter,
        "annee"          : dates.year,
        "semaine"        : dates.isocalendar().week.astype(int),
        "libelle_jour"   : dates.strftime("%A"),     # "Monday", "Tuesday"...
        "libelle_mois"   : dates.strftime("%B"),     # "January", "February"...
        "est_weekend"    : dates.dayofweek >= 5,     # Samedi=5, Dimanche=6
        "est_ferie_maroc": dates.strftime("%Y-%m-%d").isin(FERIES_MAROC),
    })

    # Calcul des périodes Ramadan
    df["periode_ramadan"] = False
    for debut, fin in RAMADAN_PERIODES:
        masque = (df["id_date"].astype(str) >= debut.replace("-","")) & \
                 (df["id_date"].astype(str) <= fin.replace("-",""))
        df.loc[masque, "periode_ramadan"] = True

    logger.info(f"[dim_temps] {len(df):,} jours générés ({date_debut} → {date_fin})")
    return df


# =============================================================
# 2. DIM_PRODUIT
# =============================================================
def build_dim_produit(df_produits: pd.DataFrame) -> pd.DataFrame:
    """
    Construit dim_produit.
    Gestion SCD Type 2 : les produits inactifs ont date_fin = aujourd'hui.
    Les produits actifs ont date_fin = '9999-12-31'.
    """
    aujourd_hui = date.today().isoformat()
    df = df_produits.copy().reset_index(drop=True)

    dim = pd.DataFrame({
        "id_produit_sk" : range(1, len(df) + 1),     # Surrogate Key (clé artificielle)
        "id_produit_nk" : df["id_produit"],           # Natural Key (clé source)
        "nom_produit"   : df["nom"],
        "categorie"     : df["categorie"],
        "sous_categorie": df["sous_categorie"],
        "marque"        : df["marque"],
        "fournisseur"   : df["fournisseur"],
        "prix_standard" : pd.to_numeric(df["prix_catalogue"], errors="coerce").round(2),
        "origine_pays"  : df["origine_pays"],
        # SCD Type 2 : date de début et fin de validité
        "date_debut"    : "2020-01-01",
        "date_fin"      : df["actif"].map(
            lambda a: "9999-12-31" if str(a) in ("True","true","1") else aujourd_hui
        ),
        "est_actif"     : df["actif"].map(
            lambda a: True if str(a) in ("True","true","1") else False
        ),
    })

    n_inactifs = (~dim["est_actif"]).sum()
    logger.info(f"[dim_produit] {len(dim)} produits dont {n_inactifs} inactifs (SCD Type 2)")
    return dim


# =============================================================
# 3. DIM_CLIENT
# =============================================================
def build_dim_client(df_clients: pd.DataFrame,
                     df_segments: pd.DataFrame) -> pd.DataFrame:
    """
    Construit dim_client avec le segment calculé depuis les commandes.
    """
    df = df_clients.copy()

    # Joindre les segments (Gold/Silver/Bronze)
    df = df.merge(df_segments[["id_client", "segment_client"]], on="id_client", how="left")
    df["segment_client"] = df["segment_client"].fillna("Bronze")
    df = df.reset_index(drop=True)

    dim = pd.DataFrame({
        "id_client_sk"     : range(1, len(df) + 1),
        "id_client_nk"     : df["id_client"],
        "nom_complet"      : df["nom_complet"],
        "tranche_age"      : df["tranche_age"],
        "sexe"             : df["sexe"],
        "ville"            : df["ville"],
        "segment_client"   : df["segment_client"],
        "canal_acquisition": df["canal_acquisition"],
        # SCD Type 2 sur segment_client
        "date_debut"       : "2020-01-01",
        "date_fin"         : "9999-12-31",
        "est_actif"        : True,
    })

    logger.info(f"[dim_client] {len(dim):,} clients construits")
    return dim


# =============================================================
# 4. DIM_REGION
# =============================================================
def build_dim_region(df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Construit dim_region depuis le référentiel officiel.
    """
    df = df_regions.copy().reset_index(drop=True)

    dim = pd.DataFrame({
        "id_region"    : range(1, len(df) + 1),
        "ville"        : df["nom_ville_standard"],
        "province"     : df["province"],
        "region_admin" : df["region_admin"],
        "zone_geo"     : df["zone_geo"],
        "pays"         : "Maroc",
    })

    logger.info(f"[dim_region] {len(dim)} régions construites")
    return dim


# =============================================================
# 5. DIM_LIVREUR
# =============================================================
def build_dim_livreur(df_commandes: pd.DataFrame) -> pd.DataFrame:
    """
    Construit dim_livreur depuis les identifiants présents dans les commandes.
    Un livreur spécial "Inconnu" (id=-1) est toujours ajouté.
    """
    zones = ["Tanger","Casablanca","Rabat","Fès","Marrakech",
             "Agadir","Tétouan","Oujda","Meknès","Laâyoune"]
    transports = ["Moto", "Camionnette", "Vélo"]

    # Récupérer tous les IDs livreurs uniques (sauf -1)
    livreurs_ids = [
        l for l in df_commandes["id_livreur"].dropna().unique()
        if l != "-1"
    ]

    rows = [{
        "id_livreur_nk"  : "-1",
        "nom_livreur"    : "Inconnu",
        "type_transport" : "Inconnu",
        "zone_couverture": "N/A",
    }]

    for lk in sorted(livreurs_ids):
        num = int(lk.replace("L", "")) if lk.startswith("L") else 0
        rows.append({
            "id_livreur_nk"  : lk,
            "nom_livreur"    : f"Livreur {lk}",
            "type_transport" : transports[num % 3],
            "zone_couverture": zones[(num - 1) % len(zones)],
        })

    dim = pd.DataFrame(rows).reset_index(drop=True)
    dim.insert(0, "id_livreur", range(1, len(dim) + 1))

    logger.info(f"[dim_livreur] {len(dim)} livreurs construits")
    return dim


# =============================================================
# 6. FAIT_VENTES
# =============================================================
def build_fait_ventes(
    df_commandes : pd.DataFrame,
    dim_temps    : pd.DataFrame,
    dim_client   : pd.DataFrame,
    dim_produit  : pd.DataFrame,
    dim_region   : pd.DataFrame,
    dim_livreur  : pd.DataFrame,
) -> pd.DataFrame:
    """
    Construit la table de faits FAIT_VENTES.
    Chaque ligne = 1 commande.

    Mesures :
      - quantite_vendue        → ADDITIVE (on peut sommer)
      - montant_ht             → ADDITIVE
      - montant_ttc            → ADDITIVE
      - cout_livraison         → ADDITIVE
      - delai_livraison_jours  → SEMI-ADDITIVE (sommer n'a pas de sens)
      - remise_pct             → NON-ADDITIVE (taux, à recalculer)
    """
    df = df_commandes.copy()

    # -- Clé date (format YYYYMMDD entier) --------------------
    df["id_date"] = df["date_commande"].dt.strftime("%Y%m%d").astype(int)
    dates_valides = set(dim_temps["id_date"])
    df = df[df["id_date"].isin(dates_valides)]

    # -- Clé produit (via natural key) -------------------------
    map_produit = dict(zip(dim_produit["id_produit_nk"], dim_produit["id_produit_sk"]))
    df["id_produit"] = df["id_produit"].map(map_produit)
    df = df.dropna(subset=["id_produit"])
    df["id_produit"] = df["id_produit"].astype(int)

    # -- Clé client (via natural key) --------------------------
    map_client = dict(zip(dim_client["id_client_nk"], dim_client["id_client_sk"]))
    df["id_client_sk"] = df["id_client"].map(map_client)
    df = df.dropna(subset=["id_client_sk"])
    df["id_client_sk"] = df["id_client_sk"].astype(int)

    # -- Clé region (via nom de ville) -------------------------
    map_region = dict(zip(dim_region["ville"], dim_region["id_region"]))
    df["id_region"] = df["ville_livraison"].map(map_region).fillna(1).astype(int)

    # -- Clé livreur (via natural key) -------------------------
    map_livreur = dict(zip(dim_livreur["id_livreur_nk"], dim_livreur["id_livreur"]))
    df["id_livreur"] = df["id_livreur"].map(map_livreur).fillna(1).astype(int)

    # -- Construction de la table de faits ---------------------
    fait = pd.DataFrame({
        "id_date"               : df["id_date"],
        "id_produit"            : df["id_produit"],
        "id_client"             : df["id_client_sk"],
        "id_region"             : df["id_region"],
        "id_livreur"            : df["id_livreur"],
        "quantite_vendue"       : df["quantite"].astype(int),
        "montant_ht"            : df["montant_ht"].round(2),
        "montant_ttc"           : df["montant_ttc"].round(2),
        "cout_livraison"        : 0.0,
        "delai_livraison_jours" : df["delai_livraison_jours"],
        "remise_pct"            : 0.0,
        "statut_commande"       : df["statut"],
    }).reset_index(drop=True)

    logger.info(f"[fait_ventes] {len(fait):,} lignes construites")
    return fait
