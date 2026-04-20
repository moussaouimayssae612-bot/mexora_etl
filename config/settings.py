# =============================================================
#  config/settings.py
#  Tous les paramètres centraux du projet Mexora ETL
# =============================================================

from pathlib import Path

# ── Chemins ───────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR  = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Fichiers sources ──────────────────────────────────────────
SOURCE_FILES = {
    "commandes" : DATA_DIR / "commandes_mexora.csv",
    "produits"  : DATA_DIR / "produits_mexora.json",
    "clients"   : DATA_DIR / "clients_mexora.csv",
    "regions"   : DATA_DIR / "regions_maroc.csv",
}

# ── PostgreSQL  (modifie user et password selon ton pgAdmin) ──
DB_CONFIG = {
    "host"     : "localhost",
    "port"     : 5432,
    "database" : "mexora_dwh",
    "user"     : "postgres",
    "password" : "mayssae123",
}
DB_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

SCHEMA_DWH       = "dwh_mexora"
SCHEMA_STAGING   = "staging_mexora"
SCHEMA_REPORTING = "reporting_mexora"

# ── Règles métier ─────────────────────────────────────────────
SEGMENT_GOLD   = 15000
SEGMENT_SILVER =  5000
TVA_MAROC      = 0.20
DELAI_RETARD   = 3

DIM_TEMPS_DEBUT = "2020-01-01"
DIM_TEMPS_FIN   = "2026-12-31"

FERIES_MAROC = [
    "2022-01-01","2022-01-11","2022-05-01","2022-07-30",
    "2022-08-14","2022-11-06","2022-11-18",
    "2023-01-01","2023-01-11","2023-05-01","2023-07-30",
    "2023-08-14","2023-11-06","2023-11-18",
    "2024-01-01","2024-01-11","2024-05-01","2024-07-30",
    "2024-08-14","2024-11-06","2024-11-18",
]

RAMADAN_PERIODES = [
    ("2022-04-02","2022-05-01"),
    ("2023-03-22","2023-04-20"),
    ("2024-03-10","2024-04-09"),
    ("2025-03-01","2025-03-29"),
]

MAPPING_STATUTS = {
    "livré":"livré","livre":"livré","LIVRE":"livré","DONE":"livré","delivered":"livré",
    "annulé":"annulé","annule":"annulé","KO":"annulé","cancelled":"annulé",
    "en_cours":"en_cours","OK":"en_cours","pending":"en_cours",
    "retourné":"retourné","retourne":"retourné","returned":"retourné",
}

MAPPING_SEXE = {
    "m":"m","h":"m","1":"m","homme":"m","male":"m",
    "f":"f","0":"f","femme":"f","female":"f",
}
