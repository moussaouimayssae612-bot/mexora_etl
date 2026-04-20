# =============================================================
#  utils/logger.py
#  Configure le système de logs pour tout le pipeline
# =============================================================

import logging
import sys
from datetime import datetime
from pathlib import Path


def get_logger(name: str = "mexora_etl") -> logging.Logger:
    """
    Retourne un logger avec 2 sorties :
      - Fichier : logs/etl_YYYYMMDD_HHMMSS.log  (tout les niveaux)
      - Console : affiche INFO et au-dessus
    """
    logger = logging.getLogger(name)

    # Eviter les doublons si importé plusieurs fois
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # Format des messages
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler fichier
    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Handler console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger
