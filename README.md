<<<<<<< HEAD
# Mexora Analytics — Pipeline ETL & Data Warehouse

Projet ETL complet pour la marketplace e-commerce **Mexora** basée à Tanger.
Ce projet construit un entrepôt de données (Data Warehouse) depuis zéro, depuis
l'extraction des données brutes jusqu'au chargement dans PostgreSQL.

---

## Structure du projet

```
mexora_etl/
│
├── config/
│   ├── __init__.py
│   └── settings.py          ← paramètres : DB, chemins, règles métier
│
├── data/
│   ├── generate_data.py     ← génère les 4 fichiers de données
│   ├── commandes_mexora.csv ← après génération
│   ├── produits_mexora.json ← après génération
│   ├── clients_mexora.csv   ← après génération
│   └── regions_maroc.csv    ← après génération
│
├── extract/
│   ├── __init__.py
│   └── extractor.py         ← Phase EXTRACT : lit les fichiers bruts
│
├── transform/
│   ├── __init__.py
│   ├── clean_commandes.py   ← nettoie les commandes (7 règles)
│   ├── clean_clients.py     ← nettoie les clients + segments
│   ├── clean_produits.py    ← nettoie les produits (SCD Type 2)
│   └── build_dimensions.py  ← construit les 5 dims + table de faits
│
├── load/
│   ├── __init__.py
│   └── loader.py            ← Phase LOAD : PostgreSQL ou CSV
│
├── utils/
│   ├── __init__.py
│   └── logger.py            ← système de logs
│
├── sql/
│   ├── create_dwh.sql       ← crée les schémas, tables, index, vues
│   ├── check_integrity.sql  ← vérifie l'intégrité du DWH
│   └── requetes_analytiques.sql ← 5 requêtes pour le dashboard
│
├── logs/                    ← créé automatiquement
├── output_dwh/              ← créé automatiquement (mode démo CSV)
│
├── main.py                  ← point d'entrée du pipeline
└── requirements.txt
```

---

## Installation

### 1. Prérequis
- Python 3.10 ou supérieur
- PostgreSQL + pgAdmin (pour le mode production)

### 2. Installer les dépendances Python

```bash
# Dans le dossier mexora_etl/
pip install -r requirements.txt
```

### 3. Générer les données de test

```bash
cd data/
python generate_data.py
cd ..
```

---

## Lancer le pipeline

### Mode démo (sans PostgreSQL — recommandé pour commencer)

```bash
python main.py
```

Les tables DWH sont sauvegardées en CSV dans le dossier `output_dwh/`.

### Mode production (avec PostgreSQL)

**Étape 1 — Créer la base dans pgAdmin :**
1. Ouvre pgAdmin
2. Clic droit sur "Databases" → "Create" → "Database"
3. Nom : `mexora_dwh`
4. Clique "Save"

**Étape 2 — Créer les tables SQL :**
1. Dans pgAdmin, sélectionne la base `mexora_dwh`
2. Clique sur "Query Tool" (icône SQL)
3. Ouvre et colle le contenu de `sql/create_dwh.sql`
4. Clique sur le bouton "Run" (▶)

**Étape 3 — Configurer la connexion dans le code :**

Ouvre `config/settings.py` et modifie ces lignes :
```python
DB_CONFIG = {
    "host"     : "localhost",
    "port"     : 5432,
    "database" : "mexora_dwh",
    "user"     : "postgres",     # ← ton user pgAdmin
    "password" : "postgres",     # ← ton mot de passe pgAdmin
}
```

**Étape 4 — Lancer le pipeline avec PostgreSQL :**
```bash
python main.py --pg
```

**Étape 5 — Vérifier l'intégrité :**
Dans pgAdmin Query Tool, exécute `sql/check_integrity.sql`

---

## Supprimer une ancienne base et recommencer à zéro

Si tu as déjà une base `mexora_dwh` d'un ancien projet et tu veux repartir
de zéro, voici comment faire :

### Option A — Supprimer seulement les schémas (garde la base)
Dans pgAdmin Query Tool sur `mexora_dwh` :
```sql
DROP SCHEMA IF EXISTS dwh_mexora       CASCADE;
DROP SCHEMA IF EXISTS staging_mexora   CASCADE;
DROP SCHEMA IF EXISTS reporting_mexora CASCADE;
```
Puis ré-exécute `sql/create_dwh.sql`.

### Option B — Supprimer toute la base
1. Dans pgAdmin, clic droit sur `mexora_dwh` → "Delete/Drop"
2. Coche "Drop with prejudice" pour forcer si des connexions sont actives
3. Recrée la base : clic droit sur "Databases" → "Create"
4. Ré-exécute `sql/create_dwh.sql`

### Option C — Via le terminal psql
```bash
psql -U postgres -c "DROP DATABASE IF EXISTS mexora_dwh;"
psql -U postgres -c "CREATE DATABASE mexora_dwh;"
psql -U postgres -d mexora_dwh -f sql/create_dwh.sql
```

---

## Résultats attendus après exécution

```
PIPELINE TERMINÉ
  dim_temps    : 2 557 lignes  (un jour = une ligne, 2020-2026)
  dim_produit  :    15 lignes  (dont 1 inactif SCD Type 2)
  dim_client   : 1 000 lignes
  dim_region   :    10 lignes
  dim_livreur  :    21 lignes
  fait_ventes  :   457 lignes  (après nettoyage des 515 brutes)
```

---

## Règles de transformation appliquées

| Fichier       | Règle | Description                                    | Impact |
|---------------|-------|------------------------------------------------|--------|
| Commandes     | R1    | Suppression doublons sur id_commande           | ~15    |
| Commandes     | R2    | Standardisation dates formats mixtes           | ~0     |
| Commandes     | R3    | Harmonisation noms de villes                   | ~150   |
| Commandes     | R4    | Standardisation statuts (OK→en_cours, etc.)    | ~0     |
| Commandes     | R5    | Suppression quantite <= 0                      | ~12    |
| Commandes     | R6    | Suppression prix = 0 (commandes test)          | ~22    |
| Commandes     | R7    | Livreurs manquants → "-1"                      | ~41    |
| Clients       | R1    | Déduplication email                            | ~30    |
| Clients       | R2    | Standardisation sexe                           | ~0     |
| Clients       | R3    | Validation dates de naissance (16-100 ans)     | ~27    |
| Clients       | R5    | Validation format email                        | ~51    |
| Produits      | R1    | Normalisation casse catégories                 | ~3     |
| Produits      | R2    | Prix NULL → médiane catégorie                  | ~1     |
| Produits      | R3    | Inactifs conservés (SCD Type 2)                | ~1     |

---

## Technologies utilisées

| Technologie  | Rôle                              |
|--------------|-----------------------------------|
| Python 3.10+ | Langage principal du pipeline ETL |
| pandas       | Manipulation et transformation    |
| SQLAlchemy   | Connexion et ORM PostgreSQL       |
| psycopg2     | Driver PostgreSQL pour Python     |
| PostgreSQL   | Data Warehouse                    |
| pgAdmin      | Interface graphique PostgreSQL    |
=======
# mexora_etl
>>>>>>> 3bdb0256e10d95040859c7694252e7c94bc8531f
