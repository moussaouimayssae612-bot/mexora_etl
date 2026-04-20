"""
data/generate_data.py
=====================
Génère les 4 fichiers de données Mexora Analytics avec des problèmes
intentionnels à corriger (comme dans le sujet PDF).

Exécution :
    cd mexora_etl/data
    python generate_data.py
"""

import pandas as pd
import numpy as np
import json
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

# ============================================================
# 1. regions_maroc.csv  — référentiel propre
# ============================================================
regions = [
    ("TNG","Tanger",     "Fahs-Anjra",          "Tanger-Tétouan-Al Hoceïma","Nord",  1065601,90000),
    ("TET","Tétouan",    "Tétouan",              "Tanger-Tétouan-Al Hoceïma","Nord",   400000,93000),
    ("CAS","Casablanca", "Casablanca",           "Casablanca-Settat",        "Centre",3752000,20000),
    ("RBA","Rabat",      "Rabat",                "Rabat-Salé-Kénitra",       "Centre", 577827,10000),
    ("FES","Fès",        "Fès",                  "Fès-Meknès",               "Centre",1150000,30000),
    ("MRK","Marrakech",  "Marrakech",            "Marrakech-Safi",           "Sud",    928850,40000),
    ("AGA","Agadir",     "Agadir-Ida-Ou-Tanane", "Souss-Massa",              "Sud",    421844,80000),
    ("OUD","Oujda",      "Oujda-Angad",          "Oriental",                 "Est",    494252,60000),
    ("MEK","Meknès",     "Meknès",               "Fès-Meknès",               "Centre", 632079,50000),
    ("LAA","Laâyoune",   "Laâyoune",             "Laâyoune-Sakia El Hamra",  "Sud",    217732,70000),
]
df_regions = pd.DataFrame(regions, columns=[
    "code_ville","nom_ville_standard","province",
    "region_admin","zone_geo","population","code_postal"
])
df_regions.to_csv("regions_maroc.csv", index=False, encoding="utf-8")
print(f"OK regions_maroc.csv : {len(df_regions)} lignes")

# ============================================================
# 2. produits_mexora.json  — avec problèmes intentionnels
# ============================================================
produits_base = [
    ("P001","iPhone 16 Pro 256Go",   "Electronique","Smartphones",    "Apple",     "Apple MENA",   12999.00,"USA",   True),
    ("P002","Samsung Galaxy S24",    "Electronique","Smartphones",    "Samsung",   "Samsung MA",    9499.00,"Corée", True),
    ("P003","Laptop Dell XPS 15",    "electronique","Ordinateurs",    "Dell",      "Dell MENA",    14500.00,"USA",   True),
    ("P004","Ecouteurs Sony WH-1000","ELECTRONIQUE","Audio",          "Sony",      "Sony MA",       2799.00,"Japon", True),
    ("P005","Montre Casio G-Shock",  "Mode",        "Montres",        "Casio",     "Casio MA",      1299.00,"Japon", True),
    ("P006","Huile d'olive Maasra",  "Alimentation","Huiles",         "Maasra",    "Coop Tanger",     89.00,"Maroc", True),
    ("P007","Couscous Ain Ifrane 1kg","Alimentation","Cereales",      "Ain Ifrane","Ain Ifrane SA",   35.00,"Maroc", True),
    ("P008","T-shirt Zara Homme",    "Mode",        "Vetements",      "Zara",      "Inditex MA",     299.00,"Espagne",True),
    ("P009","Chaussures Nike Air Max","Mode",        "Chaussures",    "Nike",      "Nike MENA",     1499.00,"USA",   True),
    ("P010","Tablet iPad Air",       "Electronique","Tablettes",      "Apple",     "Apple MENA",    7999.00,"USA",   True),
    ("P011","Cafetiere Nespresso",   "Electronique","Electromenager", "Nespresso", "Nestle MA",       None,  "Suisse",False),
    ("P012","Huile d'argan bio",     "Alimentation","Huiles",         "Zine",      "Coop Agadir",    250.00,"Maroc", True),
    ("P013","Robe Kaftan Fassi",     "Mode",        "Vetements",      "Artisanat", "Artisans Fes",   899.00,"Maroc", True),
    ("P014","Dates Medjool 500g",    "Alimentation","Fruits Secs",    "Medfouna",  "Coop Draa",      199.00,"Maroc", True),
    ("P015","Xiaomi Redmi Note 13",  "Electronique","Smartphones",    "Xiaomi",    "Xiaomi MA",     3499.00,"Chine", True),
]

produits_json = {"produits": []}
for p in produits_base:
    produits_json["produits"].append({
        "id_produit":     p[0],
        "nom":            p[1],
        "categorie":      p[2],
        "sous_categorie": p[3],
        "marque":         p[4],
        "fournisseur":    p[5],
        "prix_catalogue": p[6],
        "origine_pays":   p[7],
        "date_creation":  "2023-01-01",
        "actif":          p[8],
    })

with open("produits_mexora.json", "w", encoding="utf-8") as f:
    json.dump(produits_json, f, ensure_ascii=False, indent=2)
print(f"OK produits_mexora.json : {len(produits_json['produits'])} produits")

# ============================================================
# 3. clients_mexora.csv  — avec problèmes intentionnels
# ============================================================
prenoms_h = ["Mohamed","Ahmed","Youssef","Karim","Amine","Hassan","Omar","Khalid","Rachid","Tariq"]
prenoms_f = ["Fatima","Khadija","Zineb","Meryem","Nadia","Sara","Laila","Houda","Sanaa","Imane"]
noms_fam  = ["Benali","Alami","Filali","Berrada","Cherkaoui","Tazi","Idrissi","Benomar","Guerraoui","Lahlou"]
villes    = ["Tanger","Casablanca","Rabat","Fes","Marrakech","Agadir","Tetouan","Oujda","Meknes","Laayoune"]
canaux    = ["organic","paid_search","social_media","referral","email"]

clients = []
for i in range(1, 1001):
    if random.random() < 0.5:
        prenom = random.choice(prenoms_h)
        sexe   = random.choice(["m","1","Homme","male","h"])
    else:
        prenom = random.choice(prenoms_f)
        sexe   = random.choice(["f","0","Femme","female"])

    nom   = random.choice(noms_fam)
    email = f"{prenom.lower()}.{nom.lower()}{i}@gmail.com"
    if random.random() < 0.05:
        email = f"{prenom.lower()}.{nom.lower()}{i}gmail.com"  # sans @

    annee = random.randint(1965, 2005)
    if random.random() < 0.03:
        annee = 1850  # date invalide intentionnelle
    dob = f"{annee}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"

    ville = random.choice(villes)
    if random.random() < 0.15: ville = ville.upper()
    elif random.random() < 0.15: ville = ville.lower()

    date_insc = (datetime(2020,1,1) + timedelta(days=random.randint(0,1460))).strftime("%Y-%m-%d")
    clients.append([
        f"C{i:04d}", nom, prenom, email, dob, sexe, ville,
        f"06{random.randint(10000000,99999999)}", date_insc,
        random.choice(canaux)
    ])

df_clients = pd.DataFrame(clients, columns=[
    "id_client","nom","prenom","email","date_naissance",
    "sexe","ville","telephone","date_inscription","canal_acquisition"
])
doublons = df_clients.sample(30).copy()
doublons["id_client"] = [f"C{random.randint(1001,2000):04d}" for _ in range(30)]
df_clients = pd.concat([df_clients, doublons], ignore_index=True)
df_clients.to_csv("clients_mexora.csv", index=False, encoding="utf-8")
print(f"OK clients_mexora.csv : {len(df_clients)} lignes (dont 30 doublons)")

# ============================================================
# 4. commandes_mexora.csv  — avec problèmes intentionnels
# ============================================================
statuts_raw = ["livre","LIVRE","DONE","annule","KO","en_cours","OK","retourne"]
formats_dates = ["%Y-%m-%d", "%d/%m/%Y", "%b %d %Y"]
produit_ids = [f"P{i:03d}" for i in range(1,16)]
client_ids  = [f"C{i:04d}" for i in range(1,1001)]
variantes = {
    "Tanger":    ["Tanger","tanger","TNG","TANGER","Tnja"],
    "Casablanca":["Casablanca","casablanca","CASA","Casa"],
    "Rabat":     ["Rabat","rabat","RABAT"],
    "Fes":       ["Fes","fes","FES","Fez"],
    "Marrakech": ["Marrakech","marrakech","MRK"],
    "Agadir":    ["Agadir","agadir","AGA"],
    "Tetouan":   ["Tetouan","tetouan","TET"],
    "Oujda":     ["Oujda","oujda","OUD"],
    "Meknes":    ["Meknes","meknes","MEK"],
    "Laayoune":  ["Laayoune","laayoune","LAA"],
}

commandes = []
for i in range(1, 501):
    date_dt = datetime(2022,1,1) + timedelta(days=random.randint(0,1095))
    date_s  = date_dt.strftime(random.choice(formats_dates))
    date_liv= (date_dt + timedelta(days=random.randint(1,10))).strftime("%Y-%m-%d") \
              if random.random() > 0.07 else ""
    qte  = random.randint(1,10)
    prix = round(random.uniform(50, 15000), 2)
    if random.random() < 0.02: prix = -abs(prix)
    if random.random() < 0.03: prix = 0
    if random.random() < 0.02: qte  = -qte
    ville_k = random.choice(list(variantes.keys()))
    ville   = random.choice(variantes[ville_k])
    livreur = f"L{random.randint(1,20):03d}" if random.random() > 0.07 else ""
    commandes.append([
        f"CMD{i:05d}", random.choice(client_ids), random.choice(produit_ids),
        date_s, qte, prix, random.choice(statuts_raw), ville,
        random.choice(["carte","virement","cash","paypal"]),
        livreur, date_liv
    ])

df_cmd = pd.DataFrame(commandes, columns=[
    "id_commande","id_client","id_produit","date_commande","quantite",
    "prix_unitaire","statut","ville_livraison","mode_paiement",
    "id_livreur","date_livraison"
])
doublons_cmd = df_cmd.sample(15).copy()
df_cmd = pd.concat([df_cmd, doublons_cmd], ignore_index=True)
df_cmd.to_csv("commandes_mexora.csv", index=False, encoding="utf-8")
print(f"OK commandes_mexora.csv : {len(df_cmd)} lignes (dont 15 doublons)")
print("\nTous les fichiers generes ! Lancez maintenant : python main.py")
