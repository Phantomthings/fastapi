import pandas as pd
from sqlalchemy import create_engine

# Connexion à la base de données
engine = create_engine("mysql+pymysql://nidec:MaV38f5xsGQp83@162.19.251.55:3306/Charges")

# Récupération de la table sessions
query = "SELECT DISTINCT Site FROM Charges.kpi_sessions WHERE Site IS NOT NULL ORDER BY Site"
df_sites = pd.read_sql(query, con=engine)

# Extraction de la liste des sites
sites = df_sites['Site'].tolist()

# Affichage du nombre de sites
print(f"Nombre de sites trouvés : {len(sites)}")

# Export en fichier texte
with open('liste_sites.txt', 'w', encoding='utf-8') as f:
    for site in sites:
        f.write(f"{site}\n")

print(f"\n✅ Liste exportée dans 'liste_sites.txt'")

# Affichage des 10 premiers sites
print(f"\nAperçu (10 premiers sites) :")
for i, site in enumerate(sites[:10], 1):
    print(f"{i}. {site}")