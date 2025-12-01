from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("mysql+pymysql://nidec:MaV38f5xsGQp83@162.19.251.55:3306/Charges")

top10_query = """
SELECT 
    SUBSTRING(`MAC Address`, 1, 8) AS mac_prefix,
    COUNT(*) AS nombre_de_charges,
    ROUND(SUM(CASE WHEN is_ok = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS taux_reussite
FROM kpi_charges_mac
WHERE Vehicle IS NULL
    AND `MAC Address` IS NOT NULL
    AND `MAC Address` != ''
GROUP BY mac_prefix
ORDER BY nombre_de_charges DESC
LIMIT 10;
"""

insert_query = """
INSERT INTO kpi_mac_id (Mac, nombre_de_charges, taux_reussite)
VALUES (:mac, :count, :taux)
ON DUPLICATE KEY UPDATE 
    nombre_de_charges = VALUES(nombre_de_charges),
    taux_reussite = VALUES(taux_reussite);
"""

try:
    with engine.connect() as conn:
        print("Récupération du TOP 10 des adresses MAC non identifiées...")
        result = conn.execute(text(top10_query))
        top10_macs = result.fetchall()
        
        if not top10_macs:
            print("Aucune adresse MAC non identifiée trouvée.")
        else:
            print(f"\nTOP 10 des préfixes MAC non identifiés :")
            for i, (mac_prefix, count, taux) in enumerate(top10_macs, 1):
                print(f"{i}. {mac_prefix} : {count} charges - {taux}% réussite")
            
            print("\nSuppression des anciennes données...")
            conn.execute(text("TRUNCATE TABLE kpi_mac_id;"))
            conn.commit()
            
            print("\nInsertion des données dans kpi_mac_id...")
            for mac_prefix, count, taux in top10_macs:
                mac_clean = str(mac_prefix).strip().upper()
                
                if mac_clean.startswith("0") and len(mac_clean) > 1 and mac_clean[1] != ":":
                    mac_clean = mac_clean[1:]
                
                print(f"  {mac_prefix} -> {mac_clean} : {count} charges - {taux}% réussite")
                conn.execute(text(insert_query), {"mac": mac_clean, "count": count, "taux": taux})
            conn.commit()
            
            print(f"\n{len(top10_macs)} enregistrements insérés avec succès dans kpi_mac_id.")
            
            print("\nVérification des données insérées :")
            verify_result = conn.execute(text("SELECT * FROM kpi_mac_id ORDER BY nombre_de_charges DESC;"))
            df_verify = pd.DataFrame(verify_result.fetchall(), columns=['Mac', 'nombre_de_charges', 'taux_reussite'])
            print(df_verify)

except Exception as e:
    print(f"Erreur : {e}")
finally:
    engine.dispose()