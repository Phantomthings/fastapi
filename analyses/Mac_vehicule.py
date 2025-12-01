from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("mysql+pymysql://nidec:MaV38f5xsGQp83@162.19.251.55:3306/Charges")

data = {
    "MAC": [
        "18:4C:AE", "7C:BC:84", "28:0F:EB", "30:49:50", "18:87", 
        "A4:53:EE", "B0:52:00:DB:D1", "B0:52:00:00:03", "18:23", "7D:FA",
        "C8:6C:70", "CC:88:26", "DC:44", "FC:A4:7A", "44:42:2F",
        "8F:21:55", "A0:E2", "BD:F5:64", "98:ED:5C", "48:C5:8D",
        "E5:FA:F4", "F0:7F:C0", "F0:7F:C2", "80:0A:80:20", "4E:77:E7",
        "E0:0E:E1", "90:12:A1", "C0:FB:B6", "16:81"
    ],
    "Vehicle": [
        "Renault Mégane e-tech",
        "Renault Zoé / Nissan",
        "Renault Scénic / Peugeot e-Riffter",
        "Dacia Spring",
        "Citroën DS 3 / Peugeot e-2008 / Peugeot e-208",
        "Citroën e-C4 / Citroën e-Berlingo",
        "Opel Mokka",
        "Opel e-Corsa",
        "Fiat 500 / Ford e-Mach (>Fiat 500)",
        "Groupe - VW Audi Skoda Cupra",
        "Mercedes EQC",
        "Mercedes EQA",
        "Tesla Modèle 3 / Y / S / X",
        "Mazda CX30 / MG ZS EV / MG 4",
        "Tesla Modèle 3 / Y / S / X",
        "Tesla Modèle 3 / Y / S / X",
        "Tesla Modèle 3 / Y / S / X",
        "Tesla Modèle 3 / Y / S / X",
        "Tesla Modèle 3 / Y / S / X",
        "Volvo C40",
        "Volvo XC40",
        "MINI SE / BMW i3 (>MINI SE)",
        "Porsche Taycan",
        "MG Marvel",
        "Kia EV6 / Kia e Niro",
        "Hyundai Kona",
        "Hyundai Ioniq 5 / Ioniq 6",
        "BYD ATTO 3/BYD Han",
        "RENAULT TRUCKS"
    ]
}

df = pd.DataFrame(data)

df.to_sql(name="mac_lookup", con=engine, schema="Charges", if_exists="replace", index=False)

print("'Charges.mac_lookup' peuplée avec succès.")