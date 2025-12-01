from sqlalchemy import create_engine, text

engine = create_engine("mysql+pymysql://nidec:MaV38f5xsGQp83@162.19.251.55:3306/Charges")

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'Charges'
        AND table_name LIKE 'kpi_%'
    """))
    tables = [row[0] for row in result.fetchall()]
    
    excluded_tables = ['kpi_mac_id', 'kpi_evo', 'kpi_defauts_log']
    
    for table in tables:
        if table not in excluded_tables:
            conn.execute(text(f"TRUNCATE TABLE Charges.{table}"))
            conn.commit()
            print(f"✅ Table vidée : Charges.{table}")
        else:
            print(f"⏭️  Table ignorée : Charges.{table}")

print("\n✅ Nettoyage terminé !")