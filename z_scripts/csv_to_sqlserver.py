import pandas as pd
from sqlalchemy import create_engine
import urllib
from pathlib import Path

# === CONFIGURATION ===
server = r"d-sqlvi-devbi\devbi"
database = "Z_DBT_TUTO"
schema = "source"  # Schéma par défaut
csv_folder = r"z_ressources"

# === CONNEXION À SQL SERVER ===
params = urllib.parse.quote_plus(
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={server};"
    f"Database={database};"
    f"Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

# === TRAITEMENT DE TOUS LES CSV ===
csv_files = list(Path(csv_folder).glob("*.csv"))

if not csv_files:
    print("❌ Aucun fichier CSV trouvé dans le dossier")
else:
    print(f"📂 {len(csv_files)} fichier(s) CSV trouvé(s)\n")
    
    for csv_file in csv_files:
        table_name = csv_file.stem  # Nom du fichier sans extension
        
        print(f"📥 Traitement: {csv_file.name}")
        
        try:
            # Chargement du CSV
            df = pd.read_csv(csv_file)
            print(f"   ├─ {len(df):,} lignes, {len(df.columns)} colonnes")

            df['promotion_sk'] = df['promotion_sk'].fillna(0).astype('Int64')
            
            # Chargement vers SQL Server
            with engine.begin() as connection:
                df.to_sql(
                    name=table_name,
                    con=connection,
                    schema=schema,
                    if_exists="replace",
                    index=False,
                    # chunksize=10_000,
                    method="multi"
                )
            
            print(f"   └─ ✅ Chargé dans {schema}.{table_name}\n")
            
        except Exception as e:
            print(f"   └─ ❌ Erreur: {str(e)}\n")

print("🎉 Traitement terminé!")
