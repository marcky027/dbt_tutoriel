import pandas as pd
from sqlalchemy import create_engine
import urllib
import os

# --- 1. PARAMÈTRES DE CONNEXION ET DU FICHIER (À MODIFIER) ---

# Configuration du serveur SQL Server
SERVER_NAME = r"d-sqlvi-devbi\devbi"  # Ex: 'localhost' ou '.\SQLEXPRESS'
DATABASE_NAME = "Z_DBT_TUTO" # Ex: 'AdventureWorksDW'
SCHEMA_NAME = 'source' # Schéma cible dans la base de données
TABLE_NAME = 'fact_sales'
FULL_TABLE_NAME = f'{SCHEMA_NAME}.{TABLE_NAME}'

# Fichier CSV
CSV_FILE_PATH = r"C:\Users\jeanloum\Documents\Cours\dbt_tutoriel\z_ressources\fact_sales.csv"
CSV_DELIMITER = ','

# Authentification 
# METHOD = 'windows' (Recommandé) ou 'sql'
AUTH_METHOD = 'windows' 
# SQL_USER = 'VOTRE_NOM_UTILISATEUR_SQL'
# SQL_PASSWORD = 'VOTRE_MOT_DE_PASSE_SQL'

# --- 2. FONCTION DE CHARGEMENT PRINCIPALE ---

def load_data_to_sqlserver():
    """
    Charge les données du fichier CSV dans la table SQL Server spécifiée.
    """
    print(f"Démarrage du chargement des données vers {FULL_TABLE_NAME}...")

    # --- Étape 2.1 : Lecture et préparation des données ---
    try:
        # Lecture du CSV avec pandas
        df = pd.read_csv(CSV_FILE_PATH, sep=CSV_DELIMITER)
        print(f"Fichier lu. {len(df)} lignes trouvées.")

        # Vérification et conversion des types de colonnes pour correspondre au schéma SQL
        # La colonne 'promotion_sk' doit être gérée pour les valeurs NULL (NaN)
        
        # Remplacement des chaînes vides ou espaces par NaN, puis conversion en type entier
        # (les valeurs NaN/None seront correctement traitées comme NULL par SQLAlchemy)
        df['promotion_sk'] = df['promotion_sk'].replace('', pd.NA, regex=True).astype('Int64')
        
        # S'assurer que les colonnes numériques sont au bon format
        df['quantity'] = df['quantity'].astype(int)
        df['unit_price'] = df['unit_price'].astype(float)
        df['gross_amount'] = df['gross_amount'].astype(float)
        df['discount_amount'] = df['discount_amount'].astype(float)
        df['net_amount'] = df['net_amount'].astype(float)

        print("Préparation des données terminée (gestion des NULL et des types).")
        
    except FileNotFoundError:
        print(f"ERREUR: Le fichier CSV n'a pas été trouvé à l'emplacement: {CSV_FILE_PATH}")
        return
    except Exception as e:
        print(f"ERREUR lors de la lecture/préparation du CSV: {e}")
        return

    # --- Étape 2.2 : Configuration de la connexion SQLAlchemy ---
    
    if AUTH_METHOD == 'windows':
        # Connexion Windows (recommandé)
        # Utilisation de ODBC Driver 17 for SQL Server (doit être installé)
        # Driver recommandé : ODBC Driver 17 for SQL Server
        params = urllib.parse.quote_plus(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};Trusted_Connection=yes'
        )
        SQLALCHEMY_DATABASE_URL = f"mssql+pyodbc:///?odbc_connect={params}"
    
    elif AUTH_METHOD == 'sql':
        # Connexion SQL Server
        # NOTE: Ne pas utiliser cette méthode pour l'authentification Windows!
        SQLALCHEMY_DATABASE_URL = (
            f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SERVER_NAME}/{DATABASE_NAME}?"
            f"driver=ODBC Driver 17 for SQL Server"
        )
    else:
        print("ERREUR: Méthode d'authentification non valide. Utilisez 'windows' ou 'sql'.")
        return

    # --- Étape 2.3 : Création de l'Engine et insertion ---
    
    try:
        engine = create_engine(SQLALCHEMY_DATABASE_URL)
        print("Connexion à SQL Server établie avec SQLAlchemy.")

        # L'option 'if_exists="replace"' garantit que l'ancienne table est purgée et que 
        # le schéma est recréé selon les types de données de Pandas (proche du schéma souhaité).
        # L'option 'index=False' évite d'ajouter une colonne d'index inutile.
        # L'option 'schema=SCHEMA_NAME' force l'insertion dans le bon schéma.

        print(f"Chargement des données dans la table {FULL_TABLE_NAME}...")
        
        df.to_sql(
            name=TABLE_NAME, 
            con=engine, 
            schema=SCHEMA_NAME,
            if_exists='replace', # Remplacer la table existante
            index=False,         # Ne pas insérer l'index du DataFrame
            chunksize=5000       # Insertion par lots pour la performance
        )
        
        print(f"\nSUCCÈS: {len(df)} lignes insérées dans {FULL_TABLE_NAME}.")
        
    except Exception as e:
        print(f"\nÉCHEC: Une erreur s'est produite lors de l'insertion dans la base de données.")
        print(f"Veuillez vérifier les paramètres de connexion, l'installation du driver ODBC 17, et le fichier CSV.")
        print(f"Détail de l'erreur: {e}")
        # En cas d'échec, vous pouvez décommenter les lignes ci-dessous pour le débogage
        # print(df.head())
        # print(df.dtypes)

# --- 3. EXÉCUTION ---
if __name__ == "__main__":
    # Vérification des dépendances
    try:
        import pyodbc
    except ImportError:
        print("ERREUR: La librairie 'pyodbc' est manquante. Veuillez l'installer avec : pip install pyodbc")
    
    # Exécution du chargement
    load_data_to_sqlserver()
