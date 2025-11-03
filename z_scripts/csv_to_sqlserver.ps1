# ----------------------------------------------------------------------
# Script PowerShell pour charger un fichier CSV dans SQL Server via BCP
# ----------------------------------------------------------------------

# --- 1. PARAMÈTRES DE CONNEXION ET DU FICHIER (À MODIFIER) ---

# Paramètres de la base de données SQL Server
$ServerName = "d-sqlvi-devbi\devbi"  # Ex: "localhost" ou ".\SQLEXPRESS"
$DatabaseName = "Z_DBT_TUTO" # Ex: "AdventureWorksDW"
$TableName = "fact_sales"
$SchemaName = "source" # NOM DU SCHÉMA
$FullTableName = "$SchemaName.$TableName" # Résultat: 'source.fact_sales'
$CSVFile = "C:\Users\jeanloum\Documents\Cours\dbt_tutoriel\z_ressources\fact_sales.csv" # Chemin complet vers le fichier CSV
$Delimiter = "," # Le délimiteur utilisé dans votre CSV
$BatchSize = 1000 # Nombre de lignes à insérer par transaction SQL

# --- Méthode d'authentification ---
# Utilisez 'Sql' pour l'authentification SQL
# Utilisez 'Windows' pour l'authentification Windows (recommandé)
$AuthMethod = "Windows" 
$SqlUsername = "VOTRE_NOM_UTILISATEUR_SQL" # Seulement si $AuthMethod est 'Sql'
$SqlPassword = "VOTRE_MOT_DE_PASSE_SQL"  # Seulement si $AuthMethod est 'Sql'

# --- 2. CONFIGURATION DE LA CONNEXION ---

# Charger l'assembly ADO.NET
Add-Type -AssemblyName System.Data

if ($AuthMethod -eq "Windows") {
    $ConnectionString = "Server=$ServerName;Database=$DatabaseName;Integrated Security=True;"
} else {
    $ConnectionString = "Server=$ServerName;Database=$DatabaseName;User ID=$SqlUsername;Password=$SqlPassword;"
}

$Connection = New-Object System.Data.SqlClient.SqlConnection
$Connection.ConnectionString = $ConnectionString

# --- 3. GESTION DE LA TABLE (CRÉATION ET PURGE) ---

Write-Host "Tentative de connexion à la base de données..."
try {
    $Connection.Open()
    Write-Host "Connexion établie avec succès."

    # --- SQL pour la Création de Table ---
    # Cette étape est cruciale et définit les types de colonnes
    $SchemaSetup = @"
    IF OBJECT_ID('$FullTableName', 'U') IS NOT NULL DROP TABLE $FullTableName;

    CREATE TABLE $FullTableName (
        sales_id INT NOT NULL,
        date_sk INT NOT NULL,
        store_sk INT NOT NULL,
        product_sk INT NOT NULL,
        customer_sk INT NOT NULL,
        promotion_sk INT NULL, -- Important: INT NULL pour accepter les vides du CSV
        quantity INT NOT NULL,
        unit_price DECIMAL(18, 4) NOT NULL,
        gross_amount DECIMAL(18, 4) NOT NULL,
        discount_amount DECIMAL(18, 4) NOT NULL,
        net_amount DECIMAL(18, 4) NOT NULL,
        payment_method NVARCHAR(255) NOT NULL
    );
"@

    $Command = New-Object System.Data.SqlClient.SqlCommand($SchemaSetup, $Connection)
    $Command.ExecuteNonQuery()
    Write-Host "Table $FullTableName purgée et recréée avec le schéma approprié."
}
catch {
    Write-Error "Erreur lors de la gestion de la table SQL: $($_.Exception.Message)"
    exit 1
}

# --- 4. CHARGEMENT ET INSERTION PAR LOT (BATCH) ---

Write-Host "Démarrage du processus de chargement des données..."
$records = Import-Csv -Path $CSVFile -Delimiter $Delimiter
$totalRecords = $records.Count
$insertedCount = 0
$batchCommandText = ""
$batchCount = 0

try {
    # Itération sur chaque enregistrement du CSV
    foreach ($record in $records) {
        
        # Le CSV a des vides pour 'promotion_sk'. On remplace la chaîne vide par 'NULL' pour SQL.
        $promotionSk = $record.promotion_sk
        if ([string]::IsNullOrEmpty($promotionSk)) {
            $promotionSkSql = "NULL"
        } else {
            # Assurez-vous que c'est bien un entier
            $promotionSkSql = [int]$promotionSk
        }

        # Construire la commande INSERT pour l'enregistrement actuel
        # Note: Les chaînes de caractères (payment_method) sont entourées de guillemets simples
        $insertQuery = "
            INSERT INTO $TableName (sales_id, date_sk, store_sk, product_sk, customer_sk, promotion_sk, quantity, unit_price, gross_amount, discount_amount, net_amount, payment_method) 
            VALUES (
                $($record.sales_id),
                $($record.date_sk),
                $($record.store_sk),
                $($record.product_sk),
                $($record.customer_sk),
                $promotionSkSql,
                $($record.quantity),
                $($record.unit_price),
                $($record.gross_amount),
                $($record.discount_amount),
                $($record.net_amount),
                '$($record.payment_method)'
            );
        "
        
        # Ajouter la requête au lot (batch)
        $batchCommandText += $insertQuery
        $batchCount++
        $insertedCount++

        # Exécuter le lot lorsque la taille limite est atteinte ou si c'est le dernier enregistrement
        if ($batchCount -ge $BatchSize -or $insertedCount -eq $totalRecords) {
            
            # Créer une nouvelle transaction
            $Transaction = $Connection.BeginTransaction()
            $BatchCommand = New-Object System.Data.SqlClient.SqlCommand($batchCommandText, $Connection, $Transaction)

            try {
                # Exécuter le lot d'insertions
                $rowsAffected = $BatchCommand.ExecuteNonQuery()
                $Transaction.Commit()
                Write-Host "Lignes insérées : $insertedCount / $totalRecords (Lot de $rowsAffected lignes)." -ForegroundColor Green
            }
            catch {
                $Transaction.Rollback()
                Write-Error "Erreur lors de l'exécution du lot SQL (Ligne $insertedCount): $($_.Exception.Message)"
                # Arrêter le processus en cas d'erreur critique
                throw $_ 
            }
            finally {
                # Réinitialiser pour le prochain lot
                $batchCommandText = ""
                $batchCount = 0
            }
        }
    }
    
    Write-Host "Chargement terminé. $insertedCount lignes insérées dans $TableName." -ForegroundColor Cyan

}
catch {
    Write-Error "Erreur générale dans le processus de chargement: $($_.Exception.Message)"
}
finally {
    if ($Connection -and $Connection.State -eq [System.Data.ConnectionState]::Open) {
        $Connection.Close()
        Write-Host "Connexion SQL fermée."
    }
}
