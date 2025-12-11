# Hecpoll.Sync

Application console .NET 8 pour importer et synchroniser les flux HECPOLL (CSV/Excel) vers SQL Server.

## Vue rapide
- Projet unique : `Hecpoll.Sync` (point d'entree `Program`).
- Packages : CsvHelper, ExcelDataReader, Microsoft.Data.SqlClient, Microsoft.Extensions.Configuration.\*
- Documentation detaillee : voir `DOCS/Hecpoll.Sync-Overview.md`.

## Prerequis
- .NET 8 SDK.
- SQL Server accessible (tables TRANSACTIONS/PAYMENTS, CUSTOMERS, CONTRACTS, EMPLOYEES, CARDS, STATIONS, TERMINALS, TANKS...).
- Droits suffisants pour BULK COPY et transactions.

## Configuration
- Fichier optionnel `appsettings.json` dans `AppContext.BaseDirectory` :
  - `ConnectionStrings:Hecpoll`
  - `Hecpoll:FilesDirectory`
- Variables d'environnement prises en compte.
- Overrides en ligne de commande (prioritaires) :
  1. Argument 1 : chaine de connexion SQL Server.
  2. Argument 2 : repertoire de fichiers (racine des depots CSV/Excel).
- Exemple minimal (`appsettings.json`) :
  ```json
  {
    "ConnectionStrings": {
      "Hecpoll": "Server=.\\SQLEXPRESS;Database=HECPOLL;User Id=hecpoll_user;Password=MotDePasseARemplir;TrustServerCertificate=True;"
    },
    "Hecpoll": {
      "FilesDirectory": "C:\\hecpoll_files"
    }
  }
  ```

## Repertoires de travail
- Base (arg2 ou `Hecpoll:FilesDirectory`, defaut `C:\\hecpoll_files`).
- Sous-dossiers crees automatiquement :
  - `processing` : fichier en cours.
  - `archive` : fichier traite avec succes.
  - `error` : fichier en echec (copie si erreur).

## Flux d'execution
1. Lecture configuration + initialisation des repertoires.
2. Detection des fichiers en racine par prefixe (ordre de traitement) :
   - `Customers_`, `Contracts_`, `Cards_`, `Drivers_`, `Transaction_`.
3. Connexion SQL unique ouverte, une transaction par fichier :
   - Ref data : updates/inserts cibles (CUSTOMERS, CONTRACTS, EMPLOYEES, CARDS pending).
   - Transactions/paiements : lecture CSV, dedup par `(TransDateTime, TransNumber, DeviceAddress)`, creation de `DataTable` puis `SqlBulkCopy` vers TRANSACTIONS/PAYMENTS avec conservation des ID.
4. Deplacement du fichier vers `archive` ou `error` selon le resultat.

## Logs et codes retour
- Logs JSON dans `logs/hecpoll-sync_yyyyMMdd_HHmmss.log` + echo console.
- Codes retour :
  - `0` : succes.
  - `1` : configuration invalide (chaine de connexion manquante, repertoire base absent).
  - `2` : erreur critique lors du traitement.
  - `3` : arret par annulation (Ctrl+C).

## Commandes utiles
- Build : `dotnet build Hecpoll.Sync.csproj`
- Execution type :  
  `Hecpoll.Sync.exe "Server=...;Database=HECPOLL;User Id=...;Password=...;TrustServerCertificate=True;" "C:\\hecpoll_files"`

## Points d'attention
- Volume : l'import transactions charge un DataTable en memoire avant bulk copy; surveiller la taille des CSV massifs.
- Fichiers attendus en racine du repertoire de base (pas dans les sous-dossiers).
- Les tables legacy doivent exister avec les colonnes attendues (aucune migration schema fournie).

## Structure du depot
- `Program.cs` : orchestration principale.
- `Configuration/HecpollOptions.cs` : options d'execution.
- `RefDataImporter.cs` : imports CUSTOMERS / CONTRACTS / EMPLOYEES (Drivers) / CARDS.
- `CsvToShadowImporter.cs` : pipeline TRANSACTIONS/PAYMENTS (CSV -> DataTable -> BulkCopy).
- `Logger.cs` : logger JSON minimal.
- `DOCS/Hecpoll.Sync-Overview.md` : description detaillee de l'architecture et des flux.

## Exemple d'arborescence de fichiers
```
C:\hecpoll_files\
  Customers_20251201.csv
  Contracts_20251201.csv
  Cards_20251201.xlsx
  Drivers_20251201.csv
  Transaction_20251201.csv
  archive\
  error\
  processing\
```

## Exemple de flux
1. `Customers_*.csv` : mise a jour des clients existants (log warning si client inconnu).
2. `Contracts_*.csv` : insertion si client resolu, sinon warning; update sinon.
3. `Cards_*.xlsx` : update carte si connue; sinon insertion dans `CARDS_SAAS_PENDING`.
4. `Drivers_*.csv` : insert/update EMPLOYEES avec rattachement client.
5. `Transaction_*.csv` : deduplication sur `(TransDateTime, TransNumber, DeviceAddress)` puis bulk copy vers `TRANSACTIONS` et `PAYMENTS` avec IDs generes localement.
