# Hecpoll Sync App

Application console .NET 8 pour importer et synchroniser les flux HECPOLL (CSV/Excel) vers SQL Server.

## Structure du depot
- `HecpollConsoleApp/` : projet `Hecpoll.Sync` (point d'entree `Program.cs`).
- `HecpollConsoleApp/DOCS/Hecpoll.Sync-Overview.md` : description detaillee de l'architecture et des flux.
- `HecpollConsoleApp/CHANGELOG_HECPOLL_SYNC_REVIEW.md` : notes de revue.
- `HecpollSyncApp.sln` : solution pour Visual Studio / `dotnet`.

## Prerequis
- .NET 8 SDK.
- SQL Server accessible (tables TRANSACTIONS/PAYMENTS, CUSTOMERS, CONTRACTS, EMPLOYEES, CARDS, STATIONS, TERMINALS, TANKS...).
- Droits suffisants pour BULK COPY et transactions.

## Configuration
- Fichier optionnel `appsettings.json` dans `AppContext.BaseDirectory` (repertoire de l'executable) contenant :
  - `ConnectionStrings:Hecpoll`
  - `Hecpoll:FilesDirectory`
- Variables d'environnement supportees.
- Overrides en ligne de commande (prioritaires) :
  1. Argument 1 : chaine de connexion SQL Server.
  2. Argument 2 : repertoire racine des fichiers (CSV/Excel).

Exemple d'`appsettings.json` minimal :
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

## Utilisation rapide
- Build : `dotnet build HecpollConsoleApp/Hecpoll.Sync.csproj`
- Execution :  
  `dotnet run --project HecpollConsoleApp/Hecpoll.Sync.csproj "Server=...;Database=HECPOLL;User Id=...;Password=...;TrustServerCertificate=True;" "C:\\hecpoll_files"`

## Flux d'execution
1. Lecture configuration et initialisation des repertoires de travail.
2. Detection des fichiers en racine du repertoire source (ordre : `Customers_`, `Contracts_`, `Cards_`, `Drivers_`, `Transaction_`).
3. Connexion SQL unique, une transaction par fichier :
   - Donnees de reference : update/insert dans CUSTOMERS / CONTRACTS / EMPLOYEES (Drivers) / CARDS (pending).
   - Transactions/paiements : lecture CSV, dedup `(TransDateTime, TransNumber, DeviceAddress)`, `DataTable` puis `SqlBulkCopy` vers TRANSACTIONS et PAYMENTS.
4. Deplacement du fichier vers `archive` ou `error` selon le resultat.

## Repertoires de travail attendus
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

## Logs et codes retour
- Logs JSON dans `logs/hecpoll-sync_yyyyMMdd_HHmmss.log` + echo console.
- Codes retour : `0` succes, `1` configuration invalide, `2` erreur critique, `3` arret par annulation.

## Documentation
- Pour une description plus detaillee, voir `HecpollConsoleApp/DOCS/Hecpoll.Sync-Overview.md`.
