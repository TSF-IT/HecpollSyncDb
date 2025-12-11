# Hecpoll.Sync – Vue d'ensemble

## Architecture et projets
- Projet unique `Hecpoll.Sync` (console .NET 8) orienté batch/ETL.
- Point d'entrée : `Program` orchestre la séquence, partage une connexion SQL et applique la même mécanique de déplacement des fichiers (processing → archive → error).
- Options typées : `HecpollOptions` transporte la chaîne de connexion et les répertoires de travail (base, archive, error, processing).
- Logging maison : `Logger` écrit en JSON dans `logs/` et reflète les messages en console.

## Flux de données principaux
1. **Préparation** : chargement `appsettings.json` + variables d'environnement + éventuels arguments CLI (prioritaires). Initialisation des répertoires `archive/`, `error/`, `processing/`.
2. **Détection des fichiers** (racine du répertoire de travail) par préfixe : `Customers_`, `Contracts_`, `Cards_`, `Drivers_`, `Transaction_`.
3. **Imports de référence** (un fichier = une transaction SQL dédiée) via `RefDataImporter` :
   - `ImportCustomersAsync` : met à jour `CUSTOMERS` existants à partir du CSV SaaS.
   - `ImportContractsAsync` : insère ou met à jour `CONTRACTS` en reliant `CUSTOMERS` si possible.
   - `ImportCardsFromExcelAsync` : met à jour `CARDS` ou stocke les nouvelles entrées dans `CARDS_SAAS_PENDING` depuis Excel.
   - `ImportEmployeesFromDriversAsync` : insère ou met à jour `EMPLOYEES` (drivers) et rattache les clients.
4. **Import transactionnel** : `CsvToShadowImporter.ImportAsync`
   - Charge les tables de référence (stations, terminaux, cuves, clients, contrats, véhicules, cartes).
   - Évite les doublons via la clé `(TransDateTime, TransNumber, DeviceAddress)` déjà en base.
   - Construit des `DataTable` à partir du schéma réel (`SELECT TOP 0`) puis `SqlBulkCopy` vers `TRANSACTIONS` et `PAYMENTS` en conservant les ID générés.
   - Journalise les incohérences de lookup (station, cuve, contrat, véhicule, carte…) plutôt que de bloquer, sauf cas bloquants (station/terminal introuvable).
5. **Cycle de fichiers** : chaque fichier est déplacé vers `processing/` pendant le traitement, puis `archive/` ou `error/` en cas d'échec.

## Classes clés et responsabilités
- `Program` : orchestration, gestion des annulations (Ctrl+C), résolution de configuration, boucle sur les catégories de fichiers.
- `HecpollOptions` : agrège les chemins de travail et la chaîne de connexion.
- `Logger` : sérialisation JSON minimale, thread-safe, echo console.
- `RefDataImporter` : quatre méthodes d'import/refacto, transactions explicites, validations légères et logs en français.
- `CsvToShadowImporter` : pipeline CSV→DataTable→BulkCopy avec déduplication, normalisation (cartes, plaques), résolutions de références (stations, terminaux, cuves, contrats, clients, véhicules, cartes), et contrôles de cohérence.
- `CardEnrichmentService` / `PaymentsRepository` : modules legacy non inclus dans le build (types manquants) mais conservés pour référence.

## Dépendances techniques
- `CsvHelper` pour le streaming CSV.
- `ExcelDataReader` pour les fichiers Excel (provider code pages enregistré).
- `Microsoft.Data.SqlClient` pour l'accès SQL + `SqlBulkCopy`.
- `Microsoft.Extensions.Configuration.*` pour la configuration.

## Configuration et secrets
- Chaîne de connexion recherchée dans l'argument 1 ou `ConnectionStrings:Hecpoll` (`appsettings.json`).
- Répertoire de fichiers : argument 2 ou `Hecpoll:FilesDirectory` (défaut `C:\hecpoll_files`).
- `appsettings.json` du dépôt ne contient plus de secrets réels ; à surcharger localement (ex. `appsettings.Development.json`).

## Contraintes et points d'attention
- **Transactions** : une transaction par fichier d'import; `SqlBulkCopy` utilise `KeepIdentity` pour respecter les ID générés localement.
- **Performances** : DataTable en mémoire (volume proportionnel à la taille du fichier) puis bulk copy. Ok pour des fichiers moyens, à surveiller sur des dumps massifs.
- **Annulation** : `CancellationToken` propagé jusqu'aux accès SQL et aux boucles de lecture pour arrêter proprement.
- **Intégrité** : plusieurs lookups soft (warnings) pour limiter les blocages; seuls les cas critiques (stations/terminaux introuvables, parse dates) déclenchent des exceptions.
- **Endurcissement restant** : pas de tests automatisés, pas de validation de schéma CSV/Excel stricte, pas de retry SQL.
