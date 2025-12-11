# CHANGELOG_HECPOLL_SYNC_REVIEW

## 2025-12-11

### Architecture
- Harmonisation des namespaces sous `Hecpoll.Sync` et introduction d'options typées (`HecpollOptions`) pour centraliser chaîne de connexion et chemins de travail.
- `Program` refactoré : gestion de l'annulation (Ctrl+C), mutualisation de la détection de fichiers par préfixe et factorisation des déplacements (processing → archive/error).

### Configuration
- `appsettings.json` nettoyé (chaîne de connexion d'exemple, sans secret réel) et rappel des overrides via arguments/variables d'environnement.
- Répertoires de travail créés au démarrage si nécessaires (archive/error/processing).

### Accès aux données / robustesse
- Transactions ouvertes en async (`BeginTransactionAsync`) et `CancellationToken` propagé aux accès SQL et aux boucles de lecture (CSV/Excel/SQL).
- Imports de référence (`RefDataImporter`) : mises à jour/insertion encapsulées par fichier, logs plus explicites sur les références manquantes, commits asynchrones.
- Pipeline transactions/paiements (`CsvToShadowImporter`) : déduplication renforcée, transaction async, commit async, avertissements contextualisés lors des lookups.

### Logging
- Messages de log en français clarifiés; logger JSON conservé et utilisé par l'orchestrateur.

### Documentation
- Ajout de `DOCS/Hecpoll.Sync-Overview.md` (architecture, flux, dépendances, contraintes).

### Dette / risques connus
- `CardEnrichmentService` et `PaymentsRepository` restent hors build (dépendances/types manquants) et mériteraient une passe dédiée si réactivés.
- Pas de couverture de tests automatisés; consommation mémoire liée aux `DataTable` à surveiller sur très gros CSV/Excel.
- Les schémas CSV/Excel ne sont pas validés formellement (hypothèse de fichiers conformes aux flux HECPOLL SaaS).
