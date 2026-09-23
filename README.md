# Kordrel

Kordrel est un orchestrateur local de workflows YAML. Il lance les tâches prêtes avec Tokio, conserve les états, tentatives et logs dans SQLite, et expose un dashboard HTTP en lecture seule.

## Installation

Prérequis : Rust stable (Cargo), et Python 3 pour l'exemple. Sous Linux ou macOS :

Depuis le dossier du dépôt cloné :

```sh
cargo test
cargo install --path .
```

Les commandes du workflow s'exécutent localement **avec les permissions de l'utilisateur qui lance Kordrel**. Chargez uniquement des workflows locaux que vous jugez fiables. L'API ne permet pas de soumettre ni de lancer des commandes.

## Commandes

```sh
kordrel validate examples/workflow.yaml
kordrel simulate examples/workflow.yaml
kordrel run examples/workflow.yaml --max-parallel 2
kordrel status <run-id>
kordrel logs <run-id>
kordrel logs <run-id> by_region
kordrel resume <run-id>
kordrel serve --listen 127.0.0.1:8080
```

`kordrel run` affiche le `run-id`. Les nouvelles installations utilisent `.kordrel/kordrel.db`. Si `.forge/forge.db` existe déjà et que la nouvelle base n'existe pas, Kordrel réutilise l'ancienne base afin de conserver l'historique. `--db CHEMIN` (avant ou après la sous-commande) ou `KORDREL_DB` choisissent explicitement une base ; l'ancienne variable `FORGE_DB` reste acceptée en dernier recours. Lancez `serve` dans un second terminal avec la même base, puis ouvrez `http://127.0.0.1:8080`.

## Démonstration

1. `kordrel validate examples/workflow.yaml`
2. `kordrel simulate examples/workflow.yaml --max-parallel 2` montre `prepare`, puis `by_region` et `by_month` ensemble, puis `validate`, puis `report`.
3. `kordrel serve --listen 127.0.0.1:8080` dans un terminal.
4. `kordrel run examples/workflow.yaml --max-parallel 2` dans un autre. Le rapport se trouve dans `examples/output/report.md`.
5. Regardez les tâches et les logs sur le dashboard ; `kordrel status <run-id>` et `kordrel logs <run-id>` donnent les mêmes données en CLI.
6. Pour tester la reprise, lancez `kordrel run examples/interrupt.yaml`. Quand `attente-interruptible` a démarré (environ une seconde après le `run-id`), pressez Ctrl‑C. Lancez `kordrel resume <run-id>` : la tâche `preparation` reste réussie et le fichier `examples/output/preparation-count.txt` garde la valeur `1`. Supprimez `examples/output/interruption.marker` et `examples/output/preparation-count.txt` avant de refaire cette démonstration.

## Format YAML

Un workflow définit `name`, `max_parallel` (optionnel, défaut 4) et `tasks`. Chaque tâche définit `id`, `command`, `args`, `dependencies`, `workdir`, `timeout_seconds`, `retries` ou `max_attempts`, et `env`. `args` est une liste d'arguments passée directement au processus ; il n'y a pas de shell implicite. `workdir` est relatif au dossier du YAML. `timeout_seconds` accepte de 1 à 604800 secondes. `retries: 2` donne trois tentatives au total ; `max_attempts: 3` donne le même budget. Ne renseignez pas les deux. Le délai entre tentatives commence à 250 ms, double et plafonne à 30 s.

La validation détecte le YAML mal formé, les identifiants répétés, dépendances inconnues ou dupliquées, cycles et valeurs hors bornes. Une tâche ne démarre que si toutes ses dépendances ont réussi. Un échec définitif bloque les descendants ; les autres branches continuent.

## Architecture et reprise

- `workflow.rs` : modèle YAML, validation du graphe et simulation par groupes.
- `scheduler.rs` : ordonnanceur async, processus, parallélisme, timeout, retry et annulation.
- `db.rs` : connexion SQLite sérialisée, transactions et événements persistants.
- `api.rs` et `dashboard.html` : API en lecture seule, SSE et interface.

Les tables `runs`, `tasks`, `attempts`, `logs` et `events` enregistrent les horodatages en millisecondes Unix. SQLite utilise WAL et un délai d'attente sur les verrous. Un fichier verrou par run empêche deux commandes CLI de piloter le même run en même temps. `serve` peut lire la base pendant `run` ; son flux SSE consulte les événements persistants toutes les 500 ms, y compris ceux écrits par un autre processus.

Sur Ctrl‑C, Kordrel annule les tâches actives, tue leurs processus directs, ferme leurs tentatives en `interrupted`, replace ces tâches en `pending` et marque le run `interrupted`. Après un arrêt brutal, `resume` convertit les tentatives encore `running` en `interrupted`. **Une tentative interrompue consomme son budget** : si le budget reste disponible, la tâche est relancée ; sinon elle échoue et ses dépendants sont bloqués. Les tâches `success` ne sont jamais relancées. Les commandes sont celles du snapshot YAML enregistré lors de `run`.

## API

- `GET /api/runs` : 100 exécutions récentes.
- `GET /api/runs/{id}` : workflow, tâches, tentatives et états.
- `GET /api/runs/{id}/tasks` : tâches.
- `GET /api/runs/{id}/logs` et `/api/runs/{id}/logs/{task}` : logs persistés.
- `GET /api/runs/{id}/events` : SSE pour états et nouvelles lignes de log.

## Limites connues

Kordrel tue le processus direct en cas de timeout ou d'arrêt, mais un programme qui crée des processus détachés doit gérer lui-même ses enfants. Le stockage des logs est illimité ; prévoyez une rotation ou une purge externe pour les très gros workflows. Le serveur HTTP n'a pas d'authentification : gardez l'adresse d'écoute sur `127.0.0.1` pour des données sensibles. Les tâches sont exécutées au moins une fois lors d'une interruption brutale ; elles doivent être idempotentes pour une reprise sans effets doublés.
