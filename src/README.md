# Documentation détaillée des scripts

Détails qui ne tiennent pas dans le README (qui doit rester sur une page). Bucket S3 : `s3://<namespace>/dvf` (`user-v-mellone-ece` pour l'équipe).

## Pipeline (`src/run_pipeline.sh`)

Lance `01` → `02` → `03` → `04` à la suite, depuis la racine du dépôt (`bash src/run_pipeline.sh`). Idempotent : chaque étape réécrit sa sortie en mode `overwrite`, donc relancer tout ou une partie du pipeline ne crée jamais de doublons et ne modifie jamais le brut sur S3. Le journal complet (stdout/stderr de l'exécution) est aussi déposé sur `dvf/logs/<nom_du_fichier>.log`, pour garder une trace même quand le Pod qui a tourné a disparu depuis.

Point d'attention : la variable `SPARK_LOCAL_DIRS` est forcée vers un dossier du répertoire courant (`.spark-tmp`) plutôt que `/tmp`, pour que les fichiers temporaires de Spark (shuffle, cache) soient écrits sur le PVC du service et non sur le disque éphémère du node Kubernetes. Sinon le Pod peut être expulsé pour dépassement d'« ephemeral-storage ».

## `01_ingest_raw.sh`

- Lit : l'URL `https://files.data.gouv.fr/geo-dvf/latest/csv/<YEAR>/departements/<DEP>.csv.gz` (data.gouv.fr).
- Écrit : `dvf/raw/year=<YEAR>/dep=<DEP>/dvf_<DEP>_<YEAR>.csv` (brut, jamais modifié ensuite) et `dvf/ref/stations_ski_05.csv`.
- Lancer : `bash src/01_ingest_raw.sh`. Variables optionnelles : `YEAR`, `DEP`, `BUCKET` (sinon `KUBERNETES_NAMESPACE`).
- Attention : télécharge aussi la liste des stations de ski (`src/stations_ski_05.csv`), qui est une donnée de référence faite par l'équipe, pas une donnée brute DVF.

## `02_silver.py`

- Lit : `dvf/raw/year=2024/dep=05/*.csv`.
- Écrit : `dvf/silver/ventes_appartements/` (Parquet, une ligne = une vente d'un seul appartement avec prix au m²) et `dvf/silver/rapport_qualite/` (CSV, nombre de ventes retirées à chaque règle).
- Lancer : `cd src && python 02_silver.py`.
- 8 règles de qualité appliquées dans l'ordre (contient un appartement, nature = Vente, un seul appartement dans la vente, pas de maison ou de local dans la vente, prix renseigné, surface renseignée et positive, prix au m² entre 500 € et 20 000 €). Chaque règle compte ce qu'elle retire, affiché dans `rapport_qualite`.
- Changement de grain : le CSV brut a une ligne par bien et par mutation. Le script regroupe par `id_mutation` pour obtenir une ligne par vente.

## `03_gold.py`

- Lit : `dvf/silver/ventes_appartements/` et `dvf/ref/stations_ski_05.csv`.
- Écrit : `dvf/gold/prix_m2_par_tranche/` (Parquet) et `dvf/gold/prix_m2_par_tranche_csv/` (même résultat en CSV).
- Lancer : `cd src && python 03_gold.py`.
- Jointure en broadcast (la table des stations est petite, copiée sur chaque executor) entre les ventes et la liste des stations. Résultat : médiane, 1er et 3e quartile du prix au m² et surface médiane, par tranche de surface (< 30 m², 30-50 m², ≥ 50 m²) et par groupe (station ou reste).

## `04_verify_duckdb.py`

- Lit directement le CSV brut sur S3 (`dvf/raw/...`) et la référence des stations, sans passer par le Parquet silver ou gold.
- Recalcule en SQL (DuckDB) le même résultat que `02_silver.py` + `03_gold.py`, puis le compare au gold écrit par Spark (`dvf/gold/prix_m2_par_tranche`).
- Lancer : `python src/04_verify_duckdb.py` (option `--base data` pour une exécution locale sur un dossier qui imite le bucket, `--tolerance` pour l'écart toléré en euros).
- Affiche `RÉSULTAT : OK` si les deux moteurs tombent d'accord, `ÉCART détecté` sinon (code de sortie 1).

## `05_mesure_formats.py`

- Compare CSV et Parquet pour le même CSV brut : taille sur S3 et temps de 3 requêtes types (lecture complète, une colonne, un filtre), médiane de 5 essais, avec DuckDB.
- Lit : `dvf/raw/year=2024/dep=05/dvf_05_2024.csv`. Écrit : `dvf/mesures/dvf_05_2024.parquet` et `dvf/mesures/mesure_formats.csv`.
- Lancer : `python src/05_mesure_formats.py`.

## `06_iceberg.py` et `06_run_iceberg.sh` (bonus)

- Transforme la table silver en table Apache Iceberg (`dvf/warehouse/silver/ventes/`), pour montrer évolution de schéma, time travel et rollback. Déroulé : création partitionnée par mois de vente, ajout d'une colonne `est_station` remplie par `MERGE`, suppression volontaire des ventes de Vars, relecture de la table à l'ancien snapshot (time travel), puis `rollback_to_snapshot` pour annuler la suppression sans réécrire les fichiers de données.
- Point d'attention : l'image du service fournit Spark 4.3, incompatible avec `iceberg-spark-runtime`. `06_run_iceberg.sh` installe donc un environnement Python séparé avec `pyspark>=4.1,<4.2` (dans `~/.venv41`), copie silver et la référence des stations en local, lance `06_iceberg.py` avec ce Spark 4.1, puis resynchronise le résultat vers `dvf/warehouse`.
- Lancer : `setsid nohup bash src/06_run_iceberg.sh > ~/work/ice_run.log 2>&1 &` (en arrière-plan, car l'installation de l'environnement 4.1 prend du temps). Variable optionnelle : `ICEBERG_PACKAGE`.

## `spark_session.py`

Module partagé par `02`, `03` et `06` : construit la `SparkSession` connectée au S3 d'Onyxia (`s3a://`) à partir des variables `AWS_*` fournies par le service, et calcule l'URI de base du projet (`DVF_BASE`, par défaut `s3a://<bucket>/dvf`). Pas de script à lancer directement.

## `k8s/job-pipeline.yaml` et `k8s/run_job.sh`

- `run_job.sh` : lit l'image du Pod en cours, crée le Secret Kubernetes `dvf-s3-credentials` à partir des clés S3 temporaires du service, puis applique `job-pipeline.yaml` (en remplaçant `${IMAGE}` et `${REPO_URL}`) et suit les logs du Job.
- `job-pipeline.yaml` : clone le dépôt dans le Pod, installe DuckDB, puis lance `bash src/run_pipeline.sh`. Limites : `backoffLimit: 1`, `activeDeadlineSeconds: 1800` (abandon après 30 min), ressources `requests: 250m CPU / 512Mi` et `limits: 1 CPU / 1536Mi`, adaptées pour rester sous le quota du namespace (4 CPU / 4 Gi) même si d'autres services tournent en parallèle.
- Prérequis : le service doit avoir le rôle Kubernetes « admin » (formulaire Onyxia, section Role), sinon `kubectl` n'a pas le droit de créer de Job.
- Lancer : `bash k8s/run_job.sh`, depuis la racine du dépôt.

## Autre année ou département

`YEAR=2023 DEP=05 bash src/01_ingest_raw.sh` télécharge un autre jeu de données. Les scripts `02` à `06` lisent en dur `year=2024/dep=05` : à adapter dans le code si on change d'année ou de département.

## Préfixes S3 secondaires

| Préfixe | Contenu |
|---|---|
| `dvf/logs/` | journal complet de chaque exécution de `run_pipeline.sh` |
| `dvf/mesures/` | copie Parquet du brut et résultats de `05_mesure_formats.py` |
| `dvf/warehouse/silver/ventes/` | table Iceberg du bonus (données et métadonnées) |
