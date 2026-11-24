# Index des preuves

Chaque capture montre la date et l'heure (UTC) et le namespace Onyxia `user-v-mellone-ece` dans le terminal, imprimés par les scripts eux-mêmes avant ou après l'action concernée.

| # | Fichier | Ce qu'elle prouve |
|---|---|---|
| 01 | `01_onyxia_service_pod_pvc.jpg` | Le service `vscode-pyspark-411232` est déployé dans le namespace `user-v-mellone-ece` (chart `vscode-pyspark-3.2.8`, image `py3.14.7-spark4.3.0`), avec son Pod `Running` et son PVC `Bound` de 10 Gi. |
| 02 | `02_s3_raw_upload.jpg` | `01_ingest_raw.sh` télécharge le CSV brut (2,3 Mio, 14 192 lignes, empreinte SHA-256 affichée), puis le dépose sur S3 avec `stations_ski_05.csv`, aux chemins `dvf/raw/year=2024/dep=05/` et `dvf/ref/`. |
| 03 | `03_s3_bucket_raw.jpg` | Contenu du bucket après l'ingestion : les 2 objets bruts (2,3 MiB au total) visibles via `aws s3 ls --recursive --summarize`. |
| 04 | `04_silver_spark_rapport_qualite.jpg` | `02_silver.py` tourne sur 4 partitions (node `compute-03`) : rapport de qualité étape par étape (14 191 lignes brutes → 1 897 ventes retenues) et écriture du silver en 60,2 s. |
| 05 | `05_gold_spark_resultat.jpg` | `03_gold.py` écrit le résultat gold en 41,5 s : médiane du prix au m² par tranche et par groupe (station/reste), les chiffres cités dans le README (ex. 3e tranche : 3 051 € station contre 2 267 € reste). |
| 06 | `06_verification_duckdb_ok.jpg` | `04_verify_duckdb.py` recalcule le résultat depuis le CSV brut en SQL (DuckDB), sans passer par Spark, et retrouve exactement les mêmes nombres de ventes et médianes que le gold Spark : `RÉSULTAT : OK`. |
| 07 | `07_job_kubernetes_complete.jpg` | Le même pipeline tourne comme Job Kubernetes (`k8s/job-pipeline.yaml`) : `job.batch/dvf-pipeline` au statut `Complete`, 1/1, en 3 min 41 s. |
| 08 | `08_mesure_csv_parquet.jpg` | `05_mesure_formats.py` : Parquet pèse 18 % de la taille du CSV et les lectures DuckDB sont 3 à 34,5 fois plus rapides selon la requête (médiane de 5 essais). |
| 09 | `09_iceberg_time_travel_rollback.jpg` | Bonus `06_iceberg.py` : suppression volontaire des ventes de Vars, time travel sur un ancien snapshot (1 897 ventes retrouvées) et `rollback_to_snapshot` qui restaure l'état d'avant la suppression, visible dans l'historique des snapshots. |
