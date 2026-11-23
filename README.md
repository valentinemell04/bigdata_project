# Prix au m² des appartements en station de ski (DVF, Hautes-Alpes 2024)

## Auteurs
- MELLONE Valentine
- JOUINI Adam

Groupe 02 · Écosystème Big Data II

## Prérequis
- Un compte sur la plateforme Onyxia (`onyxia.adm.adaltas.cloud`) avec son bucket S3 personnel (même nom que le namespace).
- Un service **vscode-pyspark** (Python 3 + PySpark), lancé avec ce dépôt dans l'onglet Git. Le service fournit les variables `AWS_*` et `KUBERNETES_NAMESPACE`, ainsi que l'outil `aws`.
- La bibliothèque Python `duckdb` (installée à l'étape 2 ci-dessous).

## Dataset
| | |
|---|---|
| Nom | Demandes de Valeurs Foncières géolocalisées (DVF), département 05, année 2024 |
| URL | https://files.data.gouv.fr/geo-dvf/latest/csv/2024/departements/05.csv.gz |
| Licence | Licence Ouverte Etalab 2.0 |
| Date de téléchargement | 23/11/2026 |
| Taille | 2,4 Mo (CSV décompressé, 14 191 lignes, 40 colonnes) |

Données complémentaires faites par l'équipe : `src/stations_ski_05.csv` (22 communes de stations de ski), construit à partir de la catégorie Wikipédia « Station de sports d'hiver dans les Hautes-Alpes » (CC BY-SA 4.0). Source et choix détaillés dans `src/stations_ski_05_SOURCE.md`.

## Problème
Dans les Hautes-Alpes en 2024, les appartements des communes de stations de ski se vendent-ils plus cher au m² que ceux du reste du département ? Comme les stations vendent surtout de petites surfaces, la comparaison est faite à surface comparable, par tranche (moins de 30 m², 30 à 50 m², 50 m² et plus). Le résultat est le prix médian au m² de chaque groupe, dans chaque tranche.

## Chemins S3
Bucket : `s3://<namespace>/` (pour nous : `user-v-mellone-ece`)

| Couche | Préfixe | Contenu |
|---|---|---|
| Bronze (brut) | `dvf/raw/year=2024/dep=05/` | CSV DVF tel que téléchargé |
| Référence | `dvf/ref/` | `stations_ski_05.csv` |
| Silver | `dvf/silver/ventes_appartements/` | une ligne par vente d'appartement, Parquet |
| Silver | `dvf/silver/rapport_qualite/` | ventes retirées par règle de qualité, CSV |
| Gold | `dvf/gold/prix_m2_par_tranche/` (+ `_csv/`) | prix médian au m² par tranche et par groupe |
| Journaux | `dvf/logs/` | trace complète de chaque exécution de `run_pipeline.sh` |
| Mesures | `dvf/mesures/` | CSV vs Parquet (copie Parquet du brut + résultats) |
| Iceberg (bonus) | `dvf/warehouse/silver/ventes/` | silver en table Iceberg (données + métadonnées) |

## Reproduction
Toutes les commandes se lancent dans le terminal du service vscode-pyspark, depuis la racine du dépôt.

1. Se placer dans le dépôt : `cd /home/onyxia/work/bigdata_project`
2. Installer DuckDB pour la vérification : `pip install duckdb`
3. Lancer tout le pipeline : `bash src/run_pipeline.sh`

   Ou étape par étape :
   - `bash src/01_ingest_raw.sh` : téléchargement et dépôt du brut sur S3
   - `cd src && python 02_silver.py && cd ..` : nettoyage, une ligne par vente, Parquet
   - `cd src && python 03_gold.py && cd ..` : résultat final
   - `python src/04_verify_duckdb.py` : recalcul indépendant avec DuckDB et comparaison (affiche `OK` ou `ÉCART`)
4. Vérifier le contenu du bucket : `aws s3 ls --recursive --human-readable s3://$KUBERNETES_NAMESPACE/dvf/`
5. Mesure CSV vs Parquet : `python src/05_mesure_formats.py` (taille et temps de 3 requêtes, médiane de 5 essais)
6. Bonus Iceberg : `setsid nohup bash src/06_run_iceberg.sh > ~/work/ice_run.log 2>&1 &` (Spark 4.1 installé à part, car le Spark 4.3 de l'image est incompatible avec `iceberg-spark-runtime` ; voir l'en-tête du script)

**En Job Kubernetes.** Le même pipeline peut tourner dans un Pod dédié qui s'arrête à la fin : `bash k8s/run_job.sh`. Le service doit avoir le rôle Kubernetes « admin » (formulaire Onyxia, section Role, activé par défaut). Le script crée le Secret des clés S3, applique `k8s/job-pipeline.yaml` (limites adaptées au quota du namespace : 4 CPU / 4 Gi) et suit le Job ; le journal est aussi déposé dans `dvf/logs/`.

**Relancer.** Chaque étape réécrit entièrement sa sortie (mode `overwrite`) : on peut relancer une étape seule, ou tout le pipeline, sans créer de doublons. Le brut sur S3 n'est jamais modifié, donc silver et gold peuvent toujours être recalculés à partir de lui.

**Si le service a été supprimé.** Le dossier de travail est effacé avec le service, mais les données restent sur S3 et le code sur GitHub : relancer un service vscode-pyspark avec ce dépôt, puis reprendre à l'étape 1.

**Erreur `403` ou `ExpiredToken` sur S3.** Les identifiants S3 d'Onyxia sont temporaires : redémarrer le service pour les renouveler.

**Service expulsé (« ephemeral-storage »).** Les fichiers temporaires de Spark sont écrits sur le PVC (`SPARK_LOCAL_DIRS`, réglé par `run_pipeline.sh`) pour limiter l'usage du disque du node.

**Autre année ou département.** `YEAR=2023 DEP=05 bash src/01_ingest_raw.sh` (les scripts Spark lisent `year=2024/dep=05`, à adapter).

## Structure du dépôt
```text
README.md
evidence/   captures 01 à 09 (date UTC et namespace visibles)
src/        01 à 04 pipeline, 05 mesure, 06 Iceberg, run_pipeline.sh, table des stations
k8s/        Job Kubernetes (job-pipeline.yaml) et son lanceur (run_job.sh)
```
