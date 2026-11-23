# Prix au m² des appartements en station de ski (DVF, Hautes-Alpes 2024)

## Auteurs
- MELLONE Valentine
- JOUINI Adam

Groupe 02 · Écosystème Big Data II

## Prérequis
- Compte Onyxia (`onyxia.adm.adaltas.cloud`) avec bucket S3 personnel (même nom que le namespace).
- Service **vscode-pyspark**, lancé avec ce dépôt dans l'onglet Git (fournit `AWS_*`, `KUBERNETES_NAMESPACE`, `aws`).
- `pip install -r requirements.txt` (DuckDB).

## Dataset
| | |
|---|---|
| Nom | Demandes de Valeurs Foncières géolocalisées (DVF), département 05, année 2024 |
| URL | https://files.data.gouv.fr/geo-dvf/latest/csv/2024/departements/05.csv.gz |
| Licence | Licence Ouverte Etalab 2.0 |
| Date de téléchargement | 23/11/2026 |
| Taille | 2,4 Mo (CSV décompressé, 14 191 lignes, 40 colonnes) |

Référence complémentaire : `src/stations_ski_05.csv` (22 communes de stations, source Wikipédia, détails dans `src/stations_ski_05_SOURCE.md`).

## Problème
Dans les Hautes-Alpes en 2024, les appartements des communes de stations de ski se vendent-ils plus cher au m² que ceux du reste du département ? Comme les stations vendent surtout de petites surfaces, la comparaison est faite à surface comparable, par tranche (< 30 m², 30-50 m², ≥ 50 m²). Résultat : le prix médian au m² de chaque groupe, dans chaque tranche.

## Chemins S3
Bucket : `s3://<namespace>/` (pour nous : `user-v-mellone-ece`)

| Couche | Préfixe | Contenu |
|---|---|---|
| Bronze (brut) | `dvf/raw/year=2024/dep=05/` | CSV DVF tel que téléchargé |
| Référence | `dvf/ref/` | `stations_ski_05.csv` |
| Silver | `dvf/silver/ventes_appartements/` | une ligne par vente d'appartement, Parquet |
| Silver | `dvf/silver/rapport_qualite/` | ventes retirées par règle de qualité, CSV |
| Gold | `dvf/gold/prix_m2_par_tranche/` (+ `_csv/`) | prix médian au m² par tranche et par groupe |

Préfixes secondaires (logs, mesures, warehouse Iceberg) : voir `src/README.md`.

## Reproduction
Dans le terminal du service vscode-pyspark, depuis la racine du dépôt.

1. `cd /home/onyxia/work/bigdata_project` puis `pip install -r requirements.txt`
2. Tout le pipeline : `bash src/run_pipeline.sh`. Étape par étape : `01_ingest_raw.sh` → `02_silver.py` → `03_gold.py` → `04_verify_duckdb.py` (vérification indépendante, `OK`/`ÉCART`).
3. Bucket : `aws s3 ls --recursive --human-readable s3://$KUBERNETES_NAMESPACE/dvf/`
4. Mesure CSV/Parquet : `python src/05_mesure_formats.py`
5. En Job Kubernetes : `bash k8s/run_job.sh` (rôle « admin » requis).

**Relancer :** chaque étape réécrit sa sortie (`overwrite`), relançable sans créer de doublons.

**Problèmes courants :** service supprimé → relancer un service vscode-pyspark. Erreur `403`/`ExpiredToken` → identifiants S3 temporaires, redémarrer le service. Détails, bonus Iceberg : `src/README.md`.

## Structure du dépôt
```text
README.md
evidence/   captures 01 à 09 (date UTC et namespace visibles) — voir evidence/README.md
src/        01 à 04 pipeline, 05 mesure, 06 Iceberg (bonus), run_pipeline.sh, table des stations
k8s/        Job Kubernetes (job-pipeline.yaml) et son lanceur (run_job.sh)
```
