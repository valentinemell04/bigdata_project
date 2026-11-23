#!/usr/bin/env bash
# Lance tout le pipeline, de bout en bout. Relançable autant de fois qu'on veut :
# chaque étape réécrit sa sortie (mode overwrite), donc le résultat est toujours le même (idempotence).
# Usage (depuis la racine du dépôt) : bash src/run_pipeline.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_LOCAL_DIRS="${SPARK_LOCAL_DIRS:-$(pwd)/.spark-tmp}"; mkdir -p "$SPARK_LOCAL_DIRS"
echo "################ $(date '+%Y-%m-%d %H:%M:%S %Z') | namespace ${KUBERNETES_NAMESPACE:-?}"
bash src/01_ingest_raw.sh
(cd src && python 02_silver.py)
(cd src && python 03_gold.py)
python src/04_verify_duckdb.py
