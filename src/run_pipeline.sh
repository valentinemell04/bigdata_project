#!/usr/bin/env bash
# Lance tout le pipeline, de bout en bout. Relançable autant de fois qu'on veut :
# chaque étape réécrit sa sortie (mode overwrite), donc le résultat est toujours le même (idempotence).
# Le journal complet est aussi déposé sur S3 (dvf/logs/), pour garder une trace même quand le Pod a disparu.
# Usage (depuis la racine du dépôt) : bash src/run_pipeline.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_LOCAL_DIRS="${SPARK_LOCAL_DIRS:-$(pwd)/.spark-tmp}"; mkdir -p "$SPARK_LOCAL_DIRS"
BUCKET=${BUCKET:-$KUBERNETES_NAMESPACE}
LOG="/tmp/pipeline_$(date -u +%Y%m%dT%H%M%SZ)_${HOSTNAME:-local}.log"
trap 'aws s3 cp "$LOG" "s3://${BUCKET}/dvf/logs/$(basename "$LOG")" --quiet || true' EXIT
exec > >(tee -a "$LOG") 2>&1

echo "################ $(date '+%Y-%m-%d %H:%M:%S %Z') | namespace ${KUBERNETES_NAMESPACE:-?} | pod ${HOSTNAME:-?}"
bash src/01_ingest_raw.sh
(cd src && python 02_silver.py)
(cd src && python 03_gold.py)
python src/04_verify_duckdb.py
echo "################ fin $(date '+%Y-%m-%d %H:%M:%S %Z') : pipeline termine sans erreur"
