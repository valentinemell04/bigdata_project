#!/usr/bin/env bash
# Étape 1 (bronze) : télécharge le DVF géolocalisé et dépose le CSV BRUT sur S3,
# ainsi que la table de référence des stations de ski (faite par l'équipe).
# Usage : bash src/01_ingest_raw.sh        (variables optionnelles : YEAR, DEP, BUCKET)
set -euo pipefail
YEAR=${YEAR:-2024}
DEP=${DEP:-05}
BUCKET=${BUCKET:-$KUBERNETES_NAMESPACE}
URL="https://files.data.gouv.fr/geo-dvf/latest/csv/${YEAR}/departements/${DEP}.csv.gz"
FILE="data/raw/dvf_${DEP}_${YEAR}.csv"
S3="s3://${BUCKET}/dvf"

echo "== $(date '+%Y-%m-%d %H:%M:%S %Z') | namespace : ${KUBERNETES_NAMESPACE:-?} | bucket : ${BUCKET}"
mkdir -p data/raw
curl -sSfL "$URL" -o "${FILE}.gz"
gunzip -kf "${FILE}.gz"

echo "== Fichier brut"
ls -lh "$FILE"
wc -l "$FILE"
sha256sum "$FILE"

# Le brut : déposé tel quel, jamais modifié ensuite (couche bronze)
aws s3 cp "$FILE" "${S3}/raw/year=${YEAR}/dep=${DEP}/dvf_${DEP}_${YEAR}.csv"
# La référence des stations (petite table de dimension)
aws s3 cp src/stations_ski_05.csv "${S3}/ref/stations_ski_05.csv"

echo "== Contenu du bucket"
aws s3 ls --recursive --human-readable "${S3}/"
