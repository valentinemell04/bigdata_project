#!/usr/bin/env bash
# Lance le bonus Iceberg avec un Spark 4.1 séparé (l'image a Spark 4.3, incompatible avec iceberg-spark-runtime 4.1).
set -euo pipefail
W=/home/onyxia/work
export TMPDIR=$W/.tmp; mkdir -p "$TMPDIR"
echo "== debut $(date -u '+%Y-%m-%d %H:%M:%S UTC') | namespace $KUBERNETES_NAMESPACE"
if ! env -u PYTHONPATH -u SPARK_HOME "$W/.venv41/bin/python" -c 'import pyspark' 2>/dev/null; then
  echo "== installation de pyspark 4.1 dans $W/.venv41"
  rm -rf "$W/.venv41"; python -m venv "$W/.venv41"
  "$W/.venv41/bin/pip" install -q --no-cache-dir 'pyspark>=4.1,<4.2'
fi
rm -rf "${TMPDIR:?}"/*
env -u PYTHONPATH -u SPARK_HOME "$W/.venv41/bin/python" -c 'import pyspark; print("== pyspark du venv :", pyspark.__version__)'
L=$W/ice_local; rm -rf "$L"; mkdir -p "$L/ref"
aws s3 cp --recursive --quiet "s3://$KUBERNETES_NAMESPACE/dvf/silver/ventes_appartements/" "$L/silver/ventes_appartements/"
aws s3 cp --quiet "s3://$KUBERNETES_NAMESPACE/dvf/ref/stations_ski_05.csv" "$L/ref/"
echo "== silver et stations copies depuis S3 vers $L"
cd "$W/bigdata_project/src"
env -u SPARK_HOME -u PYTHONPATH -u SPARK_CONF_DIR -u PYSPARK_PYTHON -u PYSPARK_DRIVER_PYTHON \
    DVF_BASE="file://$L" IVY_DIR="$W/.ivy2" SPARK_LOCAL_DIRS="$W/.spark-tmp" \
    "$W/.venv41/bin/python" 06_iceberg.py
aws s3 sync --quiet "$L/warehouse" "s3://$KUBERNETES_NAMESPACE/dvf/warehouse"
echo "== table Iceberg envoyee sur s3://$KUBERNETES_NAMESPACE/dvf/warehouse"
echo "== fin $(date -u '+%Y-%m-%d %H:%M:%S UTC') ICE-OK"
