"""Bonus (cours 8) : la couche silver en table Iceberg, avec snapshots, évolution de schéma, time travel et rollback.

Lit   : <base>/silver/ventes_appartements/ (Parquet)  +  <base>/ref/stations_ski_05.csv
Écrit : <base>/warehouse/silver/ventes/   (table Iceberg : fichiers Parquet + métadonnées)

Déroulé :
  1. création de la table, partitionnée par mois de vente (partitionnement caché)   -> snapshot 1
  2. évolution de schéma : ajout de la colonne est_station, puis remplissage (MERGE)  -> snapshot 2
  3. une suppression par erreur (toutes les ventes de Vars)                          -> snapshot 3
  4. time travel : on relit la table telle qu'elle était au snapshot 2
  5. rollback : on revient au snapshot 2, sans réécrire aucun fichier de données
Usage : cd src && python 06_iceberg.py
"""
import os, time
from pyspark.sql import functions as F
from spark_session import get_spark, base_uri, bucket

t0 = time.time()
PKG = os.environ.get("ICEBERG_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0")
BASE = base_uri()
spark = get_spark("dvf-iceberg", extra={
    "spark.jars.packages": PKG,
    "spark.jars.ivy": os.environ.get("IVY_DIR", "/home/onyxia/work/.ivy2"),
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hadoop",
    "spark.sql.catalog.ice.warehouse": f"{BASE}/warehouse",
})
T = "ice.silver.ventes"

def titre(s):
    print(f"\n== {s}")

def compte(sql_from=T):
    return spark.sql(f"SELECT count(*) AS n FROM {sql_from}").collect()[0]["n"]

def snapshots():
    return spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {T}.snapshots ORDER BY committed_at")

# 1. Création (idempotent : on repart d'une table neuve à chaque exécution)
spark.sql(f"DROP TABLE IF EXISTS {T} PURGE")
ventes = (spark.read.parquet(f"{BASE}/silver/ventes_appartements")
          .withColumn("date_mutation", F.to_date("date_mutation")))
ventes.writeTo(T).using("iceberg").partitionedBy(F.months("date_mutation")).create()
titre(f"1. table créée : {compte()} ventes, partitionnée par mois (12 partitions cachées)")
spark.sql(f"SELECT partition, record_count, file_count FROM {T}.partitions ORDER BY partition").show(3, truncate=False)

# 2. Évolution de schéma + MERGE
spark.sql(f"ALTER TABLE {T} ADD COLUMN est_station BOOLEAN")
spark.read.csv(f"{BASE}/ref/stations_ski_05.csv", header=True).select("code_commune").distinct() \
    .createOrReplaceTempView("stations")
spark.sql(f"""MERGE INTO {T} t
              USING (SELECT v.id_mutation, s.code_commune IS NOT NULL AS est_station
                     FROM {T} v LEFT JOIN stations s ON v.code_commune = s.code_commune) u
              ON t.id_mutation = u.id_mutation
              WHEN MATCHED THEN UPDATE SET t.est_station = u.est_station""")
titre("2. colonne est_station ajoutée sans réécrire l'ancien schéma, puis remplie (MERGE)")
spark.sql(f"SELECT est_station, count(*) AS ventes FROM {T} GROUP BY est_station").show()
snap_bon = snapshots().collect()[-1]["snapshot_id"]

# 3. Erreur volontaire
spark.sql(f"DELETE FROM {T} WHERE nom_commune = 'Vars'")
titre(f"3. suppression par erreur des ventes de Vars : il reste {compte()} ventes")

# 4. Time travel
titre(f"4. time travel : la table au snapshot {snap_bon} contient {compte(f'{T} VERSION AS OF {snap_bon}')} ventes")
snapshots().show(truncate=False)

# 5. Rollback
spark.sql(f"CALL ice.system.rollback_to_snapshot('silver.ventes', {snap_bon})")
titre(f"5. rollback au snapshot {snap_bon} : {compte()} ventes, Vars est revenue")
spark.sql(f"SELECT made_current_at, snapshot_id, is_current_ancestor FROM {T}.history ORDER BY made_current_at").show(truncate=False)
print(f"Iceberg terminé en {time.time() - t0:.1f} s (paquet {PKG})")
