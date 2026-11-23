"""Étape 3 (gold) : prix médian au m², stations de ski contre reste du département, par tranche de surface.

Lit   : <base>/silver/ventes_appartements/  +  <base>/ref/stations_ski_05.csv
Écrit : <base>/gold/prix_m2_par_tranche/   (Parquet, et une copie CSV dans gold/prix_m2_par_tranche_csv/)
"""
import time
from pyspark.sql import functions as F
from spark_session import get_spark, base_uri

t0 = time.time()
spark = get_spark("dvf-gold")
BASE = base_uri()

ventes = spark.read.parquet(f"{BASE}/silver/ventes_appartements")
stations = (spark.read.csv(f"{BASE}/ref/stations_ski_05.csv", header=True, inferSchema=False)
            .select("code_commune").distinct().withColumn("est_station", F.lit(True)))

# Petite table de référence : broadcast join (cours 7), elle est copiée sur chaque executor
df = (ventes.join(F.broadcast(stations), "code_commune", "left")
      .withColumn("groupe", F.when(F.col("est_station"), "station").otherwise("reste"))
      .withColumn("tranche", F.when(F.col("surface") < 30, "1. moins de 30 m2")
                              .when(F.col("surface") < 50, "2. 30 a 50 m2")
                              .otherwise("3. 50 m2 et plus")))

# percentile = médiane exacte (pas approchée), comme quantile_cont de DuckDB
gold = (df.groupBy("tranche", "groupe")
        .agg(F.count("*").alias("nb_ventes"),
             F.expr("percentile(prix_m2, 0.5)").alias("prix_m2_median"),
             F.expr("percentile(prix_m2, 0.25)").alias("prix_m2_q1"),
             F.expr("percentile(prix_m2, 0.75)").alias("prix_m2_q3"),
             F.expr("percentile(surface, 0.5)").alias("surface_mediane"))
        .orderBy("tranche", "groupe"))

gold.write.mode("overwrite").parquet(f"{BASE}/gold/prix_m2_par_tranche")
gold.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{BASE}/gold/prix_m2_par_tranche_csv")

gold.show(truncate=False)

# Lecture « réponse » : écart station / reste dans chaque tranche
ecart = (gold.groupBy("tranche").pivot("groupe", ["station", "reste"]).agg(F.first("prix_m2_median"))
         .withColumn("ecart_pct", F.round(100 * (F.col("station") / F.col("reste") - 1), 1))
         .orderBy("tranche"))
ecart.show(truncate=False)
print(f"gold écrit dans {BASE}/gold/prix_m2_par_tranche  ({time.time() - t0:.1f} s)")
