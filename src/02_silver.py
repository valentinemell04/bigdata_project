"""Étape 2 (silver) : du brut DVF à une ligne = une vente d'un seul appartement, prix au m² valide.

Lit   : <base>/raw/year=2024/dep=05/*.csv
Écrit : <base>/silver/ventes_appartements/   (Parquet)
        <base>/silver/rapport_qualite/       (CSV : nombre de ventes retirées par règle)
"""
import time
from pyspark.sql import functions as F
from spark_session import get_spark, base_uri

PM2_MIN, PM2_MAX = 500, 20000  # bornes de prix au m² jugées réalistes
NON_APPART = ["Maison", "Local industriel. commercial ou assimilé"]

t0 = time.time()
spark = get_spark("dvf-silver")
BASE = base_uri()

# Tout est lu en texte (codes communes comme "05004" gardent leur zéro), puis on convertit ce qui sert.
raw = spark.read.csv(f"{BASE}/raw/year=2024/dep=05/", header=True, inferSchema=False)
raw = (raw.withColumn("valeur_fonciere", F.col("valeur_fonciere").cast("double"))
          .withColumn("surface_reelle_bati", F.col("surface_reelle_bati").cast("int"))
          .withColumn("nombre_pieces_principales", F.col("nombre_pieces_principales").cast("int")))

is_app = F.col("type_local") == "Appartement"

# Changement de grain : une ligne = un morceau de vente  ->  une ligne = une vente (id_mutation)
ventes = raw.groupBy("id_mutation").agg(
    F.first("date_mutation").alias("date_mutation"),
    F.first("nature_mutation").alias("nature"),
    F.first("valeur_fonciere", ignorenulls=True).alias("prix"),
    F.countDistinct("valeur_fonciere").alias("nb_prix"),
    F.first("code_commune").alias("code_commune"),
    F.first("nom_commune").alias("nom_commune"),
    F.sum(is_app.cast("int")).alias("nb_app_lignes"),
    F.countDistinct(F.when(is_app, F.concat_ws("|", "id_parcelle", "lot1_numero", "surface_reelle_bati"))).alias("nb_app"),
    F.sum(F.col("type_local").isin(NON_APPART).cast("int")).alias("nb_autres"),
    F.max(F.when(is_app, F.col("surface_reelle_bati"))).alias("surface"),
    F.max(F.when(is_app, F.col("nombre_pieces_principales"))).alias("pieces"),
).cache()

n_lignes, n_ventes = raw.count(), ventes.count()

# Règles de qualité appliquées dans l'ordre, en comptant ce que chacune retire
regles = [
    ("contient un appartement", F.col("nb_app_lignes") > 0),
    ("nature = Vente (hors VEFA, échange, adjudication)", F.col("nature") == "Vente"),
    ("un seul appartement dans la vente", F.col("nb_app") == 1),
    ("pas de maison ni de local dans la vente", F.col("nb_autres") == 0),
    ("prix renseigné", F.col("prix").isNotNull()),
    ("surface renseignée et > 0", F.col("surface") > 0),
    (f"prix au m² entre {PM2_MIN} et {PM2_MAX} €", F.col("prix") / F.col("surface") >= PM2_MIN),
    (f"prix au m² <= {PM2_MAX} €", F.col("prix") / F.col("surface") <= PM2_MAX),
]
rapport, df, avant = [("lignes brutes", n_lignes, None), ("ventes (id_mutation)", n_ventes, None)], ventes, n_ventes
for nom, cond in regles:
    df = df.filter(cond)
    apres = df.count()
    rapport.append((nom, apres, avant - apres))
    avant = apres

silver = df.select("id_mutation", "date_mutation", "code_commune", "nom_commune", "prix", "surface", "pieces",
                   (F.col("prix") / F.col("surface")).alias("prix_m2"))
silver.write.mode("overwrite").parquet(f"{BASE}/silver/ventes_appartements")

rq = spark.createDataFrame(rapport, "etape string, ventes_restantes long, ventes_retirees long")
rq.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{BASE}/silver/rapport_qualite")
rq.show(truncate=False)
print(f"silver : {silver.count()} ventes écrites dans {BASE}/silver/ventes_appartements  ({time.time() - t0:.1f} s)")
