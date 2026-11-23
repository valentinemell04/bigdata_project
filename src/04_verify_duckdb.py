"""Étape 4 : vérification indépendante.

Recalcule le résultat gold avec un AUTRE moteur (DuckDB, en SQL) directement depuis le CSV brut,
sans passer par le code Spark, puis compare avec le gold écrit par Spark.

Usage : python src/04_verify_duckdb.py              (sur Onyxia : lit s3://<bucket>/dvf)
        python src/04_verify_duckdb.py --base data   (en local, dossier qui imite le bucket)
"""
import argparse, glob, os, sys, time
import duckdb

p = argparse.ArgumentParser()
p.add_argument("--base", default=f"s3://{os.environ.get('BUCKET', os.environ.get('KUBERNETES_NAMESPACE', ''))}/dvf")
p.add_argument("--tolerance", type=float, default=0.01, help="écart toléré en euros sur les médianes")
args = p.parse_args()
B = args.base.rstrip("/")
t0 = time.time()

con = duckdb.connect()
# Dans le service VS Code, Onyxia crée déjà le secret s3_onyxia_connection (cours 4).
# Dans le Pod du Job, il n'existe pas : on le crée à partir des variables AWS_*.
if os.environ.get("AWS_S3_ENDPOINT") and B.startswith("s3://"):
    q = lambda v: "'" + v.replace("'", "''") + "'"
    con.execute(f"""CREATE OR REPLACE SECRET dvf_s3 (TYPE s3,
        KEY_ID {q(os.environ['AWS_ACCESS_KEY_ID'])}, SECRET {q(os.environ['AWS_SECRET_ACCESS_KEY'])},
        SESSION_TOKEN {q(os.environ.get('AWS_SESSION_TOKEN', ''))}, ENDPOINT {q(os.environ['AWS_S3_ENDPOINT'])},
        URL_STYLE 'path', REGION {q(os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))})""")

con.execute(f"""
CREATE TABLE r AS SELECT * REPLACE (TRY_CAST(valeur_fonciere AS DOUBLE) AS valeur_fonciere,
                                    TRY_CAST(surface_reelle_bati AS INTEGER) AS surface_reelle_bati)
FROM read_csv('{B}/raw/year=2024/dep=05/*.csv', header=true, all_varchar=true);

CREATE TABLE stations AS SELECT DISTINCT code_commune
FROM read_csv('{B}/ref/stations_ski_05.csv', header=true, all_varchar=true);

CREATE TABLE v AS SELECT id_mutation,
  any_value(nature_mutation) AS nature,
  max(valeur_fonciere) AS prix,
  any_value(code_commune) AS code_commune,
  count(*) FILTER (WHERE type_local = 'Appartement') AS nb_app_lignes,
  count(DISTINCT CASE WHEN type_local = 'Appartement'
        THEN concat_ws('|', id_parcelle, lot1_numero, surface_reelle_bati::VARCHAR) END) AS nb_app,
  count(*) FILTER (WHERE type_local IN ('Maison', 'Local industriel. commercial ou assimilé')) AS nb_autres,
  max(surface_reelle_bati) FILTER (WHERE type_local = 'Appartement') AS surface
FROM r GROUP BY id_mutation;

CREATE TABLE verif AS
WITH a AS (
  SELECT *, prix / surface AS prix_m2 FROM v
  WHERE nb_app_lignes > 0 AND nature = 'Vente' AND nb_app = 1 AND nb_autres = 0
    AND prix IS NOT NULL AND surface > 0 AND prix / surface BETWEEN 500 AND 20000)
SELECT CASE WHEN surface < 30 THEN '1. moins de 30 m2' WHEN surface < 50 THEN '2. 30 a 50 m2'
            ELSE '3. 50 m2 et plus' END AS tranche,
       CASE WHEN code_commune IN (SELECT code_commune FROM stations) THEN 'station' ELSE 'reste' END AS groupe,
       count(*) AS nb_ventes,
       quantile_cont(prix_m2, 0.5) AS prix_m2_median
FROM a GROUP BY ALL ORDER BY tranche, groupe;
""")
print(f"== Recalcul DuckDB indépendant ({time.time() - t0:.1f} s)")
con.sql("SELECT tranche, groupe, nb_ventes, round(prix_m2_median, 2) AS prix_m2_median FROM verif").show()

gold_path = f"{B}/gold/prix_m2_par_tranche/*.parquet"
if not B.startswith("s3://") and not glob.glob(gold_path):
    print("Pas de gold Spark trouvé : recalcul seul, pas de comparaison.")
    sys.exit(0)

cmp = con.sql(f"""
SELECT v.tranche, v.groupe,
       v.nb_ventes AS ventes_duckdb, g.nb_ventes AS ventes_spark,
       round(v.prix_m2_median, 2) AS mediane_duckdb, round(g.prix_m2_median, 2) AS mediane_spark,
       (v.nb_ventes = g.nb_ventes AND abs(v.prix_m2_median - g.prix_m2_median) <= {args.tolerance}) AS identique
FROM verif v FULL JOIN read_parquet('{gold_path}') g USING (tranche, groupe)
ORDER BY 1, 2""")
print("== Comparaison avec le gold écrit par Spark")
cmp.show()
ok = all(r[-1] for r in cmp.fetchall())
print("RÉSULTAT :", "OK, les deux moteurs donnent le même résultat" if ok else "ÉCART détecté")
sys.exit(0 if ok else 1)
