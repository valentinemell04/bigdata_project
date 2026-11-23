"""Mesure : CSV ou Parquet ? (alternative comparée dans le rapport)

Prend le CSV brut DVF, l'écrit aussi en Parquet, puis compare sur S3 :
  - la taille des deux fichiers,
  - le temps de 3 requêtes typiques (tout lire / une seule colonne / un filtre), médiane de 5 essais.
Moteur : DuckDB, qui lit les deux formats directement sur S3.

Usage : python src/05_mesure_formats.py             (sur Onyxia, bucket = namespace)
        python src/05_mesure_formats.py --base data  (en local, dossier qui imite le bucket)
"""
import argparse, os, statistics, time
import duckdb

p = argparse.ArgumentParser()
p.add_argument("--base", default=f"s3://{os.environ.get('BUCKET', os.environ.get('KUBERNETES_NAMESPACE', ''))}/dvf")
p.add_argument("--essais", type=int, default=5)
args = p.parse_args()
B = args.base.rstrip("/")
S3 = B.startswith("s3://")

con = duckdb.connect()
if os.environ.get("AWS_S3_ENDPOINT") and S3:
    q = lambda v: "'" + v.replace("'", "''") + "'"
    con.execute(f"""CREATE OR REPLACE SECRET dvf_s3 (TYPE s3,
        KEY_ID {q(os.environ['AWS_ACCESS_KEY_ID'])}, SECRET {q(os.environ['AWS_SECRET_ACCESS_KEY'])},
        SESSION_TOKEN {q(os.environ.get('AWS_SESSION_TOKEN', ''))}, ENDPOINT {q(os.environ['AWS_S3_ENDPOINT'])},
        URL_STYLE 'path', REGION {q(os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))})""")

csv_path = f"{B}/raw/year=2024/dep=05/dvf_05_2024.csv"
pq_path = f"{B}/mesures/dvf_05_2024.parquet"
if not S3:
    os.makedirs(f"{B}/mesures", exist_ok=True)

# Même contenu, deux formats : on écrit la version Parquet à partir du CSV brut
con.execute(f"COPY (SELECT * FROM read_csv('{csv_path}', header=true, all_varchar=false, sample_size=-1)) "
            f"TO '{pq_path}' (FORMAT parquet, COMPRESSION zstd)")

def taille(path):
    return con.execute(f"SELECT size FROM read_blob('{path}')").fetchall()[0][0]

src = {"CSV": f"read_csv('{csv_path}', header=true, all_varchar=false, sample_size=-1)",
       "Parquet": f"read_parquet('{pq_path}')"}
requetes = {
    "tout lire (40 colonnes)": "SELECT * FROM {t}",
    "une colonne (moyenne du prix)": "SELECT avg(valeur_fonciere) FROM {t}",
    "filtre (appartements de Vars)": "SELECT count(*) FROM {t} WHERE nom_commune = 'Vars' AND type_local = 'Appartement'",
}

lignes = []
for fmt, t in src.items():
    for nom, sql in requetes.items():
        temps = []
        for _ in range(args.essais):
            t0 = time.perf_counter()
            con.sql(sql.format(t=t)).fetchall()
            temps.append(time.perf_counter() - t0)
        lignes.append((fmt, nom, round(statistics.median(temps) * 1000, 1)))

tailles = {"CSV": taille(csv_path), "Parquet": taille(pq_path)}
con.execute("CREATE TABLE m (format VARCHAR, requete VARCHAR, mediane_ms DOUBLE)")
con.executemany("INSERT INTO m VALUES (?, ?, ?)", lignes)

print("== Taille sur le stockage")
for f, s in tailles.items():
    print(f"   {f:8s} {s / 1024:8.1f} Kio")
print(f"   Parquet = {100 * tailles['Parquet'] / tailles['CSV']:.0f} % de la taille du CSV")
print(f"\n== Temps de lecture avec DuckDB (médiane de {args.essais} essais, en ms)")
con.execute("""CREATE TABLE res AS
  SELECT requete,
         first(mediane_ms) FILTER (WHERE format = 'CSV') AS csv_ms,
         first(mediane_ms) FILTER (WHERE format = 'Parquet') AS parquet_ms,
         round(csv_ms / parquet_ms, 1) AS parquet_plus_rapide_x
  FROM m GROUP BY requete ORDER BY requete""")
con.sql("SELECT * FROM res").show()
if S3:
    con.execute(f"COPY res TO '{B}/mesures/mesure_formats.csv' (HEADER)")
    print(f"résultats écrits dans {B}/mesures/mesure_formats.csv")
