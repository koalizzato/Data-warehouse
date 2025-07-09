import os
import time
import csv
import traceback
from pyspark.sql import SparkSession

# === CONFIGURAZIONE ===

MASTER_URL = "spark://10.0.1.5:7099"
WORKER_IPS = [
    "72.146.28.253",
    "72.146.29.74",
    "72.146.29.228",
    "72.146.29.246"
]

PARQUET_BASE_PATH = "/mnt/shared"
QUERY_NAMES = [
    "DataMart_1", "DataMart_2", "DataMart_3", "DataMart_4", "DataMart_5"
]

# === FUNZIONE QUERY ===

def run_data_marts(spark, save_csv=False):
    spark.conf.set("spark.sql.parquet.mergeSchema", "false")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    times = []

    try:
        fatt = spark.read.parquet(f"{PARQUET_BASE_PATH}/fatt_corsa_taxi")
        data = spark.read.parquet(f"{PARQUET_BASE_PATH}/dim_data")
        zona_pickup = spark.read.parquet(f"{PARQUET_BASE_PATH}/dim_zona_pickup")
        zona_dropoff = spark.read.parquet(f"{PARQUET_BASE_PATH}/dim_zona_dropoff")
        taxi = spark.read.parquet(f"{PARQUET_BASE_PATH}/dim_taxi")
        pagamento = spark.read.parquet(f"{PARQUET_BASE_PATH}/dim_pagamento")
    except Exception as e:
        print("\n❌ Errore nel caricamento dei file Parquet:")
        traceback.print_exc()
        return [None] * 5

    fatt.createOrReplaceTempView("fatt")
    data.createOrReplaceTempView("dim_data")
    zona_pickup.createOrReplaceTempView("dim_zona_pickup")
    zona_dropoff.createOrReplaceTempView("dim_zona_dropoff")
    taxi.createOrReplaceTempView("dim_taxi")
    pagamento.createOrReplaceTempView("dim_pagamento")

    queries = [
        ("""
            SELECT d.nome_mese, COUNT(*) AS numero_corse
            FROM fatt f
            JOIN dim_data d ON f.id_data = d.id_data
            GROUP BY d.nome_mese
        """, "risultato_DM_1.csv"),
        ("""
            SELECT z.zone, AVG(f.total_amount) AS media_importo
            FROM fatt f
            JOIN dim_zona_pickup z ON f.fk_zona_pickup = z.id_zona_pickup
            GROUP BY z.zone
        """, "risultato_DM_2.csv"),
        ("""
            SELECT t.taxi_color, AVG(f.durata_viaggio_minuti) AS media_durata
            FROM fatt f
            JOIN dim_taxi t ON f.id_taxi = t.id_taxi
            GROUP BY t.taxi_color
        """, "risultato_DM_3.csv"),
        ("""
            SELECT p.payment_type, AVG(f.tip_amount) AS media_mancia
            FROM fatt f
            JOIN dim_pagamento p ON f.id_pagamento = p.id_pagamento
            GROUP BY p.payment_type
        """, "risultato_DM_4.csv"),
        ("""
            SELECT z.borough, COUNT(*) AS numero_corse
            FROM fatt f
            JOIN dim_zona_dropoff z ON f.fk_zona_dropoff = z.id_zona_dropoff
            GROUP BY z.borough
        """, "risultato_DM_5.csv")
    ]

    for idx, (sql, filename) in enumerate(queries):
        try:
            start = time.perf_counter()
            df = spark.sql(sql)
            df.collect()
            times.append(time.perf_counter() - start)
            if save_csv:
                df.toPandas().to_csv(f"/home/pyspark/{filename}", index=False)
        except Exception:
            print(f"\n❌ Errore Query {idx+1}:")
            traceback.print_exc()
            times.append(None)

    return times

# === GESTIONE WORKER ===

def stop_all_workers():
    os.system("~/spark/sbin/stop-workers.sh")

def start_n_workers(n):
    for ip in WORKER_IPS[:n]:
        os.system(f"ssh pyspark@{ip} '~/spark/sbin/start-worker.sh {MASTER_URL}'")

# === MAIN ===

def main():
    output_file = "/home/pyspark/benchmark_olap.csv"
    with open(output_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Workers"] + QUERY_NAMES)

        for n in range(1, len(WORKER_IPS) + 1):
            print(f"\n=== Avvio benchmark con {n} worker ===")
            stop_all_workers()
            time.sleep(3)
            start_n_workers(n)
            print("Attendo 10 secondi per permettere ai worker di registrarsi...")
            time.sleep(10)

            spark = SparkSession.builder \
                .appName("OLAP Benchmark") \
                .master(MASTER_URL) \
                .getOrCreate()

            save_csv = (n == len(WORKER_IPS))
            times = run_data_marts(spark, save_csv=save_csv)
            spark.stop()

            writer.writerow([n] + [f"{t:.2f}" if t is not None else "ERR" for t in times])
            print(f"Completato test con {n} worker: {times}")

        stop_all_workers()
        print(f"\nBenchmark completato. Risultati salvati in {output_file}")

if __name__ == "__main__":
    main()
