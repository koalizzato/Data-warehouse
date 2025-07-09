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
QUERY_NAME = "DataMart_6"

# === FUNZIONE QUERY COMPLESSA ===

def run_complex_query(spark, save_csv=False):
    spark.conf.set("spark.sql.parquet.mergeSchema", "false")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    try:
        fatt = spark.read.parquet(f"{PARQUET_BASE_PATH}/fatt_corsa_taxi")
        data = spark.read.parquet(f"{PARQUET_BASE_PATH}/dim_data")
        zona_pickup = spark.read.parquet(f"{PARQUET_BASE_PATH}/dim_zona_pickup")
        taxi = spark.read.parquet(f"{PARQUET_BASE_PATH}/dim_taxi")
        pagamento = spark.read.parquet(f"{PARQUET_BASE_PATH}/dim_pagamento")
    except Exception as e:
        print("\n❌ Errore nel caricamento dei file Parquet:")
        traceback.print_exc()
        return None

    fatt.createOrReplaceTempView("fatt")
    data.createOrReplaceTempView("dim_data")
    zona_pickup.createOrReplaceTempView("dim_zona_pickup")
    taxi.createOrReplaceTempView("dim_taxi")
    pagamento.createOrReplaceTempView("dim_pagamento")

    query = """
        SELECT
            d.nome_mese,
            t.taxi_color,
            p.payment_type,
            z.zone,
            COUNT(*) AS numero_corse,
            AVG(f.Total_amount) AS avg_importo
        FROM fatt f
        JOIN dim_data d ON f.id_data = d.id_data
        JOIN dim_taxi t ON f.id_taxi = t.id_taxi
        JOIN dim_pagamento p ON f.id_pagamento = p.id_pagamento
        JOIN dim_zona_pickup z ON f.fk_zona_pickup = z.id_zona_pickup
        WHERE f.durata_viaggio_minuti > 0
        GROUP BY d.nome_mese, t.taxi_color, p.payment_type, z.zone
        ORDER BY d.nome_mese
    """

    try:
        start = time.perf_counter()
        df = spark.sql(query)
        df.collect()  # forza esecuzione
        elapsed = time.perf_counter() - start

        if save_csv:
            df.toPandas().to_csv("/home/pyspark/risultato_DM_6.csv", index=False)

        return elapsed
    except Exception:
        print("\n❌ Errore nell'esecuzione della query complessa:")
        traceback.print_exc()
        return None

# === GESTIONE WORKER ===

def stop_all_workers():
    os.system("~/spark/sbin/stop-workers.sh")

def start_n_workers(n):
    for ip in WORKER_IPS[:n]:
        os.system(f"ssh pyspark@{ip} '~/spark/sbin/start-worker.sh {MASTER_URL}'")

# === MAIN ===

def main():
    output_file = "/home/pyspark/benchmark_olap_complex.csv"
    with open(output_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Workers", QUERY_NAME])

        for n in range(1, len(WORKER_IPS) + 1):
            print(f"\n=== Avvio benchmark COMPLESSO con {n} worker ===")
            stop_all_workers()
            time.sleep(3)
            start_n_workers(n)
            print("Attendo 10 secondi per permettere ai worker di registrarsi...")
            time.sleep(10)

            spark = SparkSession.builder \
                .appName("OLAP Benchmark Complesso") \
                .master(MASTER_URL) \
                .getOrCreate()

            save_csv = (n == len(WORKER_IPS))
            exec_time = run_complex_query(spark, save_csv=save_csv)
            spark.stop()

            time_str = f"{exec_time:.2f}" if exec_time is not None else "ERR"
            writer.writerow([n, time_str])
            print(f"Completato test complesso con {n} worker: {time_str} sec")

        stop_all_workers()
        print(f"\n✅ Benchmark COMPLESSO completato. Risultati salvati in {output_file}")

if __name__ == "__main__":
    main()
