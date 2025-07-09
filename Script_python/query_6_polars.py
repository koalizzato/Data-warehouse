import polars as pl
import time
import csv
import os

BASE_PATH = "/mnt/shared/arrow"
OUTPUT_CSV = "benchmark_polars_complex.csv"

print("📥 Lettura tabelle Star Schema...")
fatt = pl.read_ipc(f"{BASE_PATH}/fatt_corsa_taxi.feather", memory_map=False)
dim_data = pl.read_ipc(f"{BASE_PATH}/dim_data.feather", memory_map=False)
dim_zona_pickup = pl.read_ipc(f"{BASE_PATH}/dim_zona_pickup.feather", memory_map=False)
dim_taxi = pl.read_ipc(f"{BASE_PATH}/dim_taxi.feather", memory_map=False)
dim_pagamento = pl.read_ipc(f"{BASE_PATH}/dim_pagamento.feather", memory_map=False)

print("🧼 Normalizzazione dei tipi per join...")
fatt = fatt.with_columns([
    pl.col("id_data").cast(pl.Int64),
    pl.col("fk_zona_pickup").cast(pl.Int64),
    pl.col("id_taxi").cast(pl.Int64),
    pl.col("id_pagamento").cast(pl.Int64),
    pl.col("durata_viaggio_minuti").cast(pl.Float64),
    pl.col("Total_amount").cast(pl.Float64)
])

dim_data = dim_data.with_columns([
    pl.col("id_data").cast(pl.Int64),
    pl.col("nome_mese").cast(pl.Utf8)
])

dim_zona_pickup = dim_zona_pickup.with_columns([
    pl.col("id_zona_pickup").cast(pl.Int64),
    pl.col("zone").cast(pl.Utf8)
])

dim_taxi = dim_taxi.with_columns([
    pl.col("id_taxi").cast(pl.Int64),
    pl.col("taxi_color").cast(pl.Utf8)
])

dim_pagamento = dim_pagamento.with_columns([
    pl.col("id_pagamento").cast(pl.Int64),
    pl.col("payment_type").cast(pl.Utf8)
])

fatt = fatt.filter(pl.col("durata_viaggio_minuti") > 0)

print("⏱️ Esecuzione query complessa (DataMart_6)...")
start = time.perf_counter()

q6 = fatt \
    .join(dim_data, on="id_data", how="left") \
    .join(dim_taxi, on="id_taxi", how="left") \
    .join(dim_pagamento, on="id_pagamento", how="left") \
    .join(dim_zona_pickup, left_on="fk_zona_pickup", right_on="id_zona_pickup", how="left") \
    .group_by(["nome_mese", "taxi_color", "payment_type", "zone"]) \
    .agg([
        pl.col("Total_amount").mean().alias("avg_importo"),
        pl.count().alias("numero_corse")
    ]) \
    .sort("nome_mese")

elapsed = time.perf_counter() - start
print(f"✅ Completata in {elapsed:.4f} secondi.")

q6.write_csv("datamart_6_polars.csv")

with open(OUTPUT_CSV, mode="w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Engine", "Query", "ExecutionTime_sec"])
    writer.writerow(["Polars", "DataMart_6", f"{elapsed:.4f}"])

print(f"📄 Risultati salvati in 'benchmark_polars_complex.csv' e 'datamart_6_polars.csv'")
