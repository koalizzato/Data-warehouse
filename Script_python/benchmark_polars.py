import polars as pl
import time
import csv

BASE_PATH = "/mnt/shared/arrow"
OUTPUT_CSV = "benchmark_polars_star.csv"

print("📥 Lettura tabelle Star Schema...")
fatt = pl.read_ipc(f"{BASE_PATH}/fatt_corsa_taxi.feather")
dim_data = pl.read_ipc(f"{BASE_PATH}/dim_data.feather")
dim_zona_pickup = pl.read_ipc(f"{BASE_PATH}/dim_zona_pickup.feather")
dim_zona_dropoff = pl.read_ipc(f"{BASE_PATH}/dim_zona_dropoff.feather")
dim_taxi = pl.read_ipc(f"{BASE_PATH}/dim_taxi.feather")
dim_pagamento = pl.read_ipc(f"{BASE_PATH}/dim_pagamento.feather")

print("🧼 Normalizzazione dei tipi per join...")
fatt = fatt.with_columns([
    pl.col("id_data").cast(pl.Int64),
    pl.col("id_zona_pickup").cast(pl.Int64),
    pl.col("id_zona_dropoff").cast(pl.Int64),
    pl.col("id_tempo_pickup").cast(pl.Int64),
    pl.col("id_tempo_dropoff").cast(pl.Int64),
    pl.col("id_taxi").cast(pl.Int64),
    pl.col("id_pagamento").cast(pl.Int64),
    pl.col("durata_viaggio_minuti").cast(pl.Float64),
    pl.col("Total_amount").cast(pl.Float64),
    pl.col("Tip_amount").cast(pl.Float64)
])

dim_data = dim_data.with_columns([
    pl.col("id_data").cast(pl.Int64),
    pl.col("FiscalMonthName").cast(pl.Utf8)
])

dim_zona_pickup = dim_zona_pickup.with_columns([
    pl.col("id_zona_pickup").cast(pl.Int64),
    pl.col("zone").cast(pl.Utf8)
])

dim_zona_dropoff = dim_zona_dropoff.with_columns([
    pl.col("id_zona_dropoff").cast(pl.Int64),
    pl.col("borough").cast(pl.Utf8)
])

dim_taxi = dim_taxi.with_columns([
    pl.col("id_taxi").cast(pl.Int64),
    pl.col("taxi_color").cast(pl.Utf8)
])

dim_pagamento = dim_pagamento.with_columns([
    pl.col("id_pagamento").cast(pl.Int64),
    pl.col("payment_type").cast(pl.Int64)
])

fatt = fatt.filter(pl.col("durata_viaggio_minuti") > 0)

times = []
query_names = ["DataMart_1", "DataMart_2", "DataMart_3", "DataMart_4", "DataMart_5"]

start = time.perf_counter()
q1 = fatt.join(dim_data, on="id_data", how="left").group_by("FiscalMonthName").len().sort("FiscalMonthName")
times.append(time.perf_counter() - start)
q1.write_csv("datamart_1_polars_star.csv")

start = time.perf_counter()
q2 = fatt.join(dim_zona_pickup, on="id_zona_pickup", how="left").group_by("zone").agg(pl.col("Total_amount").mean().alias("avg_total")).sort("zone")
times.append(time.perf_counter() - start)
q2.write_csv("datamart_2_polars_star.csv")

start = time.perf_counter()
q3 = fatt.join(dim_taxi, on="id_taxi", how="left").group_by("taxi_color").agg(pl.col("durata_viaggio_minuti").mean().alias("avg_duration")).sort("taxi_color")
times.append(time.perf_counter() - start)
q3.write_csv("datamart_3_polars_star.csv")

start = time.perf_counter()
q4 = fatt.join(dim_pagamento, on="id_pagamento", how="left").group_by("payment_type").agg(pl.col("Tip_amount").mean().alias("avg_tip")).sort("payment_type")
times.append(time.perf_counter() - start)
q4.write_csv("datamart_4_polars_star.csv")

start = time.perf_counter()
q5 = fatt.join(dim_zona_dropoff, on="id_zona_dropoff", how="left").group_by("borough").len().sort("borough")
times.append(time.perf_counter() - start)
q5.write_csv("datamart_5_polars_star.csv")

with open(OUTPUT_CSV, mode="w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Engine"] + query_names)
    writer.writerow(["Polars_Star"] + [f"{t:.4f}" for t in times])

print("\\n✅ Benchmark completato con Star Schema. Risultati salvati in 'benchmark_polars_star.csv'")
