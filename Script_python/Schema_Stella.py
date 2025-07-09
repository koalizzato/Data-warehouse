from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, regexp_replace, abs, hour, minute, when, first

# === Inizializzazione Spark ===
spark = SparkSession.builder.appName("StarSchemaTaxiFinal").getOrCreate()

# === Lettura CSV ===
df = spark.read.option("header", True).option("inferSchema", True).csv("dati_pronti_rip2.csv")

# === Aggiunta ID univoco per tracciamento ===
df = df.withColumn("id_originale", monotonically_increasing_id())

# === Pulizia colonne numeriche ===
df = df.withColumn("durata_viaggio_minuti", abs(regexp_replace("durata_viaggio_minuti", ",", ".").cast("double")))

# === Estrazione colonne temporali ===
df = df.withColumn("ora_pu", hour("time_PU"))
df = df.withColumn("minuto_pu", minute("time_PU"))
df = df.withColumn("ora_do", hour("time_DO"))
df = df.withColumn("minuto_do", minute("time_DO"))

# === Fasce orarie ===
def fascia_oraria_expr(col_ora):
    return when(col_ora.between(0, 5), "Notte") \
        .when(col_ora.between(6, 11), "Mattina") \
        .when(col_ora.between(12, 17), "Pomeriggio") \
        .when(col_ora.between(18, 23), "Sera") \
        .otherwise("Sconosciuta")

df = df.withColumn("fascia_pu", fascia_oraria_expr(col("ora_pu")))
df = df.withColumn("fascia_do", fascia_oraria_expr(col("ora_do")))

# === Dimensioni ===

# 📅 DIM_DATA — Fix con first() per evitare duplicati
dim_data = df.groupBy(col("Date").alias("data")).agg(
    first("DayOfMonthNumber").alias("giorno_mese"),
    first("DayOfWeek").alias("giorno_settimana"),
    first("DayName").alias("nome_giorno"),
    first("FiscalMonthNumber").alias("mese"),
    first("FiscalMonthName").alias("nome_mese"),
    first("FiscalQuarter").alias("trimestre"),
    first("FiscalYear").alias("anno")
).withColumn("id_data", monotonically_increasing_id())

# 📍 DIMENSIONI SPAZIALI
dim_zona_pickup = df.selectExpr("PU_location_ID as location_id", "PU_borough as borough", "PU_zone as zone") \
    .dropna().dropDuplicates(["location_id", "borough", "zone"]) \
    .withColumn("id_zona_pickup", monotonically_increasing_id())

dim_zona_dropoff = df.selectExpr("DO_location_ID as location_id", "DO_borough as borough", "DO_zone as zone") \
    .dropna().dropDuplicates(["location_id", "borough", "zone"]) \
    .withColumn("id_zona_dropoff", monotonically_increasing_id())

# 🕒 DIMENSIONI TEMPORALI
dim_tempo_pickup = df.select("ora_pu", "minuto_pu", "fascia_pu").dropDuplicates() \
    .withColumn("id_tempo_pickup", monotonically_increasing_id())

dim_tempo_dropoff = df.select("ora_do", "minuto_do", "fascia_do").dropDuplicates() \
    .withColumn("id_tempo_dropoff", monotonically_increasing_id())

# 🚕 DIM_TAXI — fixata: join su TaxiColor + Trip_type
dim_taxi = df.select(
    col("TaxiColor").alias("taxi_color"),
    col("Trip_type").alias("trip_type")
).dropDuplicates().withColumn("id_taxi", monotonically_increasing_id())

# 💳 DIM_PAGAMENTO
dim_pagamento = df.select(col("Payment_type").cast("int").alias("payment_type")) \
    .dropDuplicates().withColumn("id_pagamento", monotonically_increasing_id())

# 🏢 DIM_FORNITORE
dim_fornitore = df.select(col("VendorID").alias("vendor_code")) \
    .dropDuplicates().withColumn("id_fornitore", monotonically_increasing_id())

# === COSTRUZIONE FACT TABLE ===
df = df.alias("main")

fatt_corsa = df \
    .join(dim_data.alias("dd"), col("main.Date") == col("dd.data"), "left") \
    .join(dim_zona_pickup.alias("dzPU"), (col("main.PU_location_ID") == col("dzPU.location_id")) &
          (col("main.PU_borough") == col("dzPU.borough")) & (col("main.PU_zone") == col("dzPU.zone")), "left") \
    .join(dim_zona_dropoff.alias("dzDO"), (col("main.DO_location_ID") == col("dzDO.location_id")) &
          (col("main.DO_borough") == col("dzDO.borough")) & (col("main.DO_zone") == col("dzDO.zone")), "left") \
    .join(dim_tempo_pickup.alias("dtPU"), (col("main.ora_pu") == col("dtPU.ora_pu")) &
          (col("main.minuto_pu") == col("dtPU.minuto_pu")) & (col("main.fascia_pu") == col("dtPU.fascia_pu")), "left") \
    .join(dim_tempo_dropoff.alias("dtDO"), (col("main.ora_do") == col("dtDO.ora_do")) &
          (col("main.minuto_do") == col("dtDO.minuto_do")) & (col("main.fascia_do") == col("dtDO.fascia_do")), "left") \
    .join(dim_taxi.alias("dt"), (col("main.TaxiColor") == col("dt.taxi_color")) &
                                (col("main.Trip_type") == col("dt.trip_type")), "left") \
    .join(dim_pagamento.alias("dp"), col("main.Payment_type") == col("dp.payment_type"), "left") \
    .join(dim_fornitore.alias("df"), col("main.VendorID") == col("df.vendor_code"), "left") \
    .select(
        monotonically_increasing_id().alias("id_corsa"),
        col("main.id_originale"),
        col("dd.id_data"),
        col("dzPU.id_zona_pickup").alias("fk_zona_pickup"),
        col("dzDO.id_zona_dropoff").alias("fk_zona_dropoff"),
        col("dtPU.id_tempo_pickup"),
        col("dtDO.id_tempo_dropoff"),
        col("dt.id_taxi"),
        col("df.id_fornitore"),
        col("dp.id_pagamento"),
        col("main.Passenger_count"),
        col("main.Trip_distance"),
        col("main.durata_viaggio_secondi"),
        col("main.durata_viaggio_minuti"),
        col("main.Fare_amount"),
        col("main.Tip_amount"),
        col("main.Tolls_amount"),
        col("main.Extra"),
        col("main.MTA_tax"),
        col("main.Improvement_surcharge"),
        col("main.Total_amount")
    )

# === Scrittura in Parquet ===
output_path = "/mnt/shared"

dim_data.write.mode("overwrite").parquet(f"{output_path}/dim_data")
dim_zona_pickup.write.mode("overwrite").parquet(f"{output_path}/dim_zona_pickup")
dim_zona_dropoff.write.mode("overwrite").parquet(f"{output_path}/dim_zona_dropoff")
dim_tempo_pickup.write.mode("overwrite").parquet(f"{output_path}/dim_tempo_pickup")
dim_tempo_dropoff.write.mode("overwrite").parquet(f"{output_path}/dim_tempo_dropoff")
dim_taxi.write.mode("overwrite").parquet(f"{output_path}/dim_taxi")
dim_pagamento.write.mode("overwrite").parquet(f"{output_path}/dim_pagamento")
dim_fornitore.write.mode("overwrite").parquet(f"{output_path}/dim_fornitore")
fatt_corsa.write.mode("overwrite").parquet(f"{output_path}/fatt_corsa_taxi")

spark.stop()
