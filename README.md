# Data-warehouse

Questo progetto si basa su due dataset reali relativi ai taxi gialli e verdi della città di New York. L’obiettivo è stato costruire un Data Warehouse completo partendo da un processo ETL automatizzato con Pentaho Kettle e progettando uno schema a stella per supportare analisi OLAP.

In una seconda fase, sono state eseguite query su larga scala utilizzando Apache Spark e Polars, al fine di confrontare le prestazioni su due dataset di dimensioni diverse (4M e 12M di righe).

## Tecnologie utilizzate
- ETL: Pentaho Kettle

- Modellazione DW: PySpark

- Analisi OLAP: Spark (con Parquet) e Polars (con Feather)

- Benchmarking: cluster distribuito con 5 VM, 16GB RAM, 4 vCPU ciascuna
