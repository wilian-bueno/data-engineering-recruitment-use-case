import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# ── File paths (from env vars injected by docker-compose) ──────────
DELTA_STORAGE = os.getenv("DELTA_STORAGE", "/home/jovyan/delta_storage")
SALES_FILE    = os.getenv("SALES_FILE",
                  "/home/jovyan/data/abi_bus_case1_beverage_sales_20210726.csv")
CHANNEL_FILE  = os.getenv("CHANNEL_FILE",
                  "/home/jovyan/data/abi_bus_case1_beverage_channel_group_20210726.csv")

# ── Delta layer paths ───────────────────────────────────────────────
BRONZE_PATH = f"{DELTA_STORAGE}/bronze"
SILVER_PATH = f"{DELTA_STORAGE}/silver"
GOLD_PATH   = f"{DELTA_STORAGE}/gold"

# ── PostgreSQL connection ───────────────────────────────────────────
PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = os.getenv("PG_PORT",     "5432")
PG_DB       = os.getenv("PG_DB",       "beverage")
PG_USER     = os.getenv("PG_USER",     "beverage")
PG_PASSWORD = os.getenv("PG_PASSWORD", "beverage")

PG_JDBC_URL  = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?sslmode=disable"
PG_WRITE_PROPS = {
    "url":      PG_JDBC_URL,
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ── SparkSession with Delta Lake + PostgreSQL JDBC ──────────────────
builder = (
    SparkSession.builder
    .appName("beverage-analytics")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", f"{DELTA_STORAGE}/warehouse")
    .config("spark.jars", "/usr/local/spark/jars/postgresql-42.7.3.jar")
    .config("spark.driver.memory", "1g")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"SparkSession ready | version={spark.version} | Delta + JDBC loaded")
