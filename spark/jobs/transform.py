"""
transform.py
────────────
Reads cleaned parquet from HDFS, builds Star Schema, writes to Snowflake.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

HDFS_CLEANED   = "hdfs://hadoopc:9000/uber/data/cleaned"
SPARK_MASTER   = "spark://spark-master:7077"
SNOWFLAKE_JAR  = "/opt/spark/jars/extra/snowflake-jdbc.jar,/opt/spark/jars/extra/spark-snowflake.jar"

# Snowflake config from env
SF_OPTIONS = {
    "sfURL"      : os.getenv("SNOWFLAKE_URL"),
    "sfUser"     : os.getenv("SNOWFLAKE_USER"),
    "sfPassword" : os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase" : os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema"   : os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
}


def get_spark():
    return (
        SparkSession.builder
        .appName("uber-transformation")
        .master(SPARK_MASTER)
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoopc:9000")
        .config("spark.jars", SNOWFLAKE_JAR)
        .getOrCreate()
    )


def write_to_snowflake(df, table: str):
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**SF_OPTIONS) \
        .option("dbtable", table) \
        .mode("overwrite") \
        .save()
    print(f"  [Done] {table} → Snowflake")


def build_dim_datetime(df):
    return df.select(
        F.monotonically_increasing_id().alias("datetime_id"),
        F.col("tpep_pickup_datetime") .alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        F.hour("tpep_pickup_datetime")                    .alias("hour"),
        F.dayofmonth("tpep_pickup_datetime")              .alias("day"),
        F.month("tpep_pickup_datetime")                   .alias("month"),
        F.year("tpep_pickup_datetime")                    .alias("year"),
        F.date_format("tpep_pickup_datetime", "EEEE")     .alias("weekday"),
        F.col("trip_duration_min")                        .cast("integer").alias("trip_duration_min"),
    ).dropDuplicates(["pickup_datetime", "dropoff_datetime"])


def build_dim_location(df):
    pu = df.select(
        F.col("PULocationID").alias("location_id"),
    )
    do = df.select(
        F.col("DOLocationID").alias("location_id"),
    )
    return pu.union(do).dropDuplicates(["location_id"]) \
        .withColumn("borough",      F.lit(None).cast("string")) \
        .withColumn("zone",         F.lit(None).cast("string")) \
        .withColumn("service_zone", F.lit(None).cast("string"))


def build_dim_vendor(df):
    return df.select(
        F.col("VendorID")         .alias("vendor_id"),
        F.col("VendorID")         .alias("VendorID"),
        F.lit(None).cast("string").alias("vendor_name"),
        F.col("store_and_fwd_flag").alias("store_fwd_flag"),
    ).dropDuplicates(["vendor_id"])


def build_dim_payment(df):
    payment_map = {1: "Credit Card", 2: "Cash", 3: "No Charge", 4: "Dispute", 5: "Unknown", 6: "Voided"}
    mapping_expr = F.create_map([F.lit(k) for pair in payment_map.items() for k in pair])
    return df.select(
        F.col("payment_type").alias("payment_id"),
        F.col("payment_type"),
        mapping_expr[F.col("payment_type")].alias("payment_desc"),
        F.col("congestion_surcharge"),
        F.col("cbd_congestion_fee"),
    ).dropDuplicates(["payment_id"])


def build_dim_rate(df):
    rate_map = {1: "Standard", 2: "JFK", 3: "Newark", 4: "Nassau", 5: "Negotiated", 6: "Group"}
    mapping_expr = F.create_map([F.lit(k) for pair in rate_map.items() for k in pair])
    return df.select(
        F.col("RatecodeID").alias("rate_id"),
        F.col("RatecodeID"),
        mapping_expr[F.col("RatecodeID")].alias("rate_desc"),
        F.col("Airport_fee").alias("airport_fee"),
    ).dropDuplicates(["rate_id"])


def build_fact_trip(df, dim_datetime, dim_location, dim_vendor, dim_payment, dim_rate):

    # Join to get surrogate keys
    df = df.join(
        dim_datetime.select("datetime_id", "pickup_datetime", "dropoff_datetime"),
        (df.tpep_pickup_datetime  == F.col("pickup_datetime")) &
        (df.tpep_dropoff_datetime == F.col("dropoff_datetime")),
        "left"
    )

    return df.select(
        F.monotonically_increasing_id().alias("trip_id"),
        F.col("datetime_id"),
        F.col("PULocationID")  .alias("location_id"),
        F.col("VendorID")      .alias("vendor_id"),
        F.col("payment_type")  .alias("payment_id"),
        F.col("RatecodeID")    .alias("rate_id"),
        F.col("passenger_count").cast("integer"),
        F.col("trip_distance") .cast("float"),
        F.col("fare_amount")   .cast("decimal(10,2)"),
        F.col("extra")         .cast("decimal(10,2)"),
        F.col("mta_tax")       .cast("decimal(10,2)"),
        F.col("tip_amount")    .cast("decimal(10,2)"),
        F.col("tolls_amount")  .cast("decimal(10,2)"),
        F.col("improvement_surcharge").cast("decimal(10,2)"),
        F.col("total_amount")  .cast("decimal(10,2)"),
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("════════════════════════════════════════════════")
    print("  Uber Transformation Job")
    print(f"  Source : {HDFS_CLEANED}")
    print(f"  Target : Snowflake")
    print("════════════════════════════════════════════════")

    df = spark.read.parquet(HDFS_CLEANED)
    print(f"  → Rows: {df.count():,}")

    # Build dimensions
    print("\n[1/6] Building dimensions...")
    dim_datetime = build_dim_datetime(df)
    dim_location = build_dim_location(df)
    dim_vendor   = build_dim_vendor(df)
    dim_payment  = build_dim_payment(df)
    dim_rate     = build_dim_rate(df)

    # Build fact
    print("[2/6] Building fact table...")
    fact_trip = build_fact_trip(df, dim_datetime, dim_location, dim_vendor, dim_payment, dim_rate)

    # Write to Snowflake
    print("[3/6] Writing to Snowflake...")
    write_to_snowflake(dim_datetime, "DIM_DATETIME")
    write_to_snowflake(dim_location, "DIM_LOCATION")
    write_to_snowflake(dim_vendor,   "DIM_VENDOR")
    write_to_snowflake(dim_payment,  "DIM_PAYMENT")
    write_to_snowflake(dim_rate,     "DIM_RATE")
    write_to_snowflake(fact_trip,    "FACT_TRIP")

    print("\n════════════════════════════════════════════════")
    print("  [Done] Transformation completed successfully")
    print("════════════════════════════════════════════════")

    spark.stop()


if __name__ == "__main__":
    main()