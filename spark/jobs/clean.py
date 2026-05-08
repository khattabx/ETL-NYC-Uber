"""
clean.py
────────
Reads raw parquet from HDFS, applies cleaning logic, writes cleaned parquet back to HDFS.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_RAW     = "hdfs://hadoopc:9000/uber/data/raw"
HDFS_CLEANED = "hdfs://hadoopc:9000/uber/data/cleaned"
SPARK_MASTER = "spark://spark-master:7077"


def get_spark():
    return (
        SparkSession.builder
        .appName("uber-cleaning")
        .master(SPARK_MASTER)
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoopc:9000")
        .getOrCreate()
    )


def clean(df):

    # 1. Remove duplicates
    df = df.dropDuplicates()

    # 2. Trip duration in minutes
    # TIMESTAMP_NTZ can't cast to long directly → use unix_timestamp after casting to timestamp
    df = df.withColumn(
        "trip_duration_min",
        (
            F.unix_timestamp(F.col("tpep_dropoff_datetime").cast("timestamp")) -
            F.unix_timestamp(F.col("tpep_pickup_datetime").cast("timestamp"))
        ) / 60
    )

    # 3. Remove invalid trips
    df = df.filter(
        (F.col("trip_duration_min") > 0) &
        (F.col("fare_amount")       > 0) &
        (F.col("trip_distance")     > 0) &
        (F.col("passenger_count")   > 0)
    )

    # 4. Fill missing values
    df = df.fillna({
        "passenger_count"      : 1,
        "trip_distance"        : 0.0,
        "fare_amount"          : 0.0,
        "extra"                : 0.0,
        "mta_tax"              : 0.0,
        "tip_amount"           : 0.0,
        "tolls_amount"         : 0.0,
        "improvement_surcharge": 0.0,
        "congestion_surcharge" : 0.0,
        "Airport_fee"          : 0.0,
        "cbd_congestion_fee"   : 0.0,
        "store_and_fwd_flag"   : "N",
    })

    # 5. Cap outliers at 99th percentile
    for col in ["fare_amount", "trip_distance", "trip_duration_min"]:
        q = df.approxQuantile(col, [0.99], 0.01)[0]
        df = df.withColumn(col, F.when(F.col(col) > q, q).otherwise(F.col(col)))

    # 6. Cast types
    df = df.withColumn("passenger_count", F.col("passenger_count").cast("integer"))
    df = df.withColumn("PULocationID",    F.col("PULocationID").cast("integer"))
    df = df.withColumn("DOLocationID",    F.col("DOLocationID").cast("integer"))
    df = df.withColumn("RatecodeID",      F.col("RatecodeID").cast("integer"))
    df = df.withColumn("payment_type",    F.col("payment_type").cast("integer"))
    df = df.withColumn("VendorID",        F.col("VendorID").cast("integer"))

    return df


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("════════════════════════════════════════════════")
    print("  Uber Cleaning Job")
    print(f"  Source  : {HDFS_RAW}")
    print(f"  Target  : {HDFS_CLEANED}")
    print("════════════════════════════════════════════════")

    df = spark.read.parquet(HDFS_RAW)
    print(f"  → Raw rows     : {df.count():,}")

    df = clean(df)
    print(f"  → Cleaned rows : {df.count():,}")

    df.write.mode("overwrite").parquet(HDFS_CLEANED)
    print("  [Done] Written to HDFS")

    spark.stop()


if __name__ == "__main__":
    main()