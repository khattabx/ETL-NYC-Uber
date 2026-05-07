from pyspark.sql import functions as F

def clean_spark_uber(df):

    # 1. Remove duplicates
    df = df.dropDuplicates()

    # 2. Create duration
    df = df.withColumn(
        "trip_duration",
        (F.col("tpep_dropoff_datetime").cast("long") -
         F.col("tpep_pickup_datetime").cast("long")) / 60
    )

    # 3. Remove invalid trips
    df = df.filter(
        (F.col("trip_duration") > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("trip_distance") > 0)
    )

    # 4. Handle missing values (simple)
    df = df.fillna({
        "fare_amount": 0,
        "trip_distance": 0,
        "passenger_count": 1
    })

    # 5. Cap outliers (approx using quantile approx)
    q = df.approxQuantile("fare_amount", [0.99], 0.01)[0]

    df = df.withColumn(
        "fare_amount",
        F.when(F.col("fare_amount") > q, q).otherwise(F.col("fare_amount"))
    )

    q2 = df.approxQuantile("trip_distance", [0.99], 0.01)[0]

    df = df.withColumn(
        "trip_distance",
        F.when(F.col("trip_distance") > q2, q2).otherwise(F.col("trip_distance"))
    )

    return df