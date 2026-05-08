"""
load_to_snowflake.py
────────────────────
Loads CSV files from HDFS → Snowflake in streaming batches.
"""

import subprocess
import os
import csv
import io
import snowflake.connector

HDFS_STAR  = "/uber/data/star"
HADOOP_BIN = "/usr/local/hadoop/bin/hdfs"
HADOOP_CTR = "hadoopc"
BATCH_SIZE = 1000  

SF_CONN = {
    "account"  : os.getenv("SNOWFLAKE_URL", "").replace(".snowflakecomputing.com", ""),
    "user"     : os.getenv("SNOWFLAKE_USER"),
    "password" : os.getenv("SNOWFLAKE_PASSWORD"),
    "database" : os.getenv("SNOWFLAKE_DATABASE", "UBER_DWH"),
    "schema"   : os.getenv("SNOWFLAKE_SCHEMA",   "PUBLIC"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
}

TABLES = [
    "DIM_DATETIME",
    "DIM_LOCATION",
    "DIM_VENDOR",
    "DIM_PAYMENT",
    "DIM_RATE",
    "FACT_TRIP",
]


def list_csv_files(table: str) -> list[str]:
    path = f"{HDFS_STAR}/{table}"
    result = subprocess.run(
        ["docker", "exec", HADOOP_CTR, HADOOP_BIN, "dfs", "-ls", path],
        capture_output=True, text=True
    )
    return [
        line.split()[-1]
        for line in result.stdout.splitlines()
        if line.endswith(".csv")
    ]


def stream_csv_from_hdfs(hdfs_path: str):
    """Stream CSV line by line from HDFS — no full load in memory."""
    process = subprocess.Popen(
        ["docker", "exec", HADOOP_CTR, HADOOP_BIN, "dfs", "-cat", hdfs_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True
    )
    reader = csv.reader(process.stdout)
    header = next(reader, None)
    yield header
    for row in reader:
        yield row
    process.stdout.close()
    process.wait()


def load_table(cur, table: str):
    print(f"\n  Loading {table}...")

    files = list_csv_files(table)
    if not files:
        print(f"  [Skip] No CSV files found for {table}")
        return

    cur.execute(f"TRUNCATE TABLE IF EXISTS {table}")
    total_rows = 0

    for file in files:
        stream    = stream_csv_from_hdfs(file)
        header    = next(stream, None)
        if not header:
            continue

        columns      = ", ".join(header)
        placeholders = ", ".join(["%s"] * len(header))
        insert_sql   = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        batch = []
        for row in stream:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                cur.executemany(insert_sql, batch)
                total_rows += len(batch)
                batch = []

        if batch:
            cur.executemany(insert_sql, batch)
            total_rows += len(batch)

    print(f"  [Done] {table} → {total_rows:,} rows")


def main():
    print("════════════════════════════════════════════════")
    print("  Snowflake Load Job")
    print(f"  Source : HDFS {HDFS_STAR}")
    print(f"  Target : {SF_CONN['database']}.{SF_CONN['schema']}")
    print("════════════════════════════════════════════════")

    conn = snowflake.connector.connect(**SF_CONN)
    cur  = conn.cursor()

    try:
        for table in TABLES:
            load_table(cur, table)
        conn.commit()
        print("\n════════════════════════════════════════════════")
        print("  [Done] All tables loaded to Snowflake ✅")
        print("════════════════════════════════════════════════")
    except Exception as e:
        conn.rollback()
        print(f"\n  [Failed] {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()