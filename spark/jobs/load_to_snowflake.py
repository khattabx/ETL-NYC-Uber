import os
import csv
import io
import requests
import snowflake.connector

HDFS_STAR  = "/uber/data/star"
WEBHDFS    = "http://hadoopc:9870/webhdfs/v1"
BATCH_SIZE = 1000

_sf_url = os.getenv("SNOWFLAKE_URL", "").replace("https://", "").replace(".snowflakecomputing.com", "")

SF_CONN = {
    "account"  : os.getenv("SNOWFLAKE_ACCOUNT") or _sf_url,
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
    url  = f"{WEBHDFS}{path}?op=LISTSTATUS&user.name=hadoop"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        files = resp.json()["FileStatuses"]["FileStatus"]
        return [
            f"{path}/{f['pathSuffix']}"
            for f in files
            if f["pathSuffix"].endswith(".csv")
        ]
    except Exception as e:
        print(f"  [Warn] Could not list {path}: {e}")
        return []

def stream_csv_from_hdfs(hdfs_path: str):
    namenode_url = (
        f"{WEBHDFS}{hdfs_path}"
        f"?op=OPEN&user.name=hadoop&noredirect=true"
    )
    r = requests.get(namenode_url, timeout=30, allow_redirects=False)
    datanode_url = r.json().get("Location") or r.headers.get("Location")

    resp = requests.get(datanode_url, stream=True, timeout=120)
    resp.raise_for_status()

    buffer = ""
    reader_started = False
    header = None

    for chunk in resp.iter_content(chunk_size=65536, decode_unicode=False):
        # Ensure chunk is decoded to string
        if chunk:
            if isinstance(chunk, bytes):
                chunk = chunk.decode("utf-8")
            buffer += chunk
        
        lines = buffer.split("\n")
        buffer = lines[-1]

        for line in lines[:-1]:
            if not line.strip():
                continue
            row = next(csv.reader([line]))
            if not reader_started:
                header = row
                reader_started = True
                yield header
            else:
                yield row

    if buffer.strip():
        yield next(csv.reader([buffer]))

def load_table(cur, table: str):
    print(f"\n  Loading {table}...")
    files = list_csv_files(table)
    if not files:
        print(f"  [Skip] No CSV files found for {table}")
        return

    total_rows = 0
    for file in files:
        stream = stream_csv_from_hdfs(file)
        header = next(stream, None)
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
    print(f"  Account: {SF_CONN['account']}")
    print("════════════════════════════════════════════════")

    if not SF_CONN["account"] or not SF_CONN["user"] or not SF_CONN["password"]:
        raise ValueError("Snowflake credentials missing — check .env file")

    conn = snowflake.connector.connect(**SF_CONN)
    cur  = conn.cursor()
    try:
        for table in TABLES:
            load_table(cur, table)
        conn.commit()
        print("\n  [Done] All tables loaded to Snowflake ✅")
    except Exception as e:
        conn.rollback()
        print(f"\n  [Failed] {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()