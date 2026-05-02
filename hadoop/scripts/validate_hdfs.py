import subprocess
import sys
import re
from datetime import datetime

# Docker-compose env
HDFS_NAMENODE   = "hdfs://hadoopc:9000"
HDFS_RAW_BASE   = "/data/uber/raw"
SPARK_MASTER    = "spark://spark-master:7077"
TRIPDATA_REGEX  = re.compile(r"^(yellow|green)_tripdata_\d{4}-\d{2}\.parquet$")
MIN_FILES       = 1
MIN_ROWS        = 1


def hdfs_cmd(args: list[str]) -> tuple[str, int]:
    r = subprocess.run(["hdfs", "dfs"] + args, capture_output=True, text=True)
    return r.stdout.strip(), r.returncode


def path_exists(path: str) -> bool:
    _, code = hdfs_cmd(["-test", "-e", path])
    return code == 0


def list_tripdata_files(path: str) -> list[str]:
    """Return only files matching *_tripdata_YYYY-MM.parquet"""
    stdout, code = hdfs_cmd(["-ls", path])
    if code != 0:
        return []
    files = []
    for line in stdout.splitlines():
        name = line.split()[-1].split("/")[-1]
        if TRIPDATA_REGEX.match(name):
            files.append(name)
    return files


def spark_row_count(hdfs_path: str) -> int:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("HDFS-Validation")
        .master(SPARK_MASTER)
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    count = spark.read.parquet(hdfs_path).count()
    spark.stop()
    return count


def main(execution_date: str | None = None) -> None:
    date  = execution_date or datetime.utcnow().strftime("%Y-%m-%d")
    year, month, day = date.split("-")
    hour  = datetime.utcnow().strftime("%H")

    partition = f"{HDFS_NAMENODE}{HDFS_RAW_BASE}/year={year}/month={month}/day={day}/hour={hour}"

    print("════════════════════════════════════════════════")
    print("  HDFS Validation")
    print(f"  Date      : {date}  Hour: {hour}")
    print(f"  Partition : {partition}")
    print("════════════════════════════════════════════════")

    # 1. Path exists?
    print("\n[1/3] Checking HDFS partition path...")
    if not path_exists(partition):
        print(f"  ✗ Not found: {partition}")
        sys.exit(1)
    print("  ✓ Path exists")

    # 2. Valid tripdata files present?
    print("\n[2/3] Listing *_tripdata_*.parquet files...")
    files = list_tripdata_files(partition)
    print(f"  → {len(files)} file(s) found:")
    for f in files:
        # Parse color + period for readable output
        parts = f.replace(".parquet", "").split("_tripdata_")
        color, period = parts[0], parts[1] if len(parts) == 2 else ("?", "?")
        print(f"      {color:6} │ {period}")

    if len(files) < MIN_FILES:
        print(f"  ✗ Expected >= {MIN_FILES} file(s)")
        sys.exit(1)
    print("  ✓ File count OK")

    # 3. Spark can read?
    print("\n[3/3] Spark row count validation...")
    try:
        rows = spark_row_count(partition)
        print(f"  → Row count: {rows:,}")
        if rows < MIN_ROWS:
            print(f"  ✗ Expected >= {MIN_ROWS} rows")
            sys.exit(1)
        print("  ✓ Spark read OK")
    except Exception as e:
        print(f"  ✗ Spark failed: {e}")
        sys.exit(1)

    print("\n════════════════════════════════════════════════")
    print(f"  [Done] Validation passed │ Files: {len(files)} │ Rows: {rows:,}")
    print("════════════════════════════════════════════════")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
    