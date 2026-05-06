import subprocess
import sys
import re
from datetime import datetime

HDFS_RAW        = "/uber/data/raw"
HADOOP_BIN      = "/usr/local/hadoop/bin/hdfs"
TRIPDATA_REGEX  = re.compile(r"^(yellow|green)_tripdata_\d{4}-\d{2}\.parquet$")
MIN_FILES       = 1

def hdfs_cmd(args: list[str]) -> tuple[str, int]:
    r = subprocess.run(
        ["docker", "exec", "hadoopc", HADOOP_BIN, "dfs"] + args,
        capture_output=True, text=True
    )
    return r.stdout.strip(), r.returncode

def path_exists(path: str) -> bool:
    _, code = hdfs_cmd(["-test", "-e", path])
    return code == 0

def list_tripdata_files(path: str) -> list[str]:
    stdout, code = hdfs_cmd(["-ls", path])
    if code != 0:
        return []
    files = []
    for line in stdout.splitlines():
        name = line.split()[-1].split("/")[-1]
        if TRIPDATA_REGEX.match(name):
            files.append(name)
    return files

def main(execution_date: str | None = None) -> None:
    date = execution_date or datetime.now().strftime("%Y-%m-%d")
    path = HDFS_RAW

    print("════════════════════════════════════════════════")
    print("  HDFS Validation")
    print(f"  Date : {date}")
    print(f"  Path : {path}")
    print("════════════════════════════════════════════════")

    # 1. Path exists?
    print("\n[1/2] Checking HDFS path...")
    if not path_exists(path):
        print(f"  [Failed] Not found: {path}")
        sys.exit(1)
    print("  [Done] Path exists")

    # 2. Files present?
    print("\n[2/2] Listing *_tripdata_*.parquet files...")
    files = list_tripdata_files(path)
    print(f"  → {len(files)} file(s) found:")
    for f in files:
        parts = f.replace(".parquet", "").split("_tripdata_")
        color = parts[0]
        period = parts[1] if len(parts) == 2 else "?"
        print(f"      {color:6} │ {period}")

    if len(files) < MIN_FILES:
        print(f"  [Failed] Expected >= {MIN_FILES} file(s)")
        sys.exit(1)

    print("\n════════════════════════════════════════════════")
    print(f"  [Done] Validation passed │ Files: {len(files)}")
    print("════════════════════════════════════════════════")

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
