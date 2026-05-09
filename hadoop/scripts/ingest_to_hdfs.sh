#!/bin/bash
set -uo pipefail

source /opt/airflow/hadoop/config/hdfs_paths.env

# Args
EXECUTION_DATE="${1:-$(date +%Y-%m-%d)}"
EXECUTION_HOUR="${2:-$(date +%H)}"
EXECUTION_MINUTE="${3:-$(date +%M)}"

MARKER_DIR="/tmp/uber_ingest_markers"
MARKER_FILE="${MARKER_DIR}/${EXECUTION_DATE}_${EXECUTION_HOUR}${EXECUTION_MINUTE}.marker"
mkdir -p "$MARKER_DIR"

echo "════════════════════════════════════════════════"
echo "  Uber HDFS Ingestion"
echo "  Date    : $EXECUTION_DATE $EXECUTION_HOUR:$EXECUTION_MINUTE"
echo "  Source  : $LOCAL_SOURCE"
echo "  Target  : $HDFS_RAW"
echo "════════════════════════════════════════════════"

# 1. Ensure HDFS dir exists 
docker exec hadoopc $HADOOP_BIN dfs -mkdir -p "$HDFS_RAW"

# 2. Find new files
echo "[1/3] Scanning *_tripdata_*.parquet ..."

if [ -f "$MARKER_FILE" ]; then
    mapfile -t FILES < <(find "$LOCAL_SOURCE" -newer "$MARKER_FILE" \
        -name "$FILE_PATTERN" -type f 2>/dev/null | sort)
else
    mapfile -t FILES < <(find "$LOCAL_SOURCE" \
        -name "$FILE_PATTERN" -type f 2>/dev/null | sort)
fi

if [ ${#FILES[@]} -eq 0 ]; then
    echo "  [Done] No new files — skipping"
    exit 0
fi
echo "  → ${#FILES[@]} file(s) found"

# 3. Upload 
echo "[2/3] Uploading..."
UPLOADED=0; SKIPPED=0; FAILED=0

for FILE in "${FILES[@]}"; do
    FILENAME=$(basename "$FILE")
    HDFS_DEST="${HDFS_RAW}/${FILENAME}"
    HADOOP_FILE="/home/hadoop/data/${FILENAME}"

    if docker exec hadoopc $HADOOP_BIN dfs -test -e "$HDFS_DEST" 2>/dev/null; then
        echo "  [Skip] $FILENAME"
        ((SKIPPED++))
        continue
    fi

    if docker exec hadoopc $HADOOP_BIN dfs -put "$HADOOP_FILE" "$HDFS_DEST" 2>/dev/null; then
        echo "  [Done] $FILENAME"
        ((UPLOADED++))
        # echo "  [Sleep] Waiting 30 min before next file..."
        # sleep 1800
    else
        echo "  [Fail] $FILENAME"
        ((FAILED++))
    fi
done

# 4. Update marker
touch "$MARKER_FILE"

echo "════════════════════════════════════════════════"
echo "[3/3] Summary → Up:$UPLOADED Skip:$SKIPPED Fail:$FAILED"
echo "════════════════════════════════════════════════"

[ "$FAILED" -gt 0 ] && exit 1
echo "  [Done] Ingestion completed successfully"
