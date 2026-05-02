#!/bin/bash

set -uo pipefail

source /opt/airflow/hadoop/config/hdfs_paths.env

EXECUTION_DATE="${1:-$(date +%Y-%m-%d)}"
EXECUTION_HOUR="${2:-$(date +%H)}"
EXECUTION_MINUTE="${3:-$(date +%M)}"

YEAR=$(date -d "$EXECUTION_DATE" +%Y)
MONTH=$(date -d "$EXECUTION_DATE" +%m)
DAY=$(date -d "$EXECUTION_DATE" +%d)
HOUR="$EXECUTION_HOUR"

HDFS_TARGET="${HDFS_RAW_BASE}/year=${YEAR}/month=${MONTH}/day=${DAY}/hour=${HOUR}"
HADOOP_BIN="/usr/local/hadoop/bin/hdfs"
HADOOP_LOCAL_SOURCE="/home/hadoop/data"

MARKER_DIR="/tmp/uber_ingest_markers"
MARKER_FILE="${MARKER_DIR}/last_ingest_${YEAR}${MONTH}${DAY}_${HOUR}${EXECUTION_MINUTE}.marker"
mkdir -p "$MARKER_DIR"

echo "════════════════════════════════════════════════"
echo "  Uber HDFS Ingestion"
echo "  Date     : $EXECUTION_DATE $HOUR:$EXECUTION_MINUTE"
echo "  Source   : $LOCAL_SOURCE"
echo "  Target   : $HDFS_TARGET"
echo "  Pattern  : $FILE_PATTERN"
echo "════════════════════════════════════════════════"

echo ""
echo "[1/4] Creating HDFS partition directory..."
docker exec hadoopc $HADOOP_BIN dfs -mkdir -p "$HDFS_TARGET"
echo "  [Done] $HDFS_TARGET"

echo ""
echo "[2/4] Scanning for *_tripdata_*.parquet files..."

if [ -f "$MARKER_FILE" ]; then
    echo "  → Incremental: files newer than last marker"
    mapfile -t FILES < <(find "$LOCAL_SOURCE" -newer "$MARKER_FILE" \
        -name "$FILE_PATTERN" -type f 2>/dev/null | sort)
else
    echo "  → Full load: no marker found"
    mapfile -t FILES < <(find "$LOCAL_SOURCE" \
        -name "$FILE_PATTERN" -type f 2>/dev/null | sort)
fi

if [ ${#FILES[@]} -eq 0 ]; then
    echo "  [Done] No new files found — nothing to ingest"
    exit 0
fi

echo "  → Found ${#FILES[@]} file(s):"
for f in "${FILES[@]}"; do echo "      $(basename "$f")"; done

echo ""
echo "[3/4] Uploading to HDFS..."
UPLOADED=0
SKIPPED=0
FAILED=0

for FILE in "${FILES[@]}"; do
    FILENAME=$(basename "$FILE")
    COLOR=$(echo "$FILENAME" | cut -d'_' -f1)
    PERIOD=$(echo "$FILENAME" | sed 's/.*tripdata_//;s/\.parquet//')
    HDFS_DEST="${HDFS_TARGET}/${FILENAME}"
    HADOOP_FILE="${HADOOP_LOCAL_SOURCE}/${FILENAME}"

    # Idempotent: skip if already on HDFS
    if docker exec hadoopc $HADOOP_BIN dfs -test -e "$HDFS_DEST" 2>/dev/null || false; then
        echo "  [Skipped] │ $COLOR │ $PERIOD"
        ((SKIPPED++))
        continue
    fi

    # File already exists in hadoopc at /home/hadoop/data — no docker cp needed
    echo "  [Uploading] │ $COLOR │ $PERIOD │ $FILENAME"
    if docker exec hadoopc $HADOOP_BIN dfs -put "$HADOOP_FILE" "$HDFS_DEST"; then
        ((UPLOADED++))
        echo "  [Done] │ $COLOR │ $PERIOD"
    else
        echo "  [Failed] │ $COLOR │ $PERIOD"
        ((FAILED++))
    fi
done

echo ""
echo "[4/4] Updating incremental marker..."
touch "$MARKER_FILE"
echo "  [Done] Marker saved: $MARKER_FILE"

echo "════════════════════════════════════════════════"
echo "[5/5] Run Summary"
echo "  [Done]    Uploaded : $UPLOADED"
echo "  [Skipped] Skipped  : $SKIPPED"
echo "  [Failed]  Failed   : $FAILED"
echo "════════════════════════════════════════════════"

if [ "$FAILED" -gt 0 ]; then
    echo "  [Failed] Ingestion finished with $FAILED error(s)"
    exit 1
fi

echo "  [Done] Ingestion completed successfully"
