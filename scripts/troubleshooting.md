## ETL NYC Uber: Data Engineering Troubleshooting Guide

This guide addresses infrastructure, permission, and compatibility issues encountered while orchestrating a Big Data pipeline using **Airflow**, **Hadoop (HDFS)**, **Spark 4.x**, and **Snowflake**.


### 1. File System Path Misalignment (Airflow)

**Issue:** Airflow workers fail to locate shell scripts because local project paths are not mapped to the container filesystem.
**Error:** `bash /opt/airflow/scripts/ingest_to_hdfs.sh: No such file or directory`
**Solution:** Define consistent Volume Mounts in `docker-compose.yaml`.

```yaml
x-airflow-common: &airflow-common
  volumes:
    - ./hadoop/scripts:/opt/airflow/hadoop/scripts
    - ./hadoop/config:/opt/airflow/hadoop/config

```

### 2. Missing Binary Dependencies (HDFS CLI)

**Issue:** The Airflow container lacks the Hadoop binary, preventing direct HDFS commands.
**Error:** `hdfs: command not found`
**Solution:** Use **Container Orchestration (Docker Exec)** to trigger the command inside the designated Hadoop container.

```bash
# Correct Approach: Proxy the command through the HDFS container
docker exec hadoopc /usr/local/hadoop/bin/hdfs dfs -mkdir -p "$HDFS_TARGET"

```

### 3. Docker-in-Docker Permission Denied

**Issue:** The Airflow user (UID 50000) lacks the privileges to communicate with the Docker socket (`/var/run/docker.sock`).
**Solution:** Align the Container Group ID with the Host Docker GID.

1. Identify Host GID: `getent group docker | cut -d: -f3`
2. Map in `.env`: `DOCKER_GID=967`
3. Update `docker-compose.yaml`:

```yaml
user: "50000:${DOCKER_GID}"

```

### 4. Spark Worker Write Access

**Issue:** The Spark worker process cannot initialize its work directory due to restricted container permissions.
**Error:** `java.io.IOException: Failed to create directory /opt/spark/work/app-xxx/0`
**Solution:** Force the Spark worker to run as `root` within its container context.

```yaml
spark-worker:
  user: "root"

```

### 5. HDFS Network Interface Binding (Loopback Issue)

**Issue:** The NameNode is bound to `localhost`, making it unreachable from Spark or Airflow containers.
**Evidence:** `tcp LISTEN 0 256 127.0.0.1:9000`
**Solution:** Explicitly set the HDFS URI to the container hostname in `core-site.xml`.

```xml
<property>
    <name>fs.default.name</name>
    <value>hdfs://hadoopc:9000</value>
</property>

```

### 6. HDFS Access Control Lists (ACLs)

**Issue:** Spark jobs running as user `spark` cannot write to HDFS directories owned by `root`.
**Error:** `AccessControlException: Permission denied: user=spark, access=WRITE`
**Solution:** Adjust HDFS directory permissions.

```bash
sudo chmod 666 /var/run/docker.sock # Allow Spark container to access Docker socket
docker exec hadoopc /usr/local/hadoop/bin/hdfs dfs -chmod 777 /uber/data # Allow Spark to write to HDFS

```

### 7. Explicit Pathing for Spark Binaries

**Issue:** `spark-submit` is not included in the container's global `$PATH`.
**Solution:** Utilize the absolute path in your Airflow `BashOperator` or Python definitions.

```python
SPARK_SUBMIT = "/opt/spark/bin/spark-submit --master spark://spark-master:7077"

```

### 8. Schema Evolution: Timestamp Casting (Spark 4.x)

**Issue:** Spark 4.x introduced stricter casting rules for `TIMESTAMP_NTZ`. Direct casting to `BIGINT/LONG` is no longer supported.
**Error:** `cannot cast "TIMESTAMP_NTZ" to "BIGINT"`
**Solution:** Cast to a standard `timestamp` before converting to Unix epoch.

```python
from pyspark.sql import functions as F

df = df.withColumn(
    "trip_duration_min",
    (F.unix_timestamp(F.col("tpep_dropoff_datetime").cast("timestamp")) -
     F.unix_timestamp(F.col("tpep_pickup_datetime").cast("timestamp"))) / 60
)

```

### 9. Dependency Conflict: Snowflake & Scala 2.13

**Issue:** The Snowflake Spark Connector is often compiled for Scala 2.12, causing class-loading errors in Spark 4.x (which uses Scala 2.13).
**Error:** `java.lang.ClassNotFoundException: scala.Serializable`
**Solution:** Decouple the Write and Load phases. Use Spark to write processed data to HDFS as CSV/Parquet, then use a Python-based loader (Snowflake Connector for Python) to ingest data into Snowflake.

### 10. Memory Management: OOM during Snowflake Load

**Issue:** Using `pd.read_csv()` or fetching full HDFS files into memory causes the container to crash (Exit Code -9).
**Solution:** Implement **Generators and Streaming**. Stream the data from HDFS and insert it into Snowflake in micro-batches.

```python
def stream_hdfs_to_snowflake(hdfs_path):
    # Use subprocess to pipe HDFS 'cat' output directly into a Python generator
    # This maintains a constant memory footprint regardless of file size
    ...
    for batch in get_batches(reader, size=1000):
        cursor.executemany(insert_sql, batch)

```

### 11. Resource Starvation (Spark Cluster)

**Issue:** New jobs remain in `WAITING` status because previous failed or zombie jobs have consumed all available CPU cores.
**Fix:** Monitor the Spark Master UI (`localhost:8080`) and terminate stalled applications using the UI or the CLI:

```bash
docker exec spark-master /opt/spark/bin/spark-submit --kill <app-id>

```
