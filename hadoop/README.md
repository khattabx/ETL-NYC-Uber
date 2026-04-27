# Hadoop Service

Pseudo-distributed Hadoop cluster running **NameNode + DataNode + ResourceManager + NodeManager** in a single container.

| | |
|---|---|
| **Image** | `asami76/hadoop-pseudo:v1.0` |
| **Hadoop** | 3.3.1 |
| **JDK** | 8 |
| **Mode** | Pseudo-Distributed |

---

## Web UIs

| Service | URL |
|---------|-----|
| HDFS NameNode | http://localhost:9870 |
| HDFS DataNode | http://localhost:9864 |
| YARN ResourceManager | http://localhost:8088 |
| YARN NodeManager | http://localhost:8042 |
| MapReduce History | http://localhost:19888 |

---

## Upload Data to HDFS

```bash
# Copy a local file into HDFS
docker exec hadoopc hdfs dfs -put /home/hadoop/data/yellow_tripdata_2025-01.parquet /data/raw/

# List files in HDFS
docker exec hadoopc hdfs dfs -ls /data/raw/

# Check HDFS disk usage
docker exec hadoopc hdfs dfs -du -h /data/
```

---

## Notes

> **Data is wiped on every restart** — `bootstrap.sh` formats the NameNode each time.
> This is intentional: data is re-uploaded by the Airflow DAG on every pipeline run.

> **Hostname `hadoopc` is required** — other services (Spark, Airflow) connect to HDFS via `hdfs://hadoopc:9000`. Do not change the container hostname.