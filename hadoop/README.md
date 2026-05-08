# Hadoop Service

This folder runs a pseudo-distributed Hadoop stack (**NameNode + DataNode + ResourceManager + NodeManager**) inside a single container, plus helper scripts for HDFS ingestion and validation.

## What Lives Here

```txt
hadoop/
├── config/
│   └── hdfs_paths.env
├── images/
│   └── ingestflow.png
└── scripts/
    ├── ingest_to_hdfs.sh
    └── validate_hdfs.py
```

## How It Connects To Airflow

The Airflow DAG at `airflow/dags/ingestion_dag.py` orchestrates the ingestion steps. It triggers the HDFS load using `scripts/ingest_to_hdfs.sh` and then runs `scripts/validate_hdfs.py` to confirm the data landed correctly.

## Web UIs

- HDFS NameNode: http://localhost:9870
- HDFS DataNode: http://localhost:9864
- YARN ResourceManager: http://localhost:8088
- YARN NodeManager: http://localhost:8042
- MapReduce History: http://localhost:19888

## Process Flow

![flow](./images/ingestflow.png)

## Note About Data Cleanup

The base image runs a `bootstrap.sh` that clears data on container teardown. Options if you need persistence:

- Override `bootstrap.sh` and rebuild the image.
- Add a custom script to back up or restore data.
- Or accept cleanup if local sources remain available.
