![Project Banner](link-to-your-image)

# NYC Uber ETL Pipeline
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebooks-F37626?logo=jupyter&logoColor=white)
![Apache Hadoop](https://img.shields.io/badge/Apache%20Hadoop-HDFS-66CCFF?logo=apachehadoop&logoColor=black)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Standalone-E25A1C?logo=apachespark&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?logo=apacheairflow&logoColor=white)

## Overview

An end-to-end, containerized ETL pipeline for NYC TLC taxi trip Parquet data that ingests raw files into HDFS on a schedule, validates the landing zone, and provides a Spark-based transformation layer designed to load curated (star-schema) tables into a SQL Server data warehouse.

![Pipeline](./images/pipeline.png)

Core stack: 
```python
Docker Compose,
Apache Airflow (orchestration),
Hadoop HDFS (raw layer), 
Apache Spark (clean/transform), 
Microsoft SQL Server (DWH), 
PostgreSQL (Airflow metadata), 
Python/Bash, and Jupyter (EDA).
```

## Project Structure

```txt
.
├── airflow/
│   └── dags/
│       └── ingestion_dag.py          # Airflow DAG: ingest -> validate -> log/alert
├── data/                             # Local Parquet drop-zone (bind-mounted into containers)
├── hadoop/
│   ├── config/
│   │   └── hdfs_paths.env            # Shared paths/patterns for ingestion scripts
│   ├── images/
│   │   └── ingestflow.png
│   ├── scripts/
│   │   ├── ingest_to_hdfs.sh         # Upload Parquet into HDFS raw path
│   │   └── validate_hdfs.py          # Validate raw layer contents in HDFS
│   └── README.md
├── spark/
│   ├── images/
│   │   └── transformLayer.png
│   ├── jobs/                         # Spark production jobs (currently scaffolding/WIP)
│   ├── notebooks/                    # EDA + cleaning helpers (pandas + PySpark)
│   └── README.md
├── DWH/                              # Data warehouse assets (currently empty/WIP)
├── images/                           # Repo-level assets (banner, dashboard screenshots, etc.)
├── scripts/                          # Misc scripts (mounted into Airflow; currently empty)
├── docker-compose.yaml               # Local stack: Hadoop, Spark, MSSQL, Airflow + Postgres
├── .env.example                      # Environment template (copy to .env)
├── CONTRIBUTING.md
└── .gitignore
```

Key directories:
- `airflow/`: scheduling and orchestration (DAGs).
- `hadoop/`: HDFS runtime plus ingestion/validation scripts used by Airflow.
- `spark/`: transformation layer (notebooks today; `jobs/` intended for `spark-submit` workflows).
- `data/`: local file drop-zone that gets bind-mounted into Hadoop/Spark/Airflow.

## Notes

Quickstart (local, Docker Compose):

```sh
cp .env.example .env
docker compose up -d hadoop spark-master spark-worker mssql airflow-postgres
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

How ingestion works:
- Drop NYC TLC Parquet files into `./data/` (expected pattern: `*_tripdata_*.parquet`, e.g. `yellow_tripdata_2025-01.parquet`).
- Airflow runs `uber_ingestion_pipeline` every 15 minutes (`airflow/dags/ingestion_dag.py`).
- The DAG calls `hadoop/scripts/ingest_to_hdfs.sh` to upload new files into `HDFS_RAW=/uber/data/raw`, then `hadoop/scripts/validate_hdfs.py` to verify the raw layer.

Environment variables (high-signal):

| Variable | Source | Why it matters |
|---|---|---|
| `AIRFLOW_FERNET_KEY` | `.env` | Required for Airflow to start and manage encrypted connections/variables. |
| `AIRFLOW_SECRET_KEY` | `.env` | Webserver/session secret key. |
| `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_ADMIN_EMAIL` | `.env` | Bootstraps the admin user via `airflow-init`. |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | `.env` | Airflow metadata database (Postgres container). |
| `MSSQL_SA_PASSWORD` | `.env` | SQL Server will refuse to start if the password is not strong enough. |
| `MSSQL_JDBC_URL` | `.env` | JDBC URL passed into Airflow tasks for DWH loads. |
| `DOCKER_GID` | `.env` | Fixes permissions for `/var/run/docker.sock` mounted into Airflow. |

Gotchas:
- Airflow ingestion/validation tasks run `docker exec hadoopc ...`; the Airflow container must be able to talk to the Docker socket and have a working `docker` client available.
- If you hit `permission denied` on `/var/run/docker.sock`, set `DOCKER_GID` in `.env` (see the comment at the top of `docker-compose.yaml`).
- `docker-compose.yaml` bind-mounts `./mssql/init.sql`, but the repository currently has no `mssql/` directory; create it (and `init.sql`) or update/remove that mount.
- The Hadoop base image may clear data on container teardown; treat HDFS as ephemeral unless you add persistence (see `hadoop/README.md`).

Service UIs and ports:
- Airflow UI: http://localhost:8082
- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081
- Spark Application UI: http://localhost:4040
- HDFS NameNode UI: http://localhost:9870
- YARN ResourceManager: http://localhost:8088
- SQL Server: `localhost:1433`

## Links
- Hadoop docs: [./hadoop/README.md](hadoop/README.md)
- Spark docs: [./spark/README.md](spark/README.md)
- Contributing: [./CONTRIBUTING.md](CONTRIBUTING.md)

## Dashboard
