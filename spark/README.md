# Spark Service

This Spark layer is the project’s transformation engine. It runs on a standalone Spark cluster (master + worker), reads/writes from HDFS, and loads curated outputs into the SQL Server DWH. Notebooks support exploration, while production jobs live under `jobs/` and are triggered by Airflow.

## What Lives Here
```txt
spark/
├── notebooks/
│   ├── 01_explore.ipynb
│   ├── 02_cleaning.ipynb
│   └── 03_transformation.ipynb
├── jobs/
│   ├── clean.py
│   └── transform.py
└── jars/
    └── (JDBC drivers etc.)
```

## How It Connects To Airflow
- The Airflow containers mount `./spark` at `/opt/airflow/spark`, so DAG tasks can reference Spark jobs, notebooks, and jars from this folder.
- Airflow tasks receive `SPARK_MASTER_URL=spark://spark-master:7077` via environment variables, which is the endpoint for `spark-submit`.
- The same environment exposes `HDFS_URL=hdfs://hadoopc:9000` and JDBC connection settings so Spark jobs can read from HDFS and write to the DWH.

## Web UIs
- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081
- Spark Application UI: http://localhost:4040
- Jupyter Lab (PySpark notebooks): http://localhost:8888

## Process Flow
![Transform Layer](images/transformLayer.png)

## Notes 
