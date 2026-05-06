# NYC Uber ETL Pipeline

This repository contains an end-to-end ETL pipeline for NYC Uber data using Airflow, Spark, Hadoop, and SQL.

## Project Structure

```
ETL-NYC-Uber/
│
├── airflow/            # DAGs & Airflow configs
├── data/               # Raw & processed datasets
├── hadoop/             # Hadoop configurations/files
├── images/             # Project images/assets
├── scripts/            # Utility scripts (Python, Bash)
├── spark/              # Spark jobs & code
├── sql/                # SQL queries
│
├── .gitignore
├── README.md
└── docker-compose.yaml # Services setup (Airflow, Spark, etc.)
```

## Contributing

Please see `CONTRIBUTING.md` for how to fork via SSH, add the upstream remote, and submit a PR with the required naming convention.