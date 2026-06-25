# ETL Test Pipeline PySpark/Airflow

Data engineering test project for processing CSV data using PySpark and Airflow.

## Stack

* Python
* PySpark
* Apache Airflow
* Docker Compose
* Parquet

## For Runing the Pipeline

Place input files into:

```text
data/input/
```

Expected files:

```text
interview.X.csv
interview.y.csv
```

Airflow is started locally using Docker Compose.

## Outputs
The pipeline writes data between processing layers:

data/bronze/
data/silver/
data/gold/

Quality check results and run metrics are saved for further validation:

data/quality/



Input CSV files are not included because of GitHub's file size limit.
