# Description: 
This project is a ETL pipeline that:
- Extract monthly data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page (Yellow Taxi Trip Records & Taxi Zone Shapfile).
- Use PySpark to transform it.
- Load raw & staging data to data lake (Minio), load staging data to data warehouse (PostgreSQL) for analytic purposes.
# Tech:
- Orchestration: Airflow
- Transform: PySpark
- Containerize: Docker
- Data lake: Minio
- Data warehouse: PostgreSQL
# Overview:
![alt text](https://github.com/huynhdoanho/NYC-taxi-Airflow-Spark-Docker/blob/8155ab405139db559083a1dcac32379cc4cc3f5f/imgs/overview.png)
# Airflow dags:
![alt text](https://github.com/huynhdoanho/NYC-taxi-Airflow-Spark-Docker/blob/8155ab405139db559083a1dcac32379cc4cc3f5f/imgs/dag.png)
