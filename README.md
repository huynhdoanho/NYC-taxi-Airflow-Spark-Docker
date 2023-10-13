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
# How to run ?
1. Clone this project
```
git clone https://github.com/huynhdoanho/NYC-taxi-Airflow-Spark-Docker
```
2. Build
```
docker compose build
```
3. Run
```
docker compose up -d
```

4. Go to  <b>localhost:8080</b>  to check Airflow dags

5. To check Minio:  <b> localhost:9001 </b>
- User: admin
- Password: admin123

6. You can also check PostgreSQL after finish the Airflow dags
- Port: 5434
- Database: airflow
- User: postgres
- Password: postgres

7. Stop
```
docker compose down
```
