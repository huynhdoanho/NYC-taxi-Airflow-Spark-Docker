import datetime
from datetime import timedelta
import shapefile
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from minio import Minio
from zipfile import ZipFile
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import hour, date_format, col
import logging
import os
from pyspark.conf import SparkConf

conf = SparkConf().setAppName("NYC_taxi")\
            .setMaster("local[*]")\
            .set("spark.dynamicAllocation.enabled", "true")\
            .set("spark.executor.memory", "1g")\
            .set('spark.executor.cores', '1')\
            .set("spark.dynamicAllocation.minExecutors", "1")\
            .set("spark.dynamicAllocation.maxExecutors", "3")\
            .set("spark.jars", "/postgresql-42.6.0.jar")

#            .set("spark.jars", "/mysql-connector-java-8.0.30.jar")

# Create a SparkSession object
spark = SparkSession.builder.config(conf=conf).getOrCreate()


current_time = datetime.datetime.now()
current_month = '01'
current_year = current_time.year

# MySQL config
# mysql_properties = {
#     "user": "admin",
#     "password": "admin123",
#     "driver": "com.mysql.jdbc.Driver"
# }
# mysql_url = "jdbc:mysql://mysql:3306/nyc_taxi"

# PostgreSQL config
postgres_url = "jdbc:postgresql://postgres:5432/airflow"

postgres_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


def load_file_to_minio(path, file_name, key_prefix):

    client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    bucket = "warehouse"

    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)

    client.fput_object(
        bucket,
        key_prefix + file_name,
        path + file_name
    )


def load_shape_file_to_minio():

    # unzip file
    with ZipFile("/home/workdir/taxi_zones.zip", 'r') as zObject:
        zObject.extractall(
            path="/home/workdir/taxi_zones")

    client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    bucket = "warehouse"

    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)

    local_directory_path = "/home/workdir/taxi_zones"

    for root, dirs, files in os.walk(local_directory_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            object_name = os.path.relpath(local_file_path, local_directory_path)

            # Upload the file to MinIO
            client.fput_object(
                bucket,
                f"shapefile/{object_name}",
                local_file_path
            )


def transform(**context):

    df = spark.read.option("header", "true").parquet(f'/home/workdir/yellow_tripdata_{current_year}-{current_month}.parquet')

    # not_time_stamp = df.columns
    # not_time_stamp.remove('tpep_pickup_datetime')
    # not_time_stamp.remove('tpep_dropoff_datetime')
    #
    # number_cols = not_time_stamp
    # number_cols.remove("store_and_fwd_flag")
    #
    # NaN values
    #df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in not_time_stamp]).show()
    df = df.na.drop()

    # extract pickup/dropoff hour
    df = df.withColumn("pickup_hour", hour(df["tpep_pickup_datetime"]))
    df = df.withColumn("dropoff_hour", hour(df["tpep_dropoff_datetime"]))

    # trip duration
    df = df.withColumn("trip_duration", (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).substr(12, 9))

    # pickup/dropoff weekday
    df = df.withColumn("pickup_weekday", date_format(col("tpep_pickup_datetime"), "EEEE"))
    df = df.withColumn("dropoff_weekday", date_format(col("tpep_dropoff_datetime"), "EEEE"))

    for col_name in df.columns:
        df = df.withColumn(col_name, col(col_name).cast('string'))

    df.write\
        .option("header", "true")\
        .mode("overwrite")\
        .parquet(f'/home/workdir/yellow_tripdata_{current_year}-{current_month}_transformed.parquet')


def create_location_file():

    sf = shapefile.Reader('/home/workdir/taxi_zones/taxi_zones.shp')

    fields_name = [field[0] for field in sf.fields[1:]]
    shp_dic = dict(zip(fields_name, list(range(len(fields_name)))))

    attributes = sf.records()

    shp_attr = [dict(zip(fields_name, attr)) for attr in attributes]

    attribute_df = spark.createDataFrame(shp_attr)
    attribute_df.show()

    def get_lat_lon(sf):
        content = []
        for sr in sf.shapeRecords():
            shape = sr.shape
            rec = sr.record
            loc_id = rec[shp_dic['LocationID']]

            x = (shape.bbox[0] + shape.bbox[2]) / 2
            y = (shape.bbox[1] + shape.bbox[3]) / 2

            content.append((loc_id, x, y))

            columns = ["LocationID", "longtitude", "lattitude"]
        return spark.createDataFrame(content, columns)

    long_lat = get_lat_lon(sf)
    loc_df = attribute_df.join(long_lat, on="LocationID").sort("LocationID")

    loc_df.toPandas().to_csv("/home/workdir/location.csv", index=False)


def load_parquet_to_db():
    df = spark.read\
        .option("header", "true")\
        .parquet(f'/home/workdir/yellow_tripdata_{current_year}-{current_month}_transformed.parquet')

    # logging.info(df.show(5))
    # logging.info(df.printSchema())
    df.write\
        .option("header", "true")\
        .jdbc(postgres_url,
              f"yellow_tripdata_{current_year}_{current_month}",
              mode="overwrite",
              properties=postgres_properties
              )


def load_location_file_to_db():
    location_df = spark.read\
        .format("csv")\
        .option("header", "true")\
        .load("/home/workdir/location.csv")
    # logging.info(location_df.head(5))
    # logging.info(location_df.printSchema())
    location_df.write\
        .jdbc(postgres_url,
              "location",
              mode="overwrite",
              properties=postgres_properties
              )




default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
  dag_id="nyc_taxi",
  schedule_interval="@daily",
  start_date=airflow.utils.dates.days_ago(1),
)

start = DummyOperator(
    task_id="Start",
    dag=dag
)

end = DummyOperator(
    task_id="End",
    dag=dag
)

extract_monthly_data = BashOperator(
    task_id='extract_monthly_data',
    bash_command=f"curl -o /home/workdir/yellow_tripdata_{current_year}-{current_month}.parquet \
    --url https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_year}-{current_month}.parquet",
    dag=dag
)

upload_parquet_file_to_minio = PythonOperator(
    task_id="upload_parquet_file_to_minio",
    python_callable=load_file_to_minio,
    op_kwargs={
        "path": "/home/workdir/",
        "file_name": f"yellow_tripdata_{current_year}-{current_month}.parquet",
        "key_prefix": "raw/"
    },
    dag=dag
)

extract_shape_file = BashOperator(
    task_id='extract_shape_file',
    bash_command=f"curl -o /home/workdir/taxi_zones.zip \
    --url https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip",
    dag=dag
)

upload_shapefile_to_minio = PythonOperator(
    task_id="upload_shapefile_to_minio",
    python_callable=load_shape_file_to_minio,
    dag=dag
)

transform_data = PythonOperator(
    task_id="transform_data",
    python_callable=transform,
    provide_context=True,
    dag=dag
)

create_location_file = PythonOperator(
    task_id="create_location_file",
    python_callable=create_location_file,
    dag=dag
)

upload_location_file_to_minio = PythonOperator(
    task_id="upload_location_file_to_minio",
    python_callable=load_file_to_minio,
    op_kwargs={
        "path": "/home/workdir/",
        "file_name": "location.csv",
        "key_prefix": "stg/"
    },
    dag=dag
)

upload_transformed_file_to_minio = PythonOperator(
    task_id="upload_transformed_file_to_minio",
    python_callable=load_file_to_minio,
    op_kwargs={
        "path": "/home/workdir/",
        "file_name": f'yellow_tripdata_{current_year}-{current_month}_transformed.parquet',
        "key_prefix": "stg/"
    },
    dag=dag
)

load_location_to_postgres = PythonOperator(
    task_id="load_location_to_postgres",
    python_callable=load_location_file_to_db,
    dag=dag
)

load_parquet_to_postgres = PythonOperator(
    task_id="load_parquet_to_postgres",
    python_callable=load_parquet_to_db,
    provide_context=True,
    dag=dag
)

start >> [extract_shape_file, extract_monthly_data]

extract_shape_file >> [upload_shapefile_to_minio, create_location_file]
create_location_file >> [upload_location_file_to_minio, load_location_to_postgres]

extract_monthly_data >> [upload_parquet_file_to_minio, transform_data]
transform_data >> [load_parquet_to_postgres, upload_transformed_file_to_minio]

[load_parquet_to_postgres, load_location_to_postgres] >> end
