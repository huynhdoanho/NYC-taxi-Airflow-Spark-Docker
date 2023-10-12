from pyspark.sql import SparkSession
from airflow.operators.python_operator import PythonOperator
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from pyspark.context import SparkContext

###############################################
# Parameters
###############################################
# spark_conn = os.environ.get("spark_conn", "spark_conn")
# spark_master = "spark://spark-master:7077"
# spark_app_name = "Spark Hello World"

###############################################
# DAG Definition
###############################################


spark = SparkSession.builder\
    .appName("NYC")\
    .getOrCreate()

# spark.conf.set("spark.executor.memory", '4g')
# spark.conf.set('spark.executor.cores', '3')
# spark.conf.set('spark.cores.max', '3')
# spark.conf.set("spark.driver.memory", '4g')

def hello():
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j

    LOGGER = log4jLogger.LogManager.getLogger(__name__)


    LOGGER.info("Running Application")
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7])
    LOGGER.info(f"RDD Count => {rdd.count()}")


now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="spark-test",
    description="This DAG runs a simple Pyspark app.",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

start = DummyOperator(task_id="start", dag=dag)

# spark_job = SparkSubmitOperator(
#     task_id="spark_job",
#     # Spark application path created in airflow and spark cluster
#     application="/opt/bitnami/spark/app/hello-world.py",
#     name=spark_app_name,
#     conn_id=spark_conn,
#     verbose=1,
#     conf={"spark.master": spark_master},
#     # application_args=[file_path],
#     dag=dag)

task = PythonOperator(
    task_id="hello",
    python_callable=hello,
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)


start >> task >> end

