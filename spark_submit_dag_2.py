from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2020, 1, 1),
                                }

dag_spark = DAG(
                        dag_id = "sparkoperator_dag",
                        default_args=default_args,
                        schedule_interval=None,
                        )

# объявляем задачу с помощью SparkSubmitOperator
spark_submit_local = SparkSubmitOperator(
                    task_id='user_interests_d5',
                    dag=dag_spark,
                    application ='/lessons/client_port.py' ,
                    conn_id= 'yarn_spark',
                    application_args = ["2022-05-04", "5", "/user/msdelendik/data/events", "/user/msdelendik/analytics/user_interests_d5"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

spark_submit_local