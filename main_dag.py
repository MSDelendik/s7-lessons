import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

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
dag_id = "datalake_etl",
default_args=default_args,
schedule_interval=None,
)

events_partitioned = SparkSubmitOperator(
task_id='events_partitioned',
dag=dag_spark,
application ='/lessons/partition_overwrite.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "/user/master/data/events", "/user/msdelendik/data/events"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

verified_tags_candidates_d7 = SparkSubmitOperator(
task_id='verified_tags_candidates_d7',
dag=dag_spark,
application ='/lessons/verified_tags_candidates.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "7", "100", "/user/msdelendik/data/events", "/user/master/data/snapshots/tags_verified/actual", "/user/msdelendik/data/analytics/verified_tags_candidates_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

verified_tags_candidates_d84 = SparkSubmitOperator(
task_id='verified_tags_candidates_d84',
dag=dag_spark,
application ='/lessons/verified_tags_candidates.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "84", "1000", "/user/msdelendik/data/events", "/user/master/data/snapshots/tags_verified/actual", "/user/msdelendik/data/analytics/verified_tags_candidates_d84"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

user_interests_d7 = SparkSubmitOperator(
task_id='user_interests_d7',
dag=dag_spark,
application ='/lessons/user_interests.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-25", "7", "/user/msdelendik/data/events", "/user/msdelendik/data/analytics/user_interests_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

user_interests_d28 = SparkSubmitOperator(
task_id='user_interests_d28',
dag=dag_spark,
application ='/lessons/user_interests.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-25", "28", "/user/msdelendik/data/events", "/user/msdelendik/data/analytics/user_interests_d28"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

user_interests_d5 = SparkSubmitOperator(
task_id='user_interests_d5',
dag=dag_spark,
application ='/lessons/user_interests.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-04", "5", "/user/msdelendik/data/events", "/user/msdelendik/analytics/user_interests_d5"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

connection_interests = SparkSubmitOperator(
task_id='connection_interests',
dag=dag_spark,
application ='/lessons/connection_interests.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-25", "7", "/user/msdelendik/data/events", "/user/msdelendik/data/analytics/user_interests_d7", "/user/master/data/snapshots/tags_verified/actual", "/user/msdelendik/data/analytics/connection_interests_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

events_partitioned \
>> [verified_tags_candidates_d7, verified_tags_candidates_d84, user_interests_d7, user_interests_d28] \
>> user_interests_d5 \
>> connection_interests