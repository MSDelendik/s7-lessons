import datetime
import pyspark.sql.functions as F
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
spark = SparkSession \
    .builder \
    .master("yarn") \
        .config("spark.driver.cores", "3") \
        .config("spark.driver.memory", "3g") \
        .appName("Data Lake load") \
        .getOrCreate()
def input_paths(date, depth):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"/user/msdelendik/data/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(depth)]

paths = input_paths('2022-05-31', 7)
messages = spark.read.parquet(*paths)
all_tags = messages.where("event.message_channel_to is not null").selectExpr(["event.message_from as user", "explode(event.tags) as tag"]).groupBy("tag").agg(F.expr("count(distinct user) as suggested_count")).where("suggested_count >= 100")
verified_tags = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")
candidates = all_tags.join(verified_tags, "tag", "left_anti")

candidates.write.parquet('/user/msdelendik/data/analytics/candidates_d7_pyspark')