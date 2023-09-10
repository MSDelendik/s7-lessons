import os
import sys

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import datetime


def input_paths(date, depth, base_input_path):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [
        f"{base_input_path}/date={(dt - datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message"
        for x in range(depth)]


def main():
    date, base_input_path, base_output_path, depth, cut = sys.argv[1:6]

    spark = SparkSession.builder \
        .master("yarn") \
        .appName(f"VerifiedTagsCandidatesJob-{date}-d{depth}-cut{cut}") \
        .getOrCreate()

    paths = input_paths(date, depth, base_input_path)
    messages = spark.read.parquet(*paths)
    all_tags = messages.where("event.message_channel_to is not null").selectExpr(
        ["event.message_from as user", "explode(event.tags) as tag"]).groupBy("tag").agg(
        F.expr("count(distinct user) as suggested_count")).where("suggested_count >= 100")
    verified_tags = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")
    candidates = all_tags.join(verified_tags, "tag", "left_anti")

    candidates.write.mode("overwrite").parquet(base_output_path)


if __name__ == "__main__":
    main()
