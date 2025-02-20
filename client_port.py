import os
import sys

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import datetime
from pyspark.sql.window import Window
 
def input_event_paths(date, depth, events_base_path):
	dt = datetime.datetime.strptime(date, '%Y-%m-%d')
	return [f"{events_base_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" 
	for x in range(int(depth))]

def partition_writer(candidates):
    return candidates.write.mode('overwrite').format('parquet')
 
#def calculate_user_interests(date, depth, spark):
def main():
    date = sys.argv[1]                 # день расчёта
    depth = sys.argv[2]                # глубина расчёта
    events_base_path = sys.argv[3]     # путь к таблице с эвентами
    output_base_path = sys.argv[4]     # куда сохранять результат

    conf = SparkConf().setAppName(f'user_interests-{date}')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    message_paths = input_event_paths(date, depth)
    tag_tops = sql.read\
    .option("basePath", {events_base_path})\
    .parquet(*message_paths)\
    .where("event_type = 'message'")\
		.where("event.message_channel_to is not null")\
		.select(F.col("event.message_id").alias("message_id"),
		        F.col("event.message_from").alias("user_id"),
		        F.explode(F.col("event.tags")).alias("tag"))\
		.groupBy("user_id", "tag")\
		.agg(F.count("*").alias("tag_count"))\
		.withColumn("rank", F.row_number().over(Window.partitionBy("user_id")\
        .orderBy(F.desc("tag_count"), F.desc("tag"))))\
		.where("rank <= 3")\
		.groupBy("user_id")\
		.pivot("rank", [1, 2, 3])\
		.agg(F.first("tag"))\
		.withColumnRenamed("1", "tag_top_1")\
		.withColumnRenamed("2", "tag_top_2")\
		.withColumnRenamed("3", "tag_top_3")
    
    reaction_paths = input_event_paths(date, depth)
    reactions = sql.read\
    .option("basePath", {events_base_path})\
    .parquet(*reaction_paths)\
    .where("event_type='reaction'")
    all_message_tags = sql.read.parquet("/user/msdelendik/data/events")\
		.where("event_type='message' and event.message_channel_to is not null")\
		.select(F.col("event.message_id").alias("message_id"),
		        F.col("event.message_from").alias("user_id"),
		        F.explode(F.col("event.tags")).alias("tag")
		)
    reaction_tags = reactions\
		.select(F.col("event.reaction_from").alias("user_id"), 
		        F.col("event.message_id").alias("message_id"), 
		        F.col("event.reaction_type").alias("reaction_type")
		).join(all_message_tags.select("message_id", "tag"), "message_id")
    reaction_tops = reaction_tags\
		.groupBy("user_id", "tag", "reaction_type")\
		.agg(F.count("*").alias("tag_count"))\
		.withColumn("rank", F.row_number().over(Window.partitionBy("user_id", "reaction_type")\
        .orderBy(F.desc("tag_count"), F.desc("tag"))))\
		.where("rank <= 3")\
		.groupBy("user_id", "reaction_type")\
		.pivot("rank", [1, 2, 3])\
		.agg(F.first("tag"))\
		.cache()
          
    like_tops = reaction_tops\
		.where("reaction_type = 'like'")\
		.drop("reaction_type")\
		.withColumnRenamed("1", "like_tag_top_1")\
		.withColumnRenamed("2", "like_tag_top_2")\
		.withColumnRenamed("3", "like_tag_top_3")
          
    dislike_tops = reaction_tops\
		.where("reaction_type = 'dislike'")\
		.drop("reaction_type")\
		.withColumnRenamed("1", "dislike_tag_top_1")\
		.withColumnRenamed("2", "dislike_tag_top_2")\
		.withColumnRenamed("3", "dislike_tag_top_3")
    
    candidates = like_tops\
		.join(dislike_tops, "user_id", "full_outer").join(tag_tops, "user_id", "full_outer")
          
    writer = partition_writer(candidates)
    writer.save(f'{output_base_path}/date={date}')
 	#candidates.write.mode("overwrite").parquet(f'{base_output_path}/date={date}')
if __name__ == "__main__":
    main