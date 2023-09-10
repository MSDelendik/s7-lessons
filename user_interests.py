import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime

def main():
        date = sys.argv[1]
        days_count = sys.argv[2]
        events_base_path = sys.argv[3]
        output_base_path = sys.argv[4]

        conf = SparkConf().setAppName(f"UserInterestsJob-{date}-d{days_count}")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

        posts_part = sql.read\
        .option("basePath", events_base_path)\
        .parquet(*input_event_paths(events_base_path, date, days_count))\
        .where("event.message_channel_to is not null and event_type = 'message'")

        posts_all = sql.read.parquet(events_base_path)\
        .where("event_type='message' and event.message_channel_to is not null")

        reactions_part = sql.read\
        .option("basePath", events_base_path)\
        .parquet(*input_event_paths(events_base_path, date, days_count))\
        .where("event_type = 'reaction'")

        result = calculate_user_interests(posts_part, posts_all, reactions_part) 
        result.write.mode("overwrite").parquet(f"{output_base_path}/date={date}")

def input_event_paths(base_path, date, depth):
        dt = datetime.datetime.strptime(date, '%Y-%m-%d')
        return [f"{base_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(int(depth))]

def calculate_user_interests(posts_part, posts_all, reactions):
        post_tops = tag_tops(posts_part)
        reaction_tops = reaction_tag_tops(posts_all, reactions)
        return post_tops.join(reaction_tops, "user_id", "full_outer")  

def tag_tops(posts):
        result = posts\
        .select(F.col("event.message_id").alias("message_id"),
                F.col("event.message_from").alias("user_id"),
                F.explode(F.col("event.tags")).alias("tag"))\
        .groupBy("user_id", "tag")\
        .agg(F.count("*").alias("tag_count"))\
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("tag_count"), F.desc("tag"))))\
        .where("rank <= 3")\
        .groupBy("user_id")\
        .pivot("rank", [1, 2, 3])\
        .agg(F.first("tag"))\
        .withColumnRenamed("1", "tag_top_1")\
        .withColumnRenamed("2", "tag_top_2")\
        .withColumnRenamed("3", "tag_top_3")
        
        return result

def reaction_tag_tops(posts_all, reactions):
        all_message_tags = posts_all\
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
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id", "reaction_type").orderBy(F.desc("tag_count"), F.desc("tag"))))\
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

        result = like_tops\
        .join(dislike_tops, "user_id", "full_outer")

        return result    

if __name__ == "__main__":
        main()