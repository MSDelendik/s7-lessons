# убрать вызов спарк серии, заменить на  Спарк-контекст и убрать все импорты find-spark
 
import os
import sys
 
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import datetime
 
def input_path(date, depth, base_input_path):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [ 
        f"{base_input_path}/date={(dt - datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message"
        for x in range(int(depth))
    ]
# for x in range(depth)
 
def partition_writer(candidates):
    return candidates.write.mode('overwrite').format('parquet')
 
def main():
    date = sys.argv[1]                 # день расчёта
    depth = sys.argv[2]                # глубина расчёта
    threshold = sys.argv[3]            # порог (мин кол-во уников)
    base_input_path = sys.argv[4]      # путь к таблице с эвентами
    tags_verified_path = sys.argv[5]   # путь к таблице с актуальными тегами
    base_output_path = sys.argv[6]     # куда сохранять результат
 
    conf = SparkConf().setAppName(f'VerifiedTagsCandidatesJob-{date}-d{depth}-cut{threshold}')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
 
    paths = input_path(date, depth, base_input_path)
    messages = sql.read.parquet(*paths)
    threshold=int(threshold)
    all_tags = messages \
                .where('event.message_channel_to is not null') \
                .selectExpr(['event.message_from as user' , 'explode(event.tags) as tag']) \
                .groupBy("tag") \
                .agg(F.expr('count(distinct user) as suggested_count')) \
                .where('suggested_count >= 300')
    verified_tags = sql.read.parquet(f'{tags_verified_path}')
    candidates = all_tags.join(verified_tags, "tag", "left_anti")
 
    #candidates.write.mode("overwrite").parquet(f'{base_output_path}/date={date}')
    writer = partition_writer(candidates)
    writer.save(f'{base_output_path}/date={date}')
 
if __name__ == "__main__":
    main()