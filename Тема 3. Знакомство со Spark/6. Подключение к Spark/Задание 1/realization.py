import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .config("spark.driver.memory", "1g") \
        .config("spark.driver.cores", 2) \
        .appName("My first session") \
        .getOrCreate()