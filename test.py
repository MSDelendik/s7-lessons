from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.window import Window
import sys
from calculate_user_interests import calculate_user_interests

def main():

    conf = SparkConf().setAppName(f"Learning Dataframes-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    date = sys.argv[1]
    days_count = int(sys.argv[2])
    event_base_path = sys.argv[3]
    output_base_path = sys.argv[4]

    # Здесь будет основная часть кода. Тело функции.
    tab = calculate_user_interests(date, days_count, sql)
    tab.write.parquet(output_base_path)
    
if __name__ == "__main__":
        main()