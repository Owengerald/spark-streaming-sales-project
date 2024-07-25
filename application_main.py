from pyspark.sql.functions import *
from pyspark.sql import SparkSession

if __name__ == '__main__':

    print("Creating Spark Session")

    spark = SparkSession.builder \
            .appName("streaming application") \
            .master("local[2]") \
            .getOrCreate()


# 1. read the data
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()


# 2. processing logic
words = lines.select(explode(split(lines.value, " ")).alias("word"))
wordCounts = words.groupBy("word").count()


# 3. write to the sink
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "checkpointdir1") \
    .start()

query.awaitTermination()