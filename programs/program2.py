from pyspark.sql.functions import *
from pyspark.sql import SparkSession

if __name__ == '__main__':

    print("Creating Spark Session")

    spark = SparkSession.builder \
            .appName("streaming application") \
            .config("spark.sql.shuffle.partitions",3) \
            .master("local[2]") \
            .getOrCreate()


# define orders schema
orders_schema = 'order_id long, order_date date, order_customer_id long, order_status string'

# 1. read the data
orders_df = spark \
    .readStream \
    .format("json") \
    .schema(orders_schema) \
    .option("path", "data/inputdir") \
    .load()


# 2. processing logic
orders_df.createOrReplaceTempView("orders") 			
completed_orders = spark.sql("SELECT * FROM orders WHERE order_status = 'COMPLETE'")


# 3. write to the sink
query = completed_orders \
    .writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "data/outputdir") \
    .option("checkpointLocation", "checkpointdir1") \
    .start() 
				
query.awaitTermination()