# spark-submit --deploy-mode cluster --master yarn \
# --num-executors 1 \
# --executor-cores 2 \
# --executor-memory 2g \
# kafka_stream.py

import pyspark
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from pyspark import SparkConf

conf = (
    pyspark.SparkConf()
)

## Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

conf = SparkConf()
conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
print("Spark Running")

sc=spark.sparkContext
sc.setLogLevel("ERROR")


kafka_bootstrap_servers = "b-5.chiholeemsk.xmm8ve.c2.kafka.ap-northeast-2.amazonaws.com:9092,b-3.chiholeemsk.xmm8ve.c2.kafka.ap-northeast-2.amazonaws.com:9092,b-6.chiholeemsk.xmm8ve.c2.kafka.ap-northeast-2.amazonaws.com:9092"
kafka_topic = "rdb_op.ecommerce.orders"

kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load() \
    .selectExpr("CAST(value AS STRING) AS kafka_message")

parsed_df = kafka_stream_df.selectExpr(
    "get_json_object(kafka_message, '$.payload') AS payload")

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("promo_id", StringType(), True),
    StructField("order_cnt", IntegerType(), False),
    StructField("order_price", IntegerType(), False),
    StructField("order_dt", StringType(), False),
    StructField("last_update_time", LongType(), False),
    StructField("cust_id", LongType(), False),
    StructField("prd_id", IntegerType(), False),
    StructField("__op", StringType(), True),
    StructField("__event_timestamp", LongType(), True)
])

parsed_df = parsed_df.select(from_json("payload", schema).alias("data"))

parsed_df.printSchema()

split_columns = parsed_df.withColumn("last_update_time_sec",
                                     (col("data.last_update_time") /
                                      1000).cast("timestamp"))

selected_columns = split_columns.select(
    col("data.id"), col("data.promo_id"), col("data.order_cnt"),
    col("data.order_price"), col("data.order_dt"),
    col("last_update_time_sec").alias("last_update_time"), col("data.cust_id"),
    col("data.prd_id"), col("data.__op"), col("data.__event_timestamp"))

# split_columns = parsed_df.withColumn("last_update_time_sec", (col("data.last_update_time") / 1000))

# # 필요한 필드만 선택
# selected_columns = split_columns.select(
#     col("data.id"),
#     col("data.promo_id"),
#     col("data.order_cnt"),
#     col("data.order_price"),
#     col("data.order_dt"),
#     col("last_update_time_sec").alias("last_update_time"),
#     col("data.cust_id"),
#     col("data.prd_id"),
#     col("data.__op"),
#     col("data.__event_timestamp")
# )

s3_bucket = "chiho-datalake"
s3_output_path = "s3://" + s3_bucket + "/output/"

stream_data = selected_columns

stream_data.createOrReplaceTempView("stream_data")

# (9*3600)-60
# 9 * 3600 : 9시간
# 60 : 1분 전
windowed_data_query = spark.sql("""
    SELECT
        'minute' as m,
        cast(current_timestamp as TIMESTAMP) + INTERVAL 9 hour as now_time,
        window(data.last_update_time, '1 minute') as window_time,
        sum(data.order_price) as sum_order_price,
        sum(data.order_cnt) as total_order_count        
    FROM
        stream_data data
    GROUP BY
        window(data.last_update_time, '1 minute')
""")

windowed_data_query.printSchema()

# (9*3600)-(24*3600)
# 9 * 3600 : 9시간
# 24*3600 : 하루 전
daily_total_data_query = spark.sql("""
    SELECT
        'day' as d,
        cast(current_timestamp as TIMESTAMP) + INTERVAL 9 hour as now_time,
        window(data.last_update_time, '1 day') as window_time,        
        sum(data.order_price) as daily_order_total,
        sum(data.order_cnt) as total_order_count
    FROM
        stream_data data
    GROUP BY
        window(data.last_update_time, '1 day')
""")

daily_total_data_query.printSchema()

def save_to_s3(df, epoch_id):
    if not df.isEmpty():
        if "m" in df.columns:
            df.write \
                .mode("append") \
                .parquet(s3_output_path + "minute")
        elif "d" in df.columns:
            df.write \
                .mode("append") \
                .parquet(s3_output_path + "day")

# windowed_data_query \
#   .writeStream \
#   .outputMode("update") \
#   .queryName("windowed_data_querya1")\
#   .format("memory") \
#   .start()

# # # 메모리에 저장한 결과 확인
# spark.sql("SELECT * FROM windowed_data_query224").show(10, False)

# daily_total_data_query \
#   .writeStream \
#   .outputMode("update") \
#   .queryName("daily_total_data_query1")\
#   .format("memory") \
#   .start()

# # # 메모리에 저장한 결과 확인
# spark.sql("SELECT * FROM daily_total_data_query1").show(10, False)







windowed_data_query.writeStream \
    .outputMode("update") \
    .foreachBatch(save_to_s3) \
    .start()

# daily_total_data_query.writeStream \
#     .outputMode("complete") \
#     .foreachBatch(save_to_s3) \
#     .start()
#
spark.streams.awaitAnyTermination()
#
#
#
#

