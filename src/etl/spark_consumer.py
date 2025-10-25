from config.spark_config import get_spark_config
from config.database_config import get_database_config
from config.spark_config import SparkConnect
from pyspark.sql.functions import *
from pyspark.sql.types import *
from  src.spark.spark_write_data import SparkWriteDatabases
from pyspark.sql import SparkSession

def main():
    db_config = get_database_config()
    jars = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ]

    # spark_connect = SparkConnect(
    #     app_name = "quandz"
    #     , master_url= 'local[*]'
    #     , executor_memory = '4g'
    #     , executor_cores = 2
    #     , driver_memory = '2g'
    #     , num_executors = 3
    #     , jars = jars
    #     , log_level = 'INFO'
    # )

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ShopeeProductProcessing") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
        .getOrCreate()

    schemaKafka = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("avatar_url", StringType(), True),
        StructField("url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("log_timestamp", StringType(), True)
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "quandz") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    df_property = kafka_df.select(col("value").cast("string"))
        # .select(from_json(col("value"), schema).alias("data")) \
        # .select("data.*")

    df_property = df_property.withColumn("value", regexp_replace(col("value"), "\\\\", "")) \
        .withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))

    df_property = df_property.select(from_json(col("value"), schemaKafka).alias("data")) \
        .select("data.*")

    # query = df_property \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()

    query = df_property.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/pyspark/") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .option("spark.mongodb.connection.uri", "mongodb://anhquan:123456@localhost:27017") \
        .option("spark.mongodb.database", "github_data") \
        .option("spark.mongodb.collection", "Users") \
        .trigger(continuous= '1 seconds') \
        .outputMode("append") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
#cdc and validate kafka to mongo after trggier mysql

