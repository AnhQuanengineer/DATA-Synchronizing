from config.spark_config import get_spark_config
from config.database_config import get_database_config
from config.spark_config import SparkConnect
from pyspark.sql.functions import *
from pyspark.sql.types import *
from  src.spark.spark_write_data import SparkWriteDatabases

def main():
    db_config = get_database_config()
    jars = [
        db_config["mysql"].jar_path,
        "/home/victo/PycharmProjects/Big_data_project/lib/mongo-spark-connector_2.12-3.0.1.jar",
        "/home/victo/PycharmProjects/Big_data_project/lib/mongodb-driver-sync-4.0.5.jar",
        "/home/victo/PycharmProjects/Big_data_project/lib/bson-4.0.5.jar",
        "/home/victo/PycharmProjects/Big_data_project/lib/mongodb-driver-core-4.0.5.jar"
    ]



    spark_connect = SparkConnect(
        app_name = "quandz"
        , master_url= 'local[*]'
        , executor_memory = '4g'
        , executor_cores = 2
        , driver_memory = '2g'
        , num_executors = 3
        , jars = None
        , log_level = 'INFO'
    )

    schema = StructType([
        StructField('actor', StructType([
            StructField('id', LongType(), False),
            StructField('login', StringType(), True),
            StructField('gravatar_id', StringType(), True),
            StructField('url', StringType(), True),
            StructField('avatar_url', StringType(), True),
        ]), True),
        StructField('repo', StructType([
            StructField('id', IntegerType(), False),
            StructField('name', StringType(), True),
            StructField('url', StringType(), True),
        ]), True)
    ])

    df = spark_connect.spark.read.schema(schema).json("/home/victo/PycharmProjects/Big_data_project/data/2015-03-01-17.json")

    df.show()

    df_write_table_users = df.withColumn("spark_temp", lit("spark_write")) \
        .select(
        col('actor.id').alias('user_id')
        ,col('actor.login').alias('login')
        ,col('actor.gravatar_id').alias('gravatar_id')
        ,col('actor.avatar_url').alias('avatar_url')
        ,col('actor.url').alias('url')
        ,col("spark_temp")
    )

    # df_write_table_users.show()

    df_write_table_repositories = df.select(
        col('repo.id').alias('repo_id')
        ,col('repo.name').alias('name')
        ,col('repo.url').alias('url')
    )

    spark_configs = get_spark_config()
    df_write = SparkWriteDatabases(spark_connect.spark, spark_configs)
    # df_write.spark_write_mysql(df=df_write_table_users,table_name= "Users",jdbc_url=db_config["mysql"].jdbc, config=db_config["mysql"],mode="append" )
    df_write.write_all_databases(df_write_table_users, "Users", "append")
    # df_write.write_all_databases(df_write_table_repositories, "Repositories", "append")

    df_validate = SparkWriteDatabases(spark_connect.spark, spark_configs)
    df_validate.validate_spark_mysql(df_write_table_users, "Users", spark_configs['mysql']["jdbc_url"], spark_configs['mysql']["config"], "append")
    df_validate.validate_spark_mongodb(df_write_table_users, spark_configs["mongodb"]["uri"], spark_configs["mongodb"]["database"], spark_configs["mongodb"]["collection"], "append")

    spark_connect.spark.stop()

#input data and validate

if __name__ == "__main__":

    main()