from docutils.parsers.rst.directives.misc import Class
from pyspark.sql import SparkSession
from config.database_config import get_database_config
from pyspark.sql.types import *
from pyspark.sql.functions import *
from database.mysql_connect import MySQLConnect

class SparkWriteDatabases:
    def __init__(self,spark :SparkSession, spark_config: Dict):
        self.spark = spark
        self.spark_config = spark_config

    def spark_write_mysql(self, df_write: DataFrame, table_name: str, jdbc_url: str, config, mode : str = "append"):
        try:
            mysql_client = MySQLConnect(config["host"], config["port"], config["user"], config["password"])
            mysql_client.connect()
            connection, cursor = mysql_client.connection, mysql_client.cursor
            connection.database = "github_data"
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN spark_temp VARCHAR(255)")
            connection.commit()
            print("----------Add column spark_temp to mysql-----------------")
            mysql_client.close()
        except Exception as e:
            raise Exception(f"--------Fail to connect Mysql: {e}-----------")

        df_write.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .mode(mode) \
            .save()

        print(f"------------Spark writed data to mysql in table: {table_name}")

    def validate_spark_mysql(self, df_write: DataFrame, table_name: str, jdbc_url: str, config: Dict, mode: str = "append"):
        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE spark_temp = 'spark_write') as sub_query") \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .load()
        # df_read.show()

        def subtract_dataframe(df_spark_write: DataFrame, df_read_database: DataFrame):
            #subtract 2 datafame
            result = df_spark_write.exceptAll(df_read_database)
            # result.show()
            print(f"-------------Result records: {result.count()}--------------------")
            if not result.isEmpty():
                result.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", table_name) \
                .option("user", config["user"]) \
                .option("password", config["password"]) \
                .mode(mode) \
                .save()

        if df_write.count() == df_read.count():
            print(f"---------------------validate {df_read.count()} records success-----------------------")
            subtract_dataframe(df_write,df_read)
            print(f"---------------validate data of records success----------------")
        else:
            subtract_dataframe(df_write,df_read)
            print(f"-------------insert missing records by using spark insert data-----------------")

        try:
            mysql_client = MySQLConnect(config["host"], config["port"], config["user"], config["password"])
            mysql_client.connect()
            connection, cursor = mysql_client.connection, mysql_client.cursor
            connection.database = "github_data"
            cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN spark_temp")
            connection.commit()
            print("----------Drop column spark_temp to mysql-----------------")
            mysql_client.close()
        except Exception as e:
            raise Exception(f"--------Fail to connect Mysql: {e}-----------")


    def spark_write_mongodb(self, df: DataFrame, database: str, collection: str, uri: str, mode: str = "append"):
        df.write \
            .format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .mode(mode) \
            .save()

        print(f"----------------Spark writed data to mongodb in collection: {database}.{collection}---------------")


    def validate_spark_mongodb(self, df_write: DataFrame, uri: str, database: str, collection: str, mode: str = "append"):
        query = {"spark_temp": "spark_write"}

        df_read = self.spark.read \
            .format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .option("pipeline", str([{"$match": query}])) \
            .load()
        df_read = df_read.select(
            col("user_id")
            , col("login")
            , col("gravatar_id")
            , col("avatar_url")
            , col("url")
            , col("spark_temp")
        )

        def subtract_dataframe(df_spark_write: DataFrame, df_read_database: DataFrame):
            #subtract 2 datafame
            result = df_spark_write.exceptAll(df_read_database)
            # result.show()
            print(f"-------------Result records: {result.count()}--------------------")
            if not result.isEmpty():
                result.write \
                .format("mongo") \
                .option("uri", uri) \
                .option("database", database) \
                .option("collection", collection) \
                .mode(mode) \
                .save()

        if df_write.count() == df_read.count():
            print(f"---------------------validate {df_read.count()} records success-----------------------")
            subtract_dataframe(df_write,df_read)
            print(f"---------------validate data of records success----------------")
        else:
            subtract_dataframe(df_write,df_read)
            print(f"-------------insert missing records by using spark insert data-----------------")

        from database.mongodb_connect import MongoDBConnect
        from config.database_config import get_database_config

        config = get_database_config()
        with MongoDBConnect(config["mongodb"].uri, config["mongodb"].db_name) as mongo_client:
            mongo_client.db.Users.update_many({}, {"$unset": {"spark_temp": ""}})

    def write_all_databases(self, df : DataFrame, table_name, mode: str = "append"):
        self.spark_write_mysql(
            df,
            table_name,
            self.spark_config["mysql"]["jdbc_url"],
            self.spark_config["mysql"]["config"],
            mode
        )
        self.spark_write_mongodb(
            df,
            self.spark_config["mongodb"]["database"],
            table_name,
            self.spark_config["mongodb"]["uri"],
            mode
        )

        # self.spark_write_mysql(
        #     df,
        #     self.spark_config["mysql"]["table_repositories"],
        #     self.spark_config["mysql"]["jdbc_url"],
        #     self.spark_config["mysql"]["config"],
        #     mode
        # )

        print("--------------Write success to all databases-------------------")
