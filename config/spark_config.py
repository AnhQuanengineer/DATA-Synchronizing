import os
from typing import Optional,List,Dict
from pyspark.sql import SparkSession,DataFrame
from config.database_config import get_database_config
from pyspark.sql.types import *
from pyspark.sql.functions import *

class SparkConnect:
    def __init__(
            self
            ,app_name: str
            , master_url: str = 'local[*]'
            , executor_memory: Optional[str] = '4g'
            , executor_cores: Optional[int] = 2
            , driver_memory: Optional[str] = '2g'
            , num_executors: Optional[int] = 3
            , jars: Optional[List[str]] = None
            , spark_conf: Optional[Dict[str, str]] = None
            , log_level: str = 'INFO'
    ):
        self.app_name = app_name
        self.spark = self.create_spark_session(master_url, executor_memory, executor_cores, driver_memory, num_executors, jars, spark_conf, log_level)
    def create_spark_session(
            self
            # app_name : str
            ,master_url : str = 'local[*]'
            ,executor_memory : Optional[str] = '4g'
            ,executor_cores : Optional[int] = 2
            ,driver_memory : Optional[str] = '2g'
            ,num_executors : Optional[int] = 3
            ,jars : Optional[List[str]] = None
            ,spark_conf : Optional[Dict[str,str]] = None
            ,log_level : str = 'INFO'
    ) -> SparkSession:
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(master_url)

        if executor_memory:
            builder.config('spark.executor.memory',executor_memory)
        if executor_cores:
            builder.config('spark.executor.cores',executor_cores)
        if driver_memory:
            builder.config('spark.driver.memory',driver_memory)
        if num_executors:
            builder.config('spark.executor.instances',num_executors)

        if jars:
            jars_path = ",".join([os.path.abspath(jar) for jar in jars])
            builder.config('spark.jars.packages',jars_path)

        if spark_conf:
            for key,value in spark_conf.items():
                builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)

        return spark

    def stop(self):
        if self.spark:
            self.spark.stop()
            print("-------------Stop spark session---------------")

def get_spark_config() -> Dict:

    db_configs = get_database_config()

    return {
        "mysql" : {
            "table_users" : db_configs["mysql"].table_users,
            "table_repositories": db_configs["mysql"].table_repositories,
            "jdbc_url" : "jdbc:mysql://{}:{}/{}".format(db_configs["mysql"].host, db_configs["mysql"].port, db_configs["mysql"].database),
            "config" : {
                "host": db_configs["mysql"].host,
                "port": db_configs["mysql"].port,
                "user": db_configs["mysql"].user,
                "password": db_configs["mysql"].password,
                "database": db_configs["mysql"].database
            }
        },
        "mongodb": {
            "database": db_configs["mongodb"].db_name,
            "collection": db_configs["mongodb"].collection,
            "uri": db_configs["mongodb"].uri
        },
        "redis": {

        }
    }

if __name__ == "__main__":
    spark_config = get_spark_config()
    print(spark_config)


