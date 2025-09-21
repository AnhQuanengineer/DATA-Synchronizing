import os
from dataclasses import dataclass

from dotenv import load_dotenv
from typing import Dict, Optional


class DatabaseConfig():
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"----------Missing config for {key}-------------")

@dataclass
class MongoDBConfig(DatabaseConfig):
    uri : str
    db_name : str
    jar_path: Optional[str] = None
    collection: str = "Users"

@dataclass
class MySQLConfig(DatabaseConfig):
    host : str
    user : str
    password : str
    database : str
    port : int
    jar_path: Optional[str] = None
    table_users: str = "Users"
    table_repositories: str = "Repositories"

@dataclass
class RedisConfig(DatabaseConfig):
    host : str
    user : str
    password : str
    database : str
    port : int
    jar_path: Optional[str] = None
    key_column: str = "id"

def get_database_config() -> Dict[str,DatabaseConfig]:
    load_dotenv()
    config = {
        "mongodb" :  MongoDBConfig(
            uri = os.getenv("MONGO_URI"),
            db_name = os.getenv("MONGO_DB_NAME"),
            jar_path= os.getenv("MONGO_JAR_PATH")
        ),
        "mysql" : MySQLConfig (
            host = os.getenv("MYSQL_HOST"),
            port = int(os.getenv("MYSQL_PORT")),
            user = os.getenv("MYSQL_USER"),
            password = os.getenv("MYSQL_PASSWORD"),
            database = os.getenv("MYSQL_DATABASE"),
            jar_path= os.getenv("MYSQL_JAR_PATH")
        ),
        "redis" : RedisConfig (
            host = os.getenv("REDIS_HOST"),
            port = int(os.getenv("REDIS_PORT")),
            user = os.getenv("REDIS_USER"),
            password = os.getenv("REDIS_PASSWORD"),
            database = os.getenv("REDIS_DB"),
            jar_path= os.getenv("REDIS_JAR_PATH")
        )
    }

    for db, setting in config.items():
        # print(type(setting))
        setting.validate()

    return config


db_config = get_database_config()
# print(db_config)