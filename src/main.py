from database.mongodb_connect import MongoDBConnect
from database.mysql_connect import MySQLConnect
from database.redis_connect import RedisConnect
from database.schema_manager import create_mongodb_schema,validate_mongodb_schema
from database.schema_manager import create_mysql_schema,validate_mysql_schema
from database.schema_manager import create_redis_schema,validate_redis_schema
from config.database_config import get_database_config


def main(config):
    #MONGODB
    with MongoDBConnect(config['mongodb'].uri, config["mongodb"].db_name) as mongo_client:
        create_mongodb_schema(mongo_client.connect())
        print("------------Inserted to MongoDB----------------")
        mongo_client.db.Users.insert_one({
            "user_id": 1,
            "login": "GoogleCodeExporter",
            "gravatar_id": "",
            "avatar_url": "https://avatars.githubusercontent.com/u/9614759?",
            "url": "https://api.github.com/users/GoogleCodeExporter"
        })
        validate_mongodb_schema(mongo_client.connect())
    #MYSQL
    with MySQLConnect(config['mysql'].host, config["mysql"].port, config["mysql"].user, config["mysql"].password) as mysql_client:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        create_mysql_schema(connection, cursor)
        cursor.execute("INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (%s, %s, %s, %s, %s)", (1,"GoogleCodeExporter","","https://avatars.githubusercontent.com/u/9614759?","https://api.github.com/users/GoogleCodeExporter"))
        connection.commit()
        print("------------Inserted data to My SQL---------------------")
        validate_mysql_schema(cursor)

    # #REDIS
    with RedisConnect(config['redis'].host, config["redis"].port, config["redis"].user, config["redis"].password, config["redis"].database) as redis_client:
        create_redis_schema(redis_client.connect())
        validate_redis_schema(redis_client.connect())
if __name__ == "__main__":
    config = get_database_config()
    main(config)