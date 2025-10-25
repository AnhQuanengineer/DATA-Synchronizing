from mysql.connector import Error
from pathlib import Path


def create_mongodb_schema(db):
    db.drop_collection("Users")
    db.create_collection("Users", validator={
        "$jsonSchema": {
            "bsonType" : "object",
            "required" : ["user_id", "login"],
            "properties": {
                "user_id": {
                    "bsonType": "int"
                },
                "login": {
                    "bsonType": "string"
                },
                "gravatar_id": {
                    "bsonType": ["string", "null"]
                },
                "avatar_url": {
                    "bsonType": ["string", "null"]
                },
                "url": {
                    "bsonType": ["string", "null"]
                }
            }
        }
    })
    # db.Users.create_index("user_id",unique = True)

    db.drop_collection("Repositories")
    db.create_collection("Repositories", validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["repo_id", "name"],
            "properties": {
                "repo_id": {
                    "bsonType": "int"
                },
                "name": {
                    "bsonType": "string"
                },
                "url": {
                    "bsonType": ["string", "null"]
                }
            }
        }
    })
    # db.Repositories.create_index("repo_id", unique=True)
    print("------------SCHEMA CREATED IN MONGODB---------------------")


def validate_mongodb_schema(db):
    collections = db.list_collection_names()
    # print(collections)
    if "Users" not in collections or "Repositories" not in collections:
        raise ValueError("---------------------Missing collection in MongoDB-----------------------")
    user = db.Users.find_one({"user_id": 1})
    if not user:
        raise ValueError("---------------------user_id not found in MongoDB-----------------------")
    print("---------------Validated schema in MongoDB----------------------")


SQL_FILE_PATH = Path('../sql/schema.sql')

def create_mysql_schema(connection,cursor):
    database = "github_data"
    cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database}')
    connection.commit()
    print(f'===========Created database {database} in MySQL!================')
    connection.database = database
    try:
        with open(SQL_FILE_PATH, 'r') as file:
            sql_script = file.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]
            for cmd in sql_commands:
                cursor.execute(cmd)
                print(f'----------------Execute: {cmd.strip()[:50]}...---------------')
            connection.commit()
            print("---------Created Mysql Schema-----------------")
    except Error as e:
        connection.rollback()
        print(f'Can not execute to sql command: {e}----------------------')

def validate_mysql_schema(cursor):
    cursor.execute(f'SHOW TABLES')
    tables = [row[0] for row in cursor.fetchall()]
    # print(tables)
    if "Users" not in tables or "Repositories" not in tables:
        raise ValueError("---------------------Missing tables in MySQL-----------------------")
    cursor.execute("SELECT * FROM Users WHERE user_id = 1")
    user = cursor.fetchone()
    if not user:
        raise ValueError("---------User not found-----------------")
    print("---------------Validated schema in MySQL----------------------")

def create_redis_schema(client):
    try:
        client.flushdb() #drop database
        client.set("user:1:login","GoogleCodeExporter")
        client.set("user:1:gravatar_id", "")
        client.set("user:1:avatar_url", "https://avatars.githubusercontent.com/u/9614759?")
        client.set("user:1:url", "https://api.github.com/users/GoogleCodeExporter")
        client.sadd("user_id", "user:1")
        print("--------------------Add data to Redis successfully---------------")
    except Exception as e:
        raise Exception(f"----------Failed to add data to Redis: {e}------------") from e

def validate_redis_schema(client):
    if not client.get("user:1:login") == "GoogleCodeExporter" :
        raise ValueError("--------Value login not found in Redis------------")
    if not client.sismember("user_id", "user:1"):
        raise ValueError("-----------User not set in Redis---------------")

    print("------------Validated schema in Redis-----------------")

#create and validate schema and data
