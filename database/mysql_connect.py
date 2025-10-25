# step 1: def (get mongo config)
# step 2: def(connect)
# step 3: def(disconnect)
# step 4: def(reconnect)
# step 5: def(exit)
import mysql.connector

from mysql.connector import Error


class MySQLConnect:
#step 1
    def __init__(self, host, port, user, password):
        self.config = {"host": host, "port": port, "user": user, "password":password}
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            print(f"--------------------connected to Mysql------------------")
            return self.cursor, self.connection
        except Error as e:
            raise Exception(f"---------Can't connect to MySql: {e}-------------") from e

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
        print("---------------Mysql connection closed----------------------")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# coneect mysql using config

