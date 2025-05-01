import dotenv
import mysql.connector

config = dotenv.dotenv_values(".env")


def get_connection():
    return mysql.connector.connect(
        host=config["MYSQL_HOST"],
        user=config["MYSQL_USER"],
        password=config["MYSQL_PASSWORD"],
        database=config["MYSQL_DATABASE"],
    )


# connection = get_connection()

# cursor = connection.cursor()

# query = "SELECT * from productlines"
# cursor.execute(query)

# for row in cursor.fetchall():
#     print(row)

# cursor.close()

# connection.close()
