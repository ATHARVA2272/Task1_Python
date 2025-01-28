import mysql.connector

def create_mysql_connection(mysql_details):
    try:
        conn = mysql.connector.connect(
            port=mysql_details['port'],
            username=mysql_details['username'],
            password=mysql_details['password'],
            database=mysql_details['database']
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        raise
