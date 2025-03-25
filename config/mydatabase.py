import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

def get_mysql_connection():
    try:
        mysql_host = os.getenv('MYSQL_HOST')
        mysql_user = os.getenv('MYSQL_USER')
        mysql_password = os.getenv('MYSQL_PASSWORD')
        mysql_database = os.getenv('MYSQL_DATABASE')

        connection = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )

        return connection

    except mysql.connector.Error as err:
        print(f"❌ Erro ao conectar ao MySQL: {err}")
        return None
    except Exception as e:
        print(f"❌ Erro: {e}")
        return None
