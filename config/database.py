import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_SERVICE = os.getenv("DB_SERVICE")

dsn = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"

def get_connection():
    try:
        connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn)
        return connection
    except oracledb.DatabaseError as e:
        print(f"Erro ao conectar ao banco: {e}")
        return None
