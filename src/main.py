import sys
import os
from decimal import Decimal

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.database import get_connection
from config.mydatabase import get_mysql_connection

QUERIES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "queries"))

def test_connection():
    connection = get_connection()
    if connection:
        print("✅ Conexão estabelecida com sucesso!")
        connection.close()
    else:
        print("❌ Falha ao conectar ao banco.")


def test_connection_mysql():

    connection = get_mysql_connection()

    if connection and connection.is_connected():
        print("✅ Conexão bem-sucedida com o banco de dados MySQL!")
        connection.close()
    else:
        print("❌ Não foi possível conectar ao banco de dados MySQL.")


def execute_query(query_name):
    query_file_path = os.path.join(QUERIES_DIR, f"{query_name}.sql")

    if not os.path.exists(query_file_path):
        print(f"❌ O arquivo {query_name}.sql não foi encontrado em {QUERIES_DIR}.")
        return None

    with open(query_file_path, "r", encoding="utf-8") as file:
        sql_query = file.read()

    connection = get_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                results = cursor.fetchall()
                return results
        except Exception as e:
            print(f"❌ Erro ao executar a query: {e}")
        finally:
            connection.close()
    return None


if __name__ == "__main__":
    print('Olá, Mundo!')
