import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.database import get_connection
from config.mydatabase import get_mysql_connection

QUERIES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "queries"))

log_path = os.path.join(os.path.dirname(__file__), "..", "logs", "sync_pedidos_cancelados.log")

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()


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


def remover_pedidos_cancelados():
    mysql_conn = get_mysql_connection()
    if not mysql_conn or not mysql_conn.is_connected():
        logger.error("❌ Falha ao conectar ao banco de dados MySQL.")
        print("❌ Falha ao conectar ao banco de dados MySQL.")
        return

    try:
        with mysql_conn.cursor() as cursor:
            pedidos_cancelados = execute_query("pedidos_cancelados")

            if not pedidos_cancelados:
                logger.info("Nenhum pedido cancelado encontrado.")
                print("⚠️ Nenhum pedido cancelado encontrado.")
                return

            logger.info("Início da sincronização\n")
            print("✅ Início da sincronização!\n")
            pedidos_processados = 0
            for row in pedidos_cancelados:
                numero_pedido = row[0]

                logger.info(f"Processando pedido cancelado: {numero_pedido}")
                print(f"Processando pedido cancelado: {numero_pedido}")

                cursor.execute("SELECT COUNT(*) FROM vendas_clientes WHERE numeroPedido = %s", (numero_pedido,))
                if cursor.fetchone()[0] == 0:
                    logger.warning(f"Pedido {numero_pedido} não encontrado na tabela. Pulando exclusão.\n")
                    print(f"⚠️ Pedido {numero_pedido} não encontrado na tabela. Pulando exclusão.\n")
                    continue

                cursor.execute(
                    "INSERT INTO backup_vendas_clientes SELECT * FROM vendas_clientes WHERE numeroPedido = %s",
                    (numero_pedido,))
                cursor.execute(
                    "INSERT INTO backup_vendas_produtos SELECT * FROM vendas_produtos WHERE numeroPedido = %s",
                    (numero_pedido,))

                cursor.execute("DELETE FROM vendas_produtos WHERE numeroPedido = %s", (numero_pedido,))
                cursor.execute("DELETE FROM vendas_clientes WHERE numeroPedido = %s", (numero_pedido,))

                pedidos_processados += 1
                logger.info(f"Pedido {numero_pedido} removido com sucesso.\n")

            mysql_conn.commit()
            logger.info("Sincronização concluída com sucesso!\n")
            print("✅ Sincronização concluída com sucesso!")

    except Exception as e:
        logger.error(f"Erro ao remover pedidos cancelados: {e}")
    finally:
        mysql_conn.close()


if __name__ == "__main__":
    remover_pedidos_cancelados()
