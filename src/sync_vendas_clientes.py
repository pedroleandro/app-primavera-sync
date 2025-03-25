import sys
import os
import logging
from decimal import Decimal

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.database import get_connection
from config.mydatabase import get_mysql_connection

QUERIES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "queries"))

log_path = os.path.join(os.path.dirname(__file__), "..", "logs", "sync_vendas_cliente.log")

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
        logger.error(f"O arquivo {query_name}.sql não foi encontrado em {QUERIES_DIR}.\n")
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
            logger.error(f"Erro ao executar a query: {e}\n")
            print(f"❌ Erro ao executar a query: {e}")
        finally:
            connection.close()
    return None


def sincronizar(results):
    if not results:
        logger.warning("Nenhum dado para sincronizar.")
        print("❌ Nenhum dado para sincronizar.")
        return

    mysql_conn = get_mysql_connection()
    if not mysql_conn or not mysql_conn.is_connected():
        logger.error("Falha ao conectar ao banco de dados MySQL.\n")
        print("❌ Falha ao conectar ao banco de dados MySQL.")
        return

    try:
        mysql_conn.set_charset_collation('utf8mb4', 'utf8mb4_unicode_ci')

        with mysql_conn.cursor() as cursor:
            logger.info("Início da sincronização\n")
            print("✅ Início da sincronização!\n")
            quantidade_pedidos_sincronizados = 0
            for row in results:

                numero_pedido = int(row[0])
                data_pedido = row[1].date()
                data_pedido_faturado = row[2].date()
                codigo_cobranca = row[3] if row[3] else None
                codigo_filial = int(row[4]) if row[4] is not None else None
                filial = row[5] if row[5] else None
                codigo_cliente = int(row[6]) if row[6] is not None else None
                nome_cliente = row[7] if row[7] else None
                nome_fantasia = row[8] if row[8] else None
                valor_faturado = Decimal(row[9])
                valor_despesas = Decimal(row[10])
                valor_liquido = Decimal(row[11])

                cursor.execute("SELECT COUNT(*) FROM vendas_clientes WHERE numeroPedido = %s", (numero_pedido,))
                exists = cursor.fetchone()[0]

                if exists == 0:
                    sql_insert = """
                        INSERT INTO vendas_clientes (
                            numeroPedido, dataPedido, dataPedidoFaturado, codigoCobranca, 
                            codigoFilial, filial, codigoCliente, nomeCliente, 
                            nomeFantasiaCliente, valorFaturadoPedido, valorDespesas, valorLiquidoPedido
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql_insert, (
                        numero_pedido, data_pedido, data_pedido_faturado, codigo_cobranca, codigo_filial,
                        filial, codigo_cliente, nome_cliente, nome_fantasia, valor_faturado, valor_despesas,
                        valor_liquido
                    ))
                    logger.info(f"Pedido {numero_pedido} inserido com sucesso.")
                    print(f"✅ Pedido {numero_pedido} inserido com sucesso.")
                    quantidade_pedidos_sincronizados += 1

        if quantidade_pedidos_sincronizados > 0:
            logger.info(f"Sincronizou {quantidade_pedidos_sincronizados} pedidos com sucesso!\n")
            print(f"✅ Sincronizou {quantidade_pedidos_sincronizados} pedidos com sucesso!\n")
        else:
            logger.warning("Não sincronizou nenhum novo pedido\n")
            print("⚠️ Não sincronizou nenhum novo pedido\n")

        mysql_conn.commit()
        logger.info("Sincronização concluída com sucesso!\n")
        print("✅ Sincronização concluída com sucesso!\n")
    except Exception as e:
        logger.error(f"Erro ao sincronizar os dados: {e}\n")
        print(f"❌ Erro ao sincronizar os dados: {e}")
    finally:
        mysql_conn.close()


if __name__ == "__main__":
    sincronizar(execute_query("vendas_clientes"))
