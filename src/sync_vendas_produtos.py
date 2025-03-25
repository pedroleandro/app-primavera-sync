import sys
import os
import logging
from decimal import Decimal

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.database import get_connection
from config.mydatabase import get_mysql_connection

QUERIES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "queries"))

log_path = os.path.join(os.path.dirname(__file__), "..", "logs", "sync_vendas_produtos.log")

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
        logger.error("Nenhum dado para sincronizar.\n")
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
                codigo_cliente = int(row[2]) if row[2] is not None else None
                nome_cliente = row[3] if row[3] else None
                codigo_produto = int(row[4]) if row[4] is not None else None
                nome_produto = row[5] if row[5] else None
                quantidade = int(row[6]) if row[6] is not None else None
                preco_venda = Decimal(row[7])
                subtotal = Decimal(row[8])
                codigo_filial = int(row[9]) if row[9] is not None else None
                filial = row[10] if row[10] else None

                cursor.execute("""
                    SELECT COUNT(*) FROM vendas_produtos 
                    WHERE numeroPedido = %s AND codigoProduto = %s
                """, (numero_pedido, codigo_produto))

                exists = cursor.fetchone()[0]

                if exists == 0:
                    sql_insert = """
                        INSERT IGNORE INTO vendas_produtos (
                            numeroPedido, dataPedido, codigoCliente, nomeCliente, 
                            codigoProduto, produto, quantidade, precoVenda, 
                            subTotalPedido, codigoFilial, filial
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql_insert, (
                        numero_pedido, data_pedido, codigo_cliente, nome_cliente, codigo_produto,
                        nome_produto, quantidade, preco_venda, subtotal, codigo_filial, filial
                    ))

                    logger.info(f"Pedido {numero_pedido} sincronizado com sucesso.")
                    print(f"✅ Pedido {numero_pedido} inserido com sucesso.")
                    quantidade_pedidos_sincronizados += 1


        if quantidade_pedidos_sincronizados > 0:
            logger.info(f"Sincronizou {quantidade_pedidos_sincronizados} pedidos com sucesso!\n")
            print(f"✅ Sincronizou {quantidade_pedidos_sincronizados} pedidos com sucesso!")
        else:
            logger.warning("Não sincronizou nenhum novo pedido\n")
            print("⚠️ Não sincronizou nenhum novo pedido\n")

        mysql_conn.commit()
        logger.info("Sincronização concluída com sucesso!\n")
        print("✅ Sincronização concluída com sucesso!")
    except Exception as e:
        logging.error(f"Erro ao sincronizar os dados: {e}\n")
        print(f"❌ Erro ao sincronizar os dados: {e}")
    finally:
        mysql_conn.close()


if __name__ == "__main__":
    sincronizar(execute_query("vendas_produtos"))