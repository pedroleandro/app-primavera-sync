import sys
import os
from calendar import monthrange
from decimal import Decimal
import logging

from datetime import datetime
from config.database import get_connection
from config.mydatabase import get_mysql_connection

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

QUERIES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "queries"))

LOGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))

os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOGS_DIR, "sync_faturamento.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def execute_query(query_name, start_date, end_date):
    query_file_path = os.path.join(QUERIES_DIR, f"{query_name}.sql")

    if not os.path.exists(query_file_path):
        logger.error(f"O arquivo {query_name}.sql não foi encontrado em {QUERIES_DIR}.\n")
        print(f"❌ O arquivo {query_name}.sql não foi encontrado em {QUERIES_DIR}.\n")
        return None

    with open(query_file_path, "r", encoding="utf-8") as file:
        sql_query = file.read()

    start_date_str = start_date.strftime('%d/%m/%Y')
    end_date_str = end_date.strftime('%d/%m/%Y')

    connection = get_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql_query, {'start_date': start_date_str, 'end_date': end_date_str})
                results = cursor.fetchall()
                return results
        except Exception as e:
            logger.error(f"Erro ao executar a query: {e}\n")
            print(f"❌ Erro ao executar a query: {e}\n")
        finally:
            connection.close()
    return None


def sincronizar_faturamento(results):
    if not results:
        logger.warning("Nenhum dado para sincronizar.\n")
        print("❌ Nenhum dado para sincronizar.\n")
        return

    mysql_conn = get_mysql_connection()
    if not mysql_conn or not mysql_conn.is_connected():
        logger.error("Falha ao conectar ao banco de dados MySQL.\n")
        print("❌ Falha ao conectar ao banco de dados MySQL.\n")
        return

    try:
        mysql_conn.set_charset_collation('utf8mb4', 'utf8mb4_unicode_ci')

        with mysql_conn.cursor() as cursor:
            for row in results:
                codigo_filial = int(row[0])
                nome_filial = row[1]
                faturamento_total = Decimal(row[2])
                data_inicial = datetime.strptime(row[3], '%d/%m/%Y').date().strftime('%Y-%m-%d')
                data_final = datetime.strptime(row[4], '%d/%m/%Y').date().strftime('%Y-%m-%d')

                cursor.execute(
                    "SELECT COUNT(*) FROM faturamento WHERE codigoFilial = %s AND dataInicial = %s AND dataFinal = %s",
                    (codigo_filial, data_inicial, data_final),
                )
                exists = cursor.fetchone()[0]

                if exists == 0:
                    sql_insert = """
                        INSERT INTO faturamento (
                            codigoFilial, filial, faturamento, dataInicial, dataFinal
                        ) VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql_insert, (codigo_filial, nome_filial, faturamento_total, data_inicial, data_final))
                    logger.info(f"Faturamento da filial {nome_filial} inserido com sucesso para o período {data_inicial} - {data_final}.\n")
                    print(f"✅ Faturamento da filial {nome_filial} inserido com sucesso para o período {data_inicial} - {data_final}.\n")
                else:
                    sql_update = """
                        UPDATE faturamento
                        SET faturamento = %s
                        WHERE codigoFilial = %s AND dataInicial = %s AND dataFinal = %s
                    """
                    cursor.execute(sql_update, (faturamento_total, codigo_filial, data_inicial, data_final))
                    logger.info(f"Faturamento da filial {nome_filial} atualizado para o período {data_inicial} - {data_final}.\n")
                    print(f"🔄 Faturamento da filial {nome_filial} atualizado para o período {data_inicial} - {data_final}.\n")

        mysql_conn.commit()
        logger.info("Sincronização de faturamento concluída com sucesso!\n")
        print("✅ Sincronização de faturamento concluída com sucesso!\n")
    except Exception as e:
        logger.error(f"Erro ao sincronizar o faturamento: {e}\n")
        print(f"❌ Erro ao sincronizar o faturamento: {e}\n")
    finally:
        mysql_conn.close()



def get_month_start_and_end(year, month):
    start_date = datetime(year, month, 1)
    last_day = monthrange(year, month)[1]
    end_date = datetime(year, month, last_day)
    return start_date, end_date


def get_sync_dates():
    today = datetime.now()
    year = today.year
    month = today.month

    date_ranges = []

    for m in range(1, month + 1):
        start_date, end_date = get_month_start_and_end(year, m)
        date_ranges.append((start_date, end_date))

    if month == 1:
        prev_year = year - 1
        start_date, end_date = get_month_start_and_end(prev_year, 12)
        date_ranges.insert(0, (start_date, end_date))

    return date_ranges

if __name__ == "__main__":

    date_ranges = get_sync_dates()

    for start_date, end_date in date_ranges:
        sincronizar_faturamento(execute_query("faturamento", start_date, end_date))


