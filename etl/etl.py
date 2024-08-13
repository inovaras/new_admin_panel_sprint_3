import os
import time
import psycopg2
from elasticsearch import Elasticsearch, helpers
import backoff
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.environ.get('PG_HOST', "localhost")
PG_PORT = os.environ.get('PG_PORT', 5432)
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_DB = os.environ.get('POSTGRES_DB')

ES_HOST = os.environ.get('ES_HOST', "http://localhost:9200")
INDEX_NAME = "movies"


#
def get_pg_connection():
    """ Подключение к PostgreSQL """

    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def get_es_client():
    """ Подключение к Elasticsearch """
    return Elasticsearch([ES_HOST])


def get_last_synced_id(conn):
    """ Получение последнего обработанного ID из PostgreSQL """
    with conn.cursor() as cursor:
        cursor.execute("SELECT last_synced_id FROM sync_state WHERE id = 1;")
        result = cursor.fetchone()
        return result[0] if result else None


def update_last_synced_id(conn, last_synced_id):
    """ Обновление последнего обработанного ID в PostgreSQL """
    with conn.cursor() as cursor:
        cursor.execute("UPDATE sync_state SET last_synced_id = %s WHERE id = 1;", (last_synced_id,))
        conn.commit()


def extract_data(conn, last_synced_id, batch_size=100):
    """ Извлечение данных из PostgreSQL"""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT id, title, description, imdb_rating, genres, directors, actors, writers
            FROM movies
            WHERE id > %s
            ORDER BY id ASC
            LIMIT %s;
        """, (last_synced_id, batch_size))
        return cursor.fetchall()


def transform_data(records):
    """ Преобразование данных в формат для Elasticsearch"""
    for record in records:
        yield {
            "_index": INDEX_NAME,
            "_id": record[0],
            "_source": {
                "id": record[0],
                "title": record[1],
                "description": record[2],
                "imdb_rating": record[3],
                "genres": record[4],
                "directors_names": [d['name'] for d in record[5]],
                "actors_names": [a['name'] for a in record[6]],
                "writers_names": [w['name'] for w in record[7]],
                "directors": record[5],
                "actors": record[6],
                "writers": record[7]
            }
        }


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def load_data_to_es(es_client, transformed_data):
    """ Загрузка данных в Elasticsearch с использованием bulk API"""
    helpers.bulk(es_client, transformed_data)


def etl_process():
    """ Основной ETL процесс"""
    pg_conn = get_pg_connection()
    es_client = get_es_client()

    try:
        while True:
            last_synced_id = get_last_synced_id(pg_conn) or 0
            records = extract_data(pg_conn, last_synced_id)
            if not records:
                print("Нет новых записей для обработки. Ожидание...")
                time.sleep(60)
                continue

            transformed_data = list(transform_data(records))
            load_data_to_es(es_client, transformed_data)

            # Обновление состояния после успешной загрузки
            new_last_synced_id = records[-1][0]
            update_last_synced_id(pg_conn, new_last_synced_id)
            print(f"Обработано и загружено {len(records)} записей. Последний ID: {new_last_synced_id}")

    except Exception as e:
        print(f"Ошибка во время ETL процесса: {str(e)}")
    finally:
        pg_conn.close()


if __name__ == "__main__":
    etl_process()
