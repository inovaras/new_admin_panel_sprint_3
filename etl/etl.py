import os
import time
import json
import psycopg2
from elasticsearch import Elasticsearch, helpers
import backoff
from dotenv import load_dotenv
import logging
import sys

load_dotenv()

from state import JsonFileStorage, State


POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'theatre-db')
POSTGRES_PORT = int(os.environ.get('POSTGRES_PORT', 5432))
POSTGRES_USER = 'app'
POSTGRES_PASSWORD = '123qwe'
POSTGRES_DB = 'movies_database'

ELASTICSEARCH_HOST = os.environ.get('ELASTICSEARCH_HOST', 'elasticsearch')
ELASTICSEARCH_PORT = int(os.environ.get('ELASTICSEARCH_PORT'))
INDEX_NAME = "movies"
STATE_FILE_PATH = "sync_state.json"


def get_pg_connection():
    """ Подключение к PostgreSQL """
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )

def extract_data(conn, last_synced_id=None, batch_size=100):
    """ Извлечение данных из PostgreSQL """
    with conn.cursor() as cursor:
        # Если last_synced_id пустой, пропускаем условие фильтрации
        if last_synced_id:
            cursor.execute("""
                SELECT fw.id, fw.title, fw.description, fw.rating, 
                       array_agg(DISTINCT g.name) AS genres,
                       array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director') AS directors,
                       array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor') AS actors,
                       array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer') AS writers
                FROM content.film_work fw
                LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
                LEFT JOIN content.genre g ON gfw.genre_id = g.id
                LEFT JOIN content.person_film_work pfw ON fw.id = pfw.film_work_id
                LEFT JOIN content.person p ON pfw.person_id = p.id
                WHERE fw.id > %s
                GROUP BY fw.id
                ORDER BY fw.id ASC
                LIMIT %s;
            """, (last_synced_id, batch_size))
        else:
            cursor.execute("""
                SELECT fw.id, fw.title, fw.description, fw.rating, 
                       array_agg(DISTINCT g.name) AS genres,
                       array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director') AS directors,
                       array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor') AS actors,
                       array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer') AS writers
                FROM content.film_work fw
                LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
                LEFT JOIN content.genre g ON gfw.genre_id = g.id
                LEFT JOIN content.person_film_work pfw ON fw.id = pfw.film_work_id
                LEFT JOIN content.person p ON pfw.person_id = p.id
                GROUP BY fw.id
                ORDER BY fw.id ASC
                LIMIT %s;
            """, (batch_size,))
        return cursor.fetchall()



def transform_data(records):
    """ Преобразование данных в формат для Elasticsearch """
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
                "directors_names": record[5],
                "actors_names": record[6],
                "writers_names": record[7]
            }
        }


@backoff.on_exception(backoff.expo, Exception, max_tries=10)
def get_es_client():
    """ Подключение к Elasticsearch с попытками повторного подключения """
    time.sleep(0.1)
    return Elasticsearch(
        hosts=[{
            'host': ELASTICSEARCH_HOST,
            'port': ELASTICSEARCH_PORT,
            'scheme': 'http'
        }],
    )


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def load_data_to_es(es_client, transformed_data):
    """ Загрузка данных в Elasticsearch с использованием bulk API """
    try:
        helpers.bulk(es_client, transformed_data)
    except Exception as e:
        logger.error(f"Oшибка: {e}")


def etl_process():
    """ Основной ETL процесс """
    pg_conn = get_pg_connection()
    es_client = get_es_client()
    logger.debug(es_client.info())
    storage = JsonFileStorage(STATE_FILE_PATH)
    state = State(storage)

    try:
        while True:
            last_synced_id = state.get_state('last_synced_id') or 0
            records = extract_data(pg_conn, last_synced_id)
            if not records:
                logger.debug("Нет новых записей для обработки. Ожидание...")
                time.sleep(0.1)
                continue

            transformed_data = list(transform_data(records))
            load_data_to_es(es_client, transformed_data)
            if len(records) != 0:
                new_last_synced_id = records[-1][0]
                logger.debug("Устанавливаем новый статус")
                state.set_state('last_synced_id', new_last_synced_id)
                logger.debug(f"Обработано и загружено {len(records)} записей. Последний ID: {new_last_synced_id}")

    except Exception as e:
            logger.error(f"Ошибка во время ETL процесса: {str(e)}")
    finally:
        pg_conn.close()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    etl_process()
