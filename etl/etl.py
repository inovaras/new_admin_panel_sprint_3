from datetime import datetime
import logging
import os
import sys
import time

import backoff
import psycopg2
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

load_dotenv()

from state import JsonFileStorage, State


# POSTGRES_HOST = '172.21.0.2'
POSTGRES_HOST = 'theatre-db'
POSTGRES_PORT = 5432
POSTGRES_USER = 'app'
POSTGRES_PASSWORD = '123qwe'
POSTGRES_DB = 'movies_database'

# ELASTICSEARCH_HOST = '172.22.0.2'
ELASTICSEARCH_HOST = 'elasticsearch'
ELASTICSEARCH_PORT = 9200
INDEX_NAME = "movies"
STATE_FILE_PATH = "sync_state.json"


def get_pg_connection():
    """Подключение к PostgreSQL."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )


def get_pg_connection_with_retry():
    """Подключение к PostgreSQL с попытками повторного подключения."""
    return backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_tries=5)(get_pg_connection)()


def extract_data(conn, last_synced_time=None, batch_size=100):
    """ Извлечение данных из PostgreSQL """
    logger.debug(f"Последняя дата обновления: {last_synced_time}")
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT
                    fw.id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    COALESCE(array_agg(DISTINCT g.name), ARRAY[]::TEXT[]) AS genres,
                    COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director'), ARRAY[]::TEXT[]) AS directors,
                    COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor'), ARRAY[]::TEXT[]) AS actors,
                    COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer'), ARRAY[]::TEXT[]) AS writers,
                    fw.modified
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                WHERE fw.modified > %s
                GROUP BY fw.id
                ORDER BY fw.modified
                LIMIT %s;
            """
            cursor.execute(query, (last_synced_time, batch_size))
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Ошибка при извлечении данных: {e}")
        raise


def transform_data(records):
    """ Преобразование данных в формат для Elasticsearch """
    for record in records:
        try:
            yield {
                "_index": INDEX_NAME,
                "_id": record[0],
                "_source": {
                    "id": record[0],
                    "title": record[1],
                    "description": record[2],
                    "imdb_rating": record[3],
                    "genres": record[4] if isinstance(record[4], list) else [],
                    "directors_names": record[5] if isinstance(record[5], list) else [],
                    "actors": [{"name": actor} for actor in record[6]] if isinstance(record[6], list) else [],
                    "writers_names": record[7] if isinstance(record[7], list) else [],
                    "modified": record[8].isoformat() if isinstance(record[8], datetime) else record[8]
                }
            }
        except IndexError as e:
            logger.error(f"Ошибка обработки записи: {record} - {str(e)}")


@backoff.on_exception(backoff.expo, Exception, max_tries=10)
def get_es_client():
    """Подключение к Elasticsearch с попытками повторного подключения."""
    time.sleep(0.1)
    return Elasticsearch(
        hosts=[{
            'host': ELASTICSEARCH_HOST,
            'port': ELASTICSEARCH_PORT,
            'scheme': 'http'
        }],
    )


def create_index_with_mapping(es_client):
    """Создание индекса с маппингом в Elasticsearch."""
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "title": {"type": "text"},
                "description": {"type": "text"},
                "imdb_rating": {"type": "float"},
                "genres": {"type": "keyword"},
                "directors_names": {"type": "text"},
                "actors": {
                    "type": "nested",
                    "properties": {
                        "name": {"type": "text"}
                    }
                },
                "writers_names": {"type": "text"},
                "modified": {"type": "date"}
            }
        }
    }

    if es_client.indices.exists(index=INDEX_NAME):
        es_client.indices.delete(index=INDEX_NAME)
        logger.info(f"Индекс {INDEX_NAME} удален")

    es_client.indices.create(index=INDEX_NAME, body=mapping)
    logger.info(f"Индекс {INDEX_NAME} создан с маппингом")


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def load_data_to_es(es_client, transformed_data):
    """ Загрузка данных в Elasticsearch с использованием bulk API """
    try:
        success, failed = helpers.bulk(es_client, transformed_data, raise_on_error=False)
        if failed:
            logger.error(f"{failed} документ(ов) не удалось проиндексировать.")
        logger.debug(f"Успешно проиндексировано {success} документ(ов).")
    except Exception as e:
        logger.error(f"Ошибка: {e}")


def etl_process():
    """ Основной ETL процесс """
    pg_conn = get_pg_connection_with_retry()
    es_client = get_es_client()

    create_index_with_mapping(es_client)

    storage = JsonFileStorage(STATE_FILE_PATH)
    state = State(storage)

    try:
        sleep_time = 5

        while True:
            last_synced_time = state.get_state('last_synced_time')

            # Обработка случая, если last_synced_time пустой
            if last_synced_time is None:
                last_synced_time = '1970-01-01T00:00:00'
            elif not isinstance(last_synced_time, str):
                last_synced_time = '1970-01-01T00:00:00'

            records = extract_data(pg_conn, last_synced_time)
            if not records:
                logger.debug(f"Нет новых записей для обработки. Ожидание {sleep_time} секунд...")
                time.sleep(sleep_time)
                continue

            sleep_time = 5
            transformed_data = list(transform_data(records))
            load_data_to_es(es_client, transformed_data)

            new_last_synced_time = records[-1][8].isoformat()
            state.set_state('last_synced_time', new_last_synced_time)
            logger.debug(f"Обработано и загружено {len(records)} записей. Последняя дата: {new_last_synced_time}")

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
