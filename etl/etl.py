from datetime import datetime, timezone
import time
from typing import Generator, List, Optional

import backoff
import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import DictCursor
from pydantic_settings import BaseSettings
from elasticsearch import Elasticsearch, helpers

from state import JsonFileStorage, State, logger


class Settings(BaseSettings):
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    postgres_db: str

    elasticsearch_host: str
    elasticsearch_port: int

    index_name: str = "movies"
    state_file_path: str = "sync_state.json"
    default_sync_time: str = datetime(1970, 1, 1, tzinfo=timezone.utc).isoformat()
    default_sleep_time: int = 5

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()


def get_pg_connection() -> PGConnection:
    """Подключение к PostgreSQL."""
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        database=settings.postgres_db
    )


def get_pg_connection_with_retry() -> PGConnection:
    """Подключение к PostgreSQL с попытками повторного подключения."""
    return backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_tries=5)(get_pg_connection)()


def extract_data(conn: PGConnection, last_synced_time: Optional[str] = None, batch_size: int = 100) -> List[dict]:
    """Извлечение данных из PostgreSQL."""
    logger.debug(f"Последняя дата обновления: {last_synced_time}")
    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            query = """
                SELECT
                   fw.id AS id,
                   fw.title AS title,
                   fw.description AS description,
                   fw.rating AS imdb_rating,
                   fw.type AS type,
                   fw.created AS created,
                   fw.modified AS modified,
                   COALESCE (
                       json_agg(
                           DISTINCT jsonb_build_object(
                               'person_role', pfw.role,
                               'person_id', p.id,
                               'person_name', p.full_name
                           )
                       ) FILTER (WHERE p.id is not null),
                       '[]'
                   ) as persons,
                   array_agg(DISTINCT g.name) as genres
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


def transform_data(records: List[dict]) -> Generator[dict, None, None]:
    """Преобразование данных в формат для Elasticsearch."""
    for record in records:
        try:
            yield {
                "_index": settings.index_name,
                "_id": record["id"],
                "_source": {
                    "id": record["id"],
                    "imdb_rating": record["imdb_rating"],
                    "genres": record["genres"],
                    "title": record["title"],
                    "description": record["description"],
                    "directors_names": [
                        person["person_name"]
                        for person in record["persons"]
                        if person["person_role"] == "director"
                    ],
                    "actors_names": [
                        person["person_name"]
                        for person in record["persons"]
                        if person["person_role"] == "actor"
                    ],
                    "writers_names": [
                        person["person_name"]
                        for person in record["persons"]
                        if person["person_role"] == "writer"
                    ],
                    "directors": [
                        {"id": person["person_id"], "name": person["person_name"]}
                        for person in record["persons"]
                        if person["person_role"] == "director"
                    ],
                    "actors": [
                        {"id": person["person_id"], "name": person["person_name"]}
                        for person in record["persons"]
                        if person["person_role"] == "actor"
                    ],
                    "writers": [
                        {"id": person["person_id"], "name": person["person_name"]}
                        for person in record[7]
                        if person["person_role"] == "writer"
                    ],
                },
            }
        except IndexError as e:
            logger.error(f"Ошибка обработки записи: {record} - {str(e)}")


@backoff.on_exception(backoff.expo, Exception, max_tries=10, jitter=backoff.full_jitter)
def get_es_client() -> Elasticsearch:
    """Подключение к Elasticsearch с попытками повторного подключения."""
    time.sleep(0.1)
    return Elasticsearch(
        hosts=[{
            'host': settings.elasticsearch_host,
            'port': settings.elasticsearch_port,
            'scheme': 'http'
        }],
    )


def create_index_with_mapping(es_client: Elasticsearch) -> None:
    """Создание индекса с маппингом в Elasticsearch."""
    mapping = {
      "settings": {
        "refresh_interval": "1s",
        "analysis": {
          "filter": {
            "english_stop": {
              "type":       "stop",
              "stopwords":  "_english_"
            },
            "english_stemmer": {
              "type": "stemmer",
              "language": "english"
            },
            "english_possessive_stemmer": {
              "type": "stemmer",
              "language": "possessive_english"
            },
            "russian_stop": {
              "type":       "stop",
              "stopwords":  "_russian_"
            },
            "russian_stemmer": {
              "type": "stemmer",
              "language": "russian"
            }
          },
          "analyzer": {
            "ru_en": {
              "tokenizer": "standard",
              "filter": [
                "lowercase",
                "english_stop",
                "english_stemmer",
                "english_possessive_stemmer",
                "russian_stop",
                "russian_stemmer"
              ]
            }
          }
        }
      },
      "mappings": {
        "dynamic": "strict",
        "properties": {
          "id": {
            "type": "keyword"
          },
          "imdb_rating": {
            "type": "float"
          },
          "genres": {
            "type": "keyword"
          },
          "title": {
            "type": "text",
            "analyzer": "ru_en",
            "fields": {
              "raw": {
                "type":  "keyword"
              }
            }
          },
          "description": {
            "type": "text",
            "analyzer": "ru_en"
          },
          "directors_names": {
            "type": "text",
            "analyzer": "ru_en"
          },
          "actors_names": {
            "type": "text",
            "analyzer": "ru_en"
          },
          "writers_names": {
            "type": "text",
            "analyzer": "ru_en"
          },
          "directors": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
              "id": {
                "type": "keyword"
              },
              "name": {
                "type": "text",
                "analyzer": "ru_en"
              }
            }
          },
          "actors": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
              "id": {
                "type": "keyword"
              },
              "name": {
                "type": "text",
                "analyzer": "ru_en"
              }
            }
          },
          "writers": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
              "id": {
                "type": "keyword"
              },
              "name": {
                "type": "text",
                "analyzer": "ru_en"
              }
            }
          }
        }
      }
     }

    if not es_client.indices.exists(index=settings.index_name):
        es_client.indices.create(index=settings.index_name, body=mapping)
        logger.info(f"Индекс {settings.index_name} создан с маппингом")


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def load_data_to_es(es_client: Elasticsearch, transformed_data: List[dict]) -> None:
    """ Загрузка данных в Elasticsearch с использованием bulk API """
    try:
        success, failed = helpers.bulk(es_client, transformed_data, raise_on_error=False)
        if failed:
            logger.error(f"{failed} документ(ов) не удалось проиндексировать.")
        logger.debug(f"Успешно проиндексировано {success} документ(ов).")
    except Exception as e:
        logger.error(f"Ошибка: {e}")


def etl_process() -> None:
    """ Основной ETL процесс """
    pg_conn = get_pg_connection_with_retry()
    es_client = get_es_client()

    create_index_with_mapping(es_client)

    storage = JsonFileStorage(settings.index_name)
    state = State(storage)
    try:
        sleep_time = settings.default_sleep_time
        while True:
            last_synced_time = state.get_state('last_synced_time')
            if last_synced_time is None or not isinstance(last_synced_time, str):
                last_synced_time = settings.default_sync_time

            records = extract_data(pg_conn, last_synced_time)
            if not records:
                logger.debug(f"Нет новых записей для обработки. Ожидание {sleep_time} секунд...")
                time.sleep(sleep_time)
                continue

            sleep_time = settings.default_sleep_time
            transformed_data = list(transform_data(records))
            load_data_to_es(es_client, transformed_data)

            new_last_synced_time = records[-1][6].isoformat()
            state.set_state('last_synced_time', new_last_synced_time)
            logger.debug(f"Обработано и загружено {len(records)} записей. Последняя дата: {new_last_synced_time}")

    except Exception as e:
        logger.error(f"Ошибка во время ETL процесса: {str(e)}")
    finally:
        pg_conn.close()


if __name__ == "__main__":
    etl_process()
