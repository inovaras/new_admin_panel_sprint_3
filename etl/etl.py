from extract_data import *
from transform_data import *
from load_data import *
from create_index import *
from state import State, logger, RedisStorage


def etl_filmwork() -> None:
    """Основной ETL процесс для filmwork."""
    with (get_pg_connection() as pg_conn,
          get_es_client() as es_client,
          get_redis_connection() as redis_conn):

        mapping = {
            "settings": {
                "refresh_interval": "1s",
                "analysis": {
                    "filter": {
                        "english_stop": {
                            "type": "stop",
                            "stopwords": "_english_"
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
                            "type": "stop",
                            "stopwords": "_russian_"
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
                        "type": "text"
                    },
                    "title": {
                        "type": "text",
                        "analyzer": "ru_en",
                        "fields": {
                            "raw": {
                                "type": "keyword"
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
        create_index_with_mapping(es_client, mapping, settings.filmwork_index_name)

        storage = RedisStorage(redis_adapter=redis_conn)
        state = State(storage)
        try:
            # sleep_time = settings.default_sleep_time
            sleep_time = 40
            while True:
                sync_time_key = 'last_synced_time_filmwork'
                last_synced_time = state.get_state(sync_time_key)
                if last_synced_time is None or not isinstance(last_synced_time, str):
                    last_synced_time = settings.default_sync_time

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
                records = extract_data(pg_conn, query, last_synced_time)
                if not records:
                    logger.debug(f"Нет новых записей для обработки. Ожидание {sleep_time} секунд...")
                    time.sleep(sleep_time)
                    continue

                sleep_time = settings.default_sleep_time
                transformed_data = list(transform_filmwork(records))
                load_data_to_es(es_client, transformed_data)

                new_last_synced_time = records[-1][6].isoformat()
                state.set_state(sync_time_key, new_last_synced_time)
                logger.debug(f"Обработано и загружено {len(records)} фильмов. Последняя дата: {new_last_synced_time}")

        except Exception as e:
            logger.error(f"Ошибка во время ETL процесса фильмов: {str(e)}")


def etl_genres()-> None:
    """Основной ETL процесс для genres."""
    with (get_pg_connection() as pg_conn,
          get_es_client() as es_client,
          get_redis_connection() as redis_conn):

        mapping = {
            "mappings": {
                "properties": {
                    "name": {
                        "type": "keyword"
                    }
                }
            }
        }
        create_index_with_mapping(es_client, mapping, settings.genres_index_name)

        storage = RedisStorage(redis_adapter=redis_conn)
        state = State(storage)
        try:
            sleep_time = settings.default_sleep_time
            sleep_time = 1
            while True:
                sync_time_key = 'last_synced_time_genres'
                try:
                    last_synced_time = state.get_state(sync_time_key)
                except KeyError as k_err:
                    last_synced_time = settings.default_sync_time
                    state.set_state(sync_time_key, last_synced_time)
                if last_synced_time is None or not isinstance(last_synced_time, str):
                    last_synced_time = settings.default_sync_time

                query = """
                    SELECT DISTINCT g.name
                    FROM content.genre g
                """
                records = extract_data(pg_conn, query, last_synced_time)
                if not records:
                    logger.debug(f"Нет новых записей для обработки. Ожидание {sleep_time} секунд...")
                    time.sleep(sleep_time)
                    continue

                sleep_time = settings.default_sleep_time
                transformed_data = list(transform_genres(records))
                load_data_to_es(es_client, transformed_data)

                new_last_synced_time = records[-1][6].isoformat()
                state.set_state(sync_time_key, new_last_synced_time)
                logger.debug(f"Обработано и загружено {len(records)} жанров. Последняя дата: {new_last_synced_time}")

        except Exception as e:
            logger.error(f"Ошибка во время ETL процесса жанров: {str(e)}")


def etl_persons()-> None:
    """Основной ETL процесс для persons."""
    with (get_pg_connection() as pg_conn,
          get_es_client() as es_client,
          get_redis_connection() as redis_conn):

        mapping = {
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "uuid": {
                        "type": "keyword"
                    },
                    "full_name": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "movies": {
                        "type": "text"
                    }
                }
            }
        }
        create_index_with_mapping(es_client, mapping, settings.persons_index_name)

        storage = RedisStorage(redis_adapter=redis_conn)
        state = State(storage)
        try:
            # sleep_time = settings.default_sleep_time
            sleep_time = 50
            while True:
                #TODO change key
                sync_time_key = 'last_synced_time_persons'
                last_synced_time = state.get_state(sync_time_key)
                if last_synced_time is None or not isinstance(last_synced_time, str):
                    last_synced_time = settings.default_sync_time

                query = """
                        SELECT
                            p.id AS person_id,
                            p.full_name,
                            json_agg(DISTINCT fw.title) AS movies
                        FROM content.person p
                        LEFT JOIN content.person_film_work pfw ON p.id = pfw.person_id
                        LEFT JOIN content.film_work fw ON pfw.film_work_id = fw.id
                        GROUP BY p.id, p.full_name;
                    """
                records = extract_data(pg_conn, query, last_synced_time)
                if not records:
                    logger.debug(f"Нет новых записей для обработки. Ожидание {sleep_time} секунд...")
                    time.sleep(sleep_time)
                    continue

                sleep_time = settings.default_sleep_time
                transformed_data = list(transform_genres(records))
                load_data_to_es(es_client, transformed_data)

                new_last_synced_time = records[-1][6].isoformat()
                state.set_state(sync_time_key, new_last_synced_time)
                logger.debug(
                    f"Обработано и загружено {len(records)} персоналий. Последняя дата: {new_last_synced_time}")

        except Exception as e:
            logger.error(f"Ошибка во время ETL процесса персоналий: {str(e)}")



if __name__ == "__main__":
    etl_filmwork()
