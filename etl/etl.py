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
            sleep_time = settings.default_sleep_time
            sync_time_key = 'last_synced_time_filmwork'
            while True:

                last_synced_time = state.get_state(sync_time_key)
                if last_synced_time is None:
                    last_synced_time = settings.default_sync_time
                    logger.debug(
                        f"Ключ состояния {sync_time_key} для фильмов не найден, использовано значение по умолчанию: {last_synced_time}")
                else:
                    # logger.debug(f"Последняя дата обновления фильмов (ключ {sync_time_key}): {last_synced_time}")
                    pass

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
                    # logger.debug(f"Нет новых записей фильмов для обработки. Ожидание {sleep_time} секунд...")
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
            sync_time_key = 'last_synced_time_genres'
            while True:
                last_synced_time = state.get_state(sync_time_key)
                if last_synced_time is None:
                    last_synced_time = settings.default_sync_time
                    logger.debug(
                        f"Ключ состояния {sync_time_key} для жанров не найден, использовано значение по умолчанию: {last_synced_time}")
                else:
                    # logger.debug(f"Последняя дата обновления жанров (ключ {sync_time_key}): {last_synced_time}")
                    pass

                query = """
                    SELECT
                        g.id AS id,
                        g.name AS name,
                        g.modified 
                    FROM content.genre g

                    WHERE g.modified > %s
                    GROUP BY g.id
                    ORDER BY g.modified
                    LIMIT %s;
                """
                records = extract_data(pg_conn, query, last_synced_time)
                if not records:
                    # logger.debug(f"Нет новых записей жанров для обработки. Ожидание {sleep_time} секунд...")
                    time.sleep(sleep_time)
                    continue

                sleep_time = settings.default_sleep_time
                transformed_data = list(transform_genres(records))
                load_data_to_es(es_client, transformed_data)

                new_last_synced_time = records[-1][2].isoformat()
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
                    "full_name": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "movies": {
                        "type": "keyword",
                    }
                }
            }
        }
        create_index_with_mapping(es_client, mapping, settings.persons_index_name)

        storage = RedisStorage(redis_adapter=redis_conn)
        state = State(storage)
        try:
            sleep_time = settings.default_sleep_time
            sync_time_key = 'last_synced_time_persons'

            while True:
                last_synced_time = state.get_state(sync_time_key)
                if last_synced_time is None:
                    last_synced_time = settings.default_sync_time
                    logger.debug(
                        f"Ключ состояния {sync_time_key} для персоналий не найден, использовано значение по умолчанию: {last_synced_time}")
                else:
                    # logger.debug(f"Последняя дата обновления персоналий (ключ {sync_time_key}): {last_synced_time}")
                    pass

                query = """
                        SELECT
                            p.id AS person_id,
                            p.full_name,
                            json_agg(DISTINCT fw.id) AS movies,
                            p.modified 
                        FROM content.person p
                        LEFT JOIN content.person_film_work pfw ON p.id = pfw.person_id
                        LEFT JOIN content.film_work fw ON pfw.film_work_id = fw.id
                        WHERE p.modified > %s
                        GROUP BY p.id
                        ORDER BY p.modified
                        LIMIT %s;
                        
                    """
                records = extract_data(pg_conn, query, last_synced_time)
                if not records:
                    # logger.debug(f"Нет новых записей персоналий для обработки. Ожидание {sleep_time} секунд...")
                    time.sleep(sleep_time)
                    continue

                sleep_time = settings.default_sleep_time
                transformed_data = list(transform_persons(records))
                load_data_to_es(es_client, transformed_data)

                new_last_synced_time = records[-1][3].isoformat()
                state.set_state(sync_time_key, new_last_synced_time)
                logger.debug(
                    f"Обработано и загружено {len(records)} персоналий. Последняя дата: {new_last_synced_time}")

        except Exception as e:
            logger.error(f"Ошибка во время ETL процесса персоналий: {str(e)}")



if __name__ == "__main__":
    etl_filmwork()
