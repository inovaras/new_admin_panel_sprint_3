from typing import Generator, List

from state import *
from get_connections import *


def transform_filmwork(records: List[dict]) -> Generator[dict, None, None]:
    """Преобразование данных в формат для Elasticsearch."""
    for record in records:
        try:
            yield {
                "_index": settings.filmwork_index_name,
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


def transform_genres(records: List[dict]) -> Generator[dict, None, None]:
    """Преобразование данных жанров в формат для Elasticsearch."""
    for record in records:
        yield {
            "_index": settings.genres_index_name,
            "_id": record["name"],
            "_source": {
                "id": record["id"],
                "name": record["name"]
            }
        }


def transform_persons(records: List[Dict[str, Any]]) -> Generator[Dict[str, Any], None, None]:
    """Преобразование данных персоналий в формат для Elasticsearch."""
    for record in records:
        yield {
            "_index": settings.persons_index_name,
            "_id": record["person_id"],
            "_source": {
                "uuid": record["person_id"],
                "full_name": record["full_name"],
                "movies": record["movies"]
            }
        }