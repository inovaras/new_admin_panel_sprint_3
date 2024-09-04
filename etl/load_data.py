from typing import Generator, List

from state import *
from get_connections import *


def load_data_to_es(es_client: Elasticsearch, transformed_data: List[dict]) -> None:
    """ Загрузка данных в Elasticsearch с использованием bulk API """
    try:
        success, failed = helpers.bulk(es_client, transformed_data, raise_on_error=False)
        if failed:
            logger.error(f"{failed} документ(ов) не удалось проиндексировать.")
        logger.debug(f"Успешно проиндексировано {success} документ(ов).")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
