from get_connections import *
from state import *


def create_index_with_mapping(es_client: Elasticsearch, mapping, index_name) -> None:
    """Создание индекса с маппингом в Elasticsearch."""

    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name, body=mapping)
        logger.info(f"Индекс {index_name} создан с маппингом")
    else:
        # Получаем текущий маппинг
        current_mapping = es_client.indices.get_mapping(index=index_name)

        # Извлекаем релевантные части для сравнения
        current_mapping_mappings = current_mapping[index_name]['mappings']

        # Сравниваем только настройки и маппинг
        if current_mapping_mappings == mapping['mappings']:
            logger.info(f"Индекс {index_name} уже существует и имеет тот же маппинг.")
        else:
            print(current_mapping)
            print(current_mapping[index_name])
            logger.warning(
                f"Индекс {index_name} существует, но маппинги отличаются.")

            # Удаление старого индекса
            es_client.indices.delete(index=index_name)
            logger.info(f"Старый индекс {index_name} удален")

            # Создание нового индекса с обновленным маппингом
            es_client.indices.create(index=index_name, body=mapping)
            logger.info(f"Новый индекс {index_name} создан с обновленным маппингом")
            new_mapping = es_client.indices.get_mapping(index=index_name)
            print(new_mapping)
            print(new_mapping[index_name])

