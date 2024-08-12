import time
import psycopg2
from elasticsearch import Elasticsearch


def get_last_sync():
    # Подключение к Postgres и получение последнего синка
    pass


def save_last_sync(sync_id):
    # Сохранение нового значения синка
    pass


def extract_data(last_sync):
    # Получение данных из Postgres
    pass


def transform_data(raw_data):
    # Преобразование данных
    pass


def load_data_to_es(transformed_data):
    # Загрузка данных в Elasticsearch
    pass


def etl_process():
    while True:
        last_sync = get_last_sync()
        raw_data = extract_data(last_sync)
        transformed_data = transform_data(raw_data)
        load_data_to_es(transformed_data)
        save_last_sync(last_sync)
        time.sleep(60)  # Ожидание 60 секунд перед следующим циклом


if __name__ == "__main__":
    etl_process()
