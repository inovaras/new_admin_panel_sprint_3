import time
import backoff
import psycopg2
from elasticsearch import Elasticsearch, helpers
from psycopg2.extensions import connection as PGConnection

import redis
import elasticsearch
from redis.client import Redis


from settings import *


@backoff.on_exception(
    wait_gen=backoff.expo,
    exception=psycopg2.OperationalError,
    jitter=backoff.full_jitter,
    max_value=5,
)
def get_pg_connection() -> PGConnection:
    """Подключение к PostgreSQL."""
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        database=settings.postgres_db
    )


# TODO add handler logs
@backoff.on_exception(
    wait_gen=backoff.expo,
    exception=(elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout),
    jitter=backoff.full_jitter,
    max_value=5,
)
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


@backoff.on_exception(
    backoff.expo,
    exception=(redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError, redis.exceptions.TimeoutError),
    jitter=backoff.full_jitter,
    max_value=5,
)
def get_redis_connection():
    return Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)