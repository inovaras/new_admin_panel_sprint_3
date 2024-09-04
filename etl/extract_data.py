from psycopg2.extras import DictCursor
from typing import Optional
from typing import Generator, List

from state import *
from get_connections import *


def extract_data(conn: PGConnection, query, last_synced_time: Optional[str] = None, batch_size: int = 100) -> List[dict]:
    """Извлечение данных из PostgreSQL."""
    logger.debug(f"Последняя дата обновления: {last_synced_time}")
    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:

            cursor.execute(query, (last_synced_time, batch_size))
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Ошибка при извлечении данных: {e}")
        raise
