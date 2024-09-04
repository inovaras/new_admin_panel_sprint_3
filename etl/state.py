import abc
import json
import logging
import sys
import os
from typing import Any, Dict
from redis.client import Redis
from redis.exceptions import BusyLoadingError, ConnectionError, TimeoutError


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class RedisStorage(BaseStorage):

    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter
        self.key = None

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        self.redis_adapter.hset(name="state", mapping=state)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        state = self.redis_adapter.hgetall(name="state")
        # logger.debug(state)
        return state


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        data = self.retrieve_state()

        data.update(state)
        with open(self.file_path, mode="w") as file:
            json.dump(data, fp=file)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        if not os.path.exists(self.file_path):
            with open(self.file_path, mode="w") as f:
                json.dump({}, f)

            return {}
        else:
            with open(self.file_path) as file:
                data: dict = json.load(file)

            return data


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self.storage.save_state(state={key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        # state = self.storage.retrieve_state()
        # if len(state) == 0:
        #     return None
        # state = state[key]
        # return state
        try:
            state = self.storage.retrieve_state()
            return state.get(key, None)  # Если ключа нет, возвращаем None
        except KeyError:
            return None




if __name__ == "__main__":
    # storage = JsonFileStorage(file_path="storage.json")
    # state = State(storage=storage)
    # my_state = state.get_state(key="mike")
    # state.set_state(key="bob", value=555)

    # Run 3 retries with exponential backoff strategy
    storage = RedisStorage(
        redis_adapter=Redis(
            host="172.18.0.3",
            decode_responses=True,
            # retry=RedisStorage.retry,
            retry_on_error=[BusyLoadingError, ConnectionError, TimeoutError],
        )
    )
    state = State(storage=storage)
    state.set_state(key="bob", value=555)
    my_state = state.get_state(key="bob")
    print(my_state)