import json
import logging
import os
import sys
from threading import Lock
from typing import Any, Dict


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class BaseStorage:
    """Абстрактное хранилище состояния."""
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        raise NotImplementedError

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        raise NotImplementedError


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self._lock = Lock()

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в JSON-файл."""
        with self._lock:
            with open(self.file_path, 'w') as file:
                json.dump(state, file)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из JSON-файла."""
        if not os.path.exists(self.file_path) or os.stat(self.file_path).st_size == 0:
            logger.error(f"Файл состояния {self.file_path} не существует или пуст.")
            return {}
        with self._lock:
            try:
                with open(self.file_path, 'r') as file:
                    return json.load(file)
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка декодирования JSON в файле {self.file_path}: {e}")
                return {}


class State:
    """Класс для работы с состоянием."""
    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        state_value = self.state.get(key)
        logger.debug(f"Значение состояния для ключа '{key}': {state_value} (Тип: {type(state_value)})")
        return state_value
