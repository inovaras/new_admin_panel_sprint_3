import json
import os
from typing import Any, Dict


class BaseStorage:
    """Абстрактное хранилище состояния."""
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        raise NotImplementedError

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        raise NotImplementedError

class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл (JSON)."""
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в JSON-файл."""
        with open(self.file_path, 'w') as file:
            json.dump(state, file)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из JSON-файла."""
        if not os.path.exists(self.file_path):
            return {}
        with open(self.file_path, 'r') as file:
            return json.load(file)


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
        return self.state.get(key)
