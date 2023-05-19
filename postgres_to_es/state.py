import abc
import json
import os
from typing import Any
from datetime import datetime


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state):
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self, key: str):
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.state = dict()
        self.file_path = file_path

    def save_state(self, state) -> None:
        """Сохранить состояние в хранилище."""
        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r') as json_file:
                data = json.load(json_file)
                data.update(state)
                state = data
        with open(self.file_path, 'w') as json_file:
            json.dump(state, json_file)

    def retrieve_state(self, key: str):
        """Получить состояние из хранилища."""
        with open(self.file_path, 'r') as json_file:
            state = json.load(json_file)
        return state[key] if key in state else None


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: JsonFileStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        value = value.strftime("%m/%d/%Y, %H:%M:%S")
        self.storage.save_state(state={key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        return datetime.strptime(self.storage.retrieve_state(key), "%m/%d/%Y, %H:%M:%S")
