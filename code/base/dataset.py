from abc import ABC, abstractmethod


class Dataset(ABC):

    def __init__(self):
        self._data = {}

    @abstractmethod
    def load(self):
        pass
