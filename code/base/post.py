import re

from abc import abstractmethod, ABC

import numpy as np


class Post(ABC):

    """
    Base class for posts on a stream
    """

    def __init__(self, uid):
        self._uid = uid

        self._created_at = None

        self._text = ''
        self._comments = {}
        self._meta = {}

    @abstractmethod
    def load_from_file(self, filename):
        pass

    def __repr__(self):
        return f'Post<{self._uid}>'

    def __hash__(self):
        return int(self._uid)

    def token_count(self):
        return len(re.split('\s+', self._text))

    def stat(self):
        print(f'Object: {self}\n')

        cnt = self.token_count()
        print(f'Token count (by white-space): {cnt} (10^{np.log10(cnt):.2f})\n')
