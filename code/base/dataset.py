from abc import ABC, abstractmethod


class Dataset(ABC):

    def __init__(self):
        self._data = {}

    @abstractmethod
    def load(self):
        pass

    def stat(self):
        # summarize the Facebook data we have on disk
        direct, nested = 0, 0
        posts = 0
        for page in self._data.values():
            d, n = page.comment_count()
            direct += d
            nested += n

            posts += page.post_count()

        print(f'Pages: {len(self._data)}\n')
        print(f'Posts: {posts}\n')

        print(f'Direct comments: {direct}')
        print(f'Nested comments: {nested}')
        print(f'Total comments: {direct + nested}\n')
