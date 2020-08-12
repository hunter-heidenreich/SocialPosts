from abc import abstractmethod, ABC


class Stream(ABC):

    def __init__(self, name, domain=None, uid=None):

        self._name = name
        self._domain = domain
        self._uid = uid

        self._posts = []

    def __repr__(self):
        return f'Stream<{self._name}({self._uid})::{self._domain}>'

    def get_post(self, post_id):
        # TODO
        pass


if __name__ == '__main__':
    print(Stream('CNN', 'Facebook'))
