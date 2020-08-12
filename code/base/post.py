from abc import abstractmethod, ABC


class Post(ABC):

    """
    Base class for posts on a stream
    """

    def __init__(self, uid, stream_ref=None):
        self._uid = uid
        self._stream = stream_ref

        self._text = None
        self._meta = {}
