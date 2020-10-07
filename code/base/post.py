from abc import abstractmethod, ABC


class Post(ABC):

    """
    Base class for posts on a stream
    """

    def __init__(self, uid):
        # unique identifier
        self._uid = uid

        # identifier of the parent
        self._pid = None

        # datetime created
        self._created_at = None

        # text of post
        self._text = ''

        # dictionary of child posts in (uid, Post)
        self._comments = {}

        # any additional meta features
        self._meta = {}

    def __repr__(self):
        return f'Post<{self._uid}>'

    def __hash__(self):
        return self._uid

    @property
    def uid(self):
        return self._uid

    @property
    def pid(self):
        return self._pid

    @pid.setter
    def pid(self, value):
        self._pid = value

    @property
    def meta(self):
        return self._meta

    @property
    def text(self):
        return self._text

    @text.setter
    def text(self, value):
        self._text = value

    @property
    def comments(self):
        return self._comments

    @abstractmethod
    def load_from_file(self, filename):
        pass

    @staticmethod
    @abstractmethod
    def format_time(s):
        pass

    @property
    def created_at(self):
        return self._created_at

    @created_at.setter
    def created_at(self, timestr):
        self._created_at = self.format_time(timestr)

    def comment_count(self):
        """
        Recursively computes the number of comments
        that are descendants of this comment.
        Returns a 2-tuple of all the direct children
        of this post and all the nested comments.
        """
        direct, nested = 0, 0
        for comment in self._comments.values():
            direct += 1
            d, n = comment.comment_count()
            nested += d + n

        return direct, nested

    def extract_post_reply_pairs(self):
        """
        Extracts the unique post text and paired post-replies
        starting with this post as a source
        """
        text = [{'id': self.uid, 'text': self.text}]
        pairs = [{'reply': comment.uid, 'post': self.uid} for comment in self._comments.values()]

        for comment in self._comments.values():
            t, p = comment.extract_post_reply_pairs()
            text += t
            pairs += p

        return text, pairs

    def stat(self):
        print(f'Object: {self}\n')

        # number of comments
        direct, nested = self.comment_count()
        print(f'Direct comments: {direct}')
        print(f'Nested comments: {nested}')
        print(f'Total comments: {direct + nested}')
