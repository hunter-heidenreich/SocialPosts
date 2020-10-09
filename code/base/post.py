from abc import abstractmethod, ABC


class Post(ABC):

    """
    Base class for posts on a stream
    """

    def __init__(self, uid, name=None):
        # unique identifier
        self._uid = uid

        # identifier of the parent
        self._pid = None

        # name identifier for page/domain
        self._name = name

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

    def merge_copies(self, other):
        import pdb

        if self.created_at != other.created_at:
            print('Mismatch in created_at')
            pdb.set_trace()

        if self.text != other.text:
            if len(other.text) > len(self.text):
                self.text = other.text

        for comment_id, comment in other.comments.items():
            if comment_id in self.comments:
                self.comments[comment_id].merge_copies(comment)
            else:
                self.comments[comment_id] = comment

        for k, v in other.meta.items():
            if k in self.meta and self.meta[k] != v:
                pass
            else:
                self.meta[k] = v

    def add_comment(self, comm):
        xid = comm.__hash__()

        if xid in self._comments:
            print(xid)
            comm.merge_copies(self._comments[xid])

        self._comments[xid] = comm

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

    def extract_post_reply_pairs(self, text=None, pairs=None):
        """
        Extracts the unique post text and paired post-replies
        starting with this post as a source
        """
        if not text:
            text = {}

        if not pairs:
            pairs = {}

        text[self.uid] = {'id': self.uid, 'text': self.text}
        for cid in self.comments:
            pairs[(self.uid, cid)] = {'post': self.uid, 'reply': cid}
            if cid not in text:
                text, pairs = self.comments[cid].extract_post_reply_pairs(text=text, pairs=pairs)

        return text, pairs

    def stat(self):
        print(f'Object: {self}\n')

        # number of comments
        direct, nested = self.comment_count()
        print(f'Direct comments: {direct}')
        print(f'Nested comments: {nested}')
        print(f'Total comments: {direct + nested}')
