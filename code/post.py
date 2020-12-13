import re
import html

from re import sub, match
from abc import ABC, abstractmethod
from datetime import datetime


class UniversalPost(ABC):

    def __init__(self, post_id, text='', author=None, created_at=None, board_id=None, reply_to=None, root_id=None, platform=None, lang=None):
        # a unique identifier
        self._post_id = post_id

        # the text of the post
        self._text = text

        # the username/name of the author
        self._author = author

        # created datetime object
        self._created_at = created_at

        # any grouping that this post falls under
        self._board_id = board_id

        # collection of IDs this post was generated in reply to
        self._reply_to = set() if not reply_to else set(reply_to)

        # the root of this post's discussion DAG (None if it is the originator)
        self._root_id = root_id

        # platform name
        self._platform = platform

        # language
        self._lang = lang

    def __hash__(self):
        return self._post_id

    def __repr__(self):
        return f'UniveralPost<{self.platform}/{self.board}/{self._post_id}/{self.author}/{self.created_at}::{self.text[:40]}>'

    @property
    def post_id(self):
        return self._post_id

    @property
    def text(self):
        return self._text

    @text.setter
    def text(self, t):
        self._text = t

    @property
    def author(self):
        return self._author

    @author.setter
    def author(self, a):
        self._author = a

    @property
    def created_at(self):
        return self._created_at

    @created_at.setter
    def created_at(self, x):
        if type(x) == str:
            self._string_to_creation(x)
        elif type(x) == float:
            self._created_at = datetime.fromtimestamp(x)
        else:
            self._created_at = x

    @abstractmethod
    def _string_to_creation(self, x):
        pass

    @property
    def reply_to(self):
        return self._reply_to

    @reply_to.setter
    def reply_to(self, reply_to):
        self._reply_to = reply_to

    def add_reply_to(self, tid):
        self._reply_to.add(tid)

    def remove_reply_to(self, tid):
        self._reply_to.remove(tid)

    @property
    def board(self):
        return self._board_id

    @board.setter
    def board(self, bid):
        self._board_id = bid

    @property
    def root(self):
        return self._root_id

    @root.setter
    def root(self, rid):
        self._root_id = rid

    @property
    def platform(self):
        return self._platform

    @platform.setter
    def platform(self, p):
        self._platform = p

    @property
    def lang(self):
        return self._lang

    @lang.setter
    def lang(self, lang):
        self._lang = lang

    def from_json(self, data):
        self._post_id = data['post_id']
        self.text = data['text']
        self.author = data['author']
        self.created_at = datetime.fromtimestamp(data['created_at'])

        for pid in data['reply_to']:
            self.add_reply_to(pid)

        self.board = data['board_id']
        self.root = data['root_id']
        self.platform = data['platform']

    def to_json(self):
        return {
            'post_id': self._post_id,
            'text': self.text,
            'author': self.author,
            'created_at': self.created_at.timestamp() if self.created_at else None,
            'reply_to': list(self.reply_to),
            'board_id': self.board,
            'root_id': self.root,
            'platform': self.platform
        }

    def conversational_stats(self):
        return {
            'posts': 1,
            'voices': {self.author} if self.author else set()
        }

    def token_stats(self):
        tokens = re.split('\s+', self.text)
        normal = set(tokens)
        lower = {n.lower() for n in normal}
        return {
            'unique': normal,
            'unique_lower': lower,
            'tokens': len(tokens)
        }

    def get_names(self):
        if self.author:
            return {self.author}

        return set()

    def redact(self, name_map):
        for name, pseudo in name_map.items():
            self.text = re.sub(name, pseudo, self.text)

        if self.author in name_map:
            self.author = name_map[self.author]


class Tweet(UniversalPost):

    def __init__(self, **args):
        super(Tweet, self).__init__(**args)

        if type(self.created_at) == str:
            self._string_to_creation(self.created_at)

    def _string_to_creation(self, timestr):
        self.created_at = datetime.strptime(timestr, '%a %b %d %H:%M:%S +0000 %Y')

    def get_names(self):
        names = re.findall(r'@([^\s:]+)', self.text)

        return super(Tweet, self).get_names() | set(names)

    def redact(self, name_map):
        for name in re.findall(r'@([^\s:]+)', self.text):
            self.text = self.text.replace(name, name_map[name])

        self.author = name_map[self.author]


class FBPost(UniversalPost):

    TEXT = ['name', 'message', 'story', 'description', 'caption']

    def __init__(self, **args):
        super(FBPost, self).__init__(**args)

        if type(self.created_at) == str:
            self._string_to_creation(self.created_at)

    def _string_to_creation(self, x):
        self.created_at = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S+0000')

    @staticmethod
    def find_text(raw_post):
        text = ''
        for key in FBPost.TEXT:
            text = (text + ' ' + raw_post.get(key, '')).strip()

        if len(text) == 0:
            ks = set(raw_post.keys())
            ks -= set(FBPost.TEXT)

            ignore_keys = {'picture', 'link', 'video', 'updated_time', 'id', 'created_time', 'shares', 'text'}
            ks -= ignore_keys

            if ks:
                print(ks)
                print('Empty source post...')
                import pdb
                pdb.set_trace()

        return text


class RedditPost(UniversalPost):
    def _string_to_creation(self, x):
        self.created_at = datetime.fromtimestamp(float(x))

    def get_names(self):
        names = re.findall(r'/?u/([A-Za-z0-9_-]+)', self.text)

        return super(RedditPost, self).get_names() | set(names)

    def redact(self, name_map):
        for k, v in name_map.items():
            self.text = re.sub(k, v, self.text)

        if self.author:
            self.author = name_map[self.author]


class ChanPost(UniversalPost):
    def _string_to_creation(self, x):
        pass

    @staticmethod
    def exclude_replies(comment):
        refs = re.findall('>>(\d+)', comment)

        lines = comment.split("\n")
        lines = filter(lambda x: not bool(match(">>(\d+)", x.strip())), lines)
        comment = "\n".join(lines)
        comment = sub(">>(\d+) ", " ", comment)

        return comment, refs

    @staticmethod
    def clean_text(comment):
        comment = html.unescape(comment)
        comment = sub("<w?br/?>", "\n", comment)
        comment = sub("<a href=\".+\" class=\"(\w+)\">", \
                      " ", comment)
        comment = sub("</a>", " ", comment)
        comment = sub("<span class=\"(\w+)\">", " ", comment)
        comment = sub("</span>", " ", comment)
        comment = sub("<pre class=\"(\w+)\">", " ", comment)
        comment = sub("</pre>", " ", comment)

        comment, rfs = ChanPost.exclude_replies(comment)

        comment = sub("[^\x00-\x7F]", " ", comment)

        comment = sub("&(amp|lt|gt|ge|le)(;|)", " ", comment)

        comment = sub("\\s\\s+", " ", comment)
        comment = sub("\n", " ", comment)
        comment = str(comment).strip()

        return comment, rfs
