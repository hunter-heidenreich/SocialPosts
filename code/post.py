import re
import html

from re import sub, match
from abc import ABC, abstractmethod
from datetime import datetime


class UniversalPost(ABC):

    """
    The Universal Post class.

    This is designed to be the abstract, baseline object
    that all social media posts inherit from.
    The only mandatory field is the post_id.
    """

    def __init__(self, post_id, text='', author=None, created_at=None, board_id=None, reply_to=None, platform=None, lang=None):
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

        # platform name
        self._platform = platform

        # language
        self._lang = lang

    def __hash__(self):
        return self._post_id

    def __repr__(self):
        if self.board:
            return f'UniveralPost<{self.platform}/{self.board}/{self._post_id}/{self.author}/{self.created_at}::{self.text[:50]}>'
        else:
            return f'UniveralPost<{self.platform}/{self._post_id}/{self.author}/{self.created_at}::{self.text[:50]}>'

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
        """
        A function for pre-processing a platform's time strings
        to a proper datetime object.
        Will be specified downstream.
        """
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
        """
        Given an exported JSON object for a Universal Post,
        this function loads the saved data into its fields
        """
        self._post_id = data['post_id']
        self.text = data['text']
        self.author = data['author']
        self.created_at = datetime.fromtimestamp(data['created_at'])

        for pid in data['reply_to']:
            self.add_reply_to(pid)

        self.board = data['board_id']
        self.platform = data['platform']

    def to_json(self):
        """
        Function for exporting a Universal Post
        into a JSON object for storage and later use
        """
        return {
            'post_id': self._post_id,
            'text': self.text,
            'author': self.author,
            'created_at': self.created_at.timestamp() if self.created_at else None,
            'reply_to': list(self.reply_to),
            'board_id': self.board,
            'platform': self.platform
        }

    def get_mentions(self):
        """
        By default, this will simply return the author
        of the post (if available)
        for appropriate anonymization, down-playtform
        """
        if self.author:
            return {self.author}

        return set()

    def redact(self, redact_map):
        """
        Given a set of terms,
        this function will properly redact
        all instances of those terms.

        This function is mainly to use for redacting usernames
        or user mentions, so as to protect users
        """
        for term, replacement in redact_map.items():
            self.text = re.sub(term, replacement, self.text)

        # for in-build anonymization, this will convert to an appropriate username
        if self.author in redact_map:
            self.author = redact_map[self.author]


class Tweet(UniversalPost):

    """
    Twitter post object with additional Twitter-specific features
    """

    def __init__(self, **args):
        super(Tweet, self).__init__(**args)

        if type(self.created_at) == str:
            self._string_to_creation(self.created_at)

    def _string_to_creation(self, timestr):
        self.created_at = datetime.strptime(timestr, '%a %b %d %H:%M:%S +0000 %Y')

    def get_mentions(self):
        # twitter mention regex
        names = re.findall(r'@([^\s:]+)', self.text)

        return super(Tweet, self).get_mentions() | set(names)


class FBPost(UniversalPost):

    """
    Facebook post object
    """

    # List of keys that may contain text data for a Facebook post
    TEXT = ['name', 'message', 'story', 'description', 'caption']

    def __init__(self, **args):
        super(FBPost, self).__init__(**args)

        if type(self.created_at) == str:
            self._string_to_creation(self.created_at)

    def _string_to_creation(self, x):
        self.created_at = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S+0000')

    @staticmethod
    def find_text(raw_post):
        """
        Given a raw post object, this function
        returns the text by searching through the keys
        """
        text = ''
        for key in FBPost.TEXT:
            text = (text + ' ' + raw_post.get(key, '')).strip()

        if len(text) == 0:
            ks = set(raw_post.keys())
            ks -= set(FBPost.TEXT)

            assert len(ks) == 0
            # un-comment if new keys are found
            # ignore_keys = {
            #     'picture', 'link', 'video', 'updated_time', 'id', 'created_time', 'shares', 'text', 'source'
            # }
            # ks -= ignore_keys
            #
            # if ks:
            #     print(ks)
            #     print('Empty source post...')
            #     import pdb
            #     pdb.set_trace()

        return text


class RedditPost(UniversalPost):
    """
    Reddit post object
    """
    def _string_to_creation(self, x):
        self.created_at = datetime.fromtimestamp(float(x))

    def get_mentions(self):
        # Reddit user regex
        names = re.findall(r'/?u/([A-Za-z0-9_-]+)', self.text)

        return super(RedditPost, self).get_mentions() | set(names)


class ChanPost(UniversalPost):
    def _string_to_creation(self, x):
        pass

    @staticmethod
    def exclude_replies(comment):
        """
        Function to remove quotes from a reply
        and return reference to the posts that
        were replied to
        """
        refs = re.findall('>>(\d+)', comment)

        lines = comment.split("\n")
        lines = filter(lambda x: not bool(match(">>(\d+)", x.strip())), lines)
        comment = "\n".join(lines)
        comment = sub(">>(\d+) ", " ", comment)

        return comment, refs

    @staticmethod
    def clean_text(comment):
        """
        Cleans the raw HTML of a cached 4chan post,
        returning both the references and teh comment itself
        """
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
