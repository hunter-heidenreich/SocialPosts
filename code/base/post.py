import re
import string

from abc import abstractmethod, ABC, abstractstaticmethod
from collections import Counter

import spacy

import numpy as np

from nltk.tokenize import word_tokenize

SP = spacy.load('en_core_web_sm')


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

    def set_meta(self, prop, value):
        """
        Adds additional meta information to this post object
        """
        self._meta[prop] = value

    def get_meta(self, prop):
        return self._meta.get(prop, None)

    def set_text(self, text):
        self._text = text

    def get_text(self):
        return self._text

    @abstractmethod
    def load_from_file(self, filename):
        pass

    @staticmethod
    @abstractmethod
    def format_time(s):
        pass

    def set_time(self, timestr):
        self._created_at = self.format_time(timestr)

    def get_time(self):
        return self._created_at

    def __repr__(self):
        return f'Post<{self._uid}>'

    def __hash__(self):
        return int(self._uid)

    def token_count(self):
        cnt = len(re.split('\s+', self._text))
        for comment in self._comments.values():
            cnt += comment.token_count()

        return cnt

    def comment_count(self):
        direct, nested = 0, 0
        for comment in self._comments.values():
            direct += 1
            d, n = comment.comment_count()
            nested += d + n

        return direct, nested

    def preprocess_thread(self):
        """
        Given a post (or sub-post), pre-processes the
        text of that post to yield a "cleaned up" text
        :return: The cleaned text to represent discourse
        """
        # to lowercase
        text = self._text.lower()

        # strip URLs
        text = re.sub(r'https?:\S+', '', text)

        # remove punctuation
        text = text.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))

        # tokenize
        tokens = word_tokenize(text)

        # remove empty tokens
        tokens = [token for token in tokens if token]

        # remove stop words
        all_stopwords = SP.Defaults.stop_words
        tokens = [token for token in tokens if token not in all_stopwords]

        # remove 1 character tokens
        tokens = [token for token in tokens if len(token) > 1]

        # combine string
        ts = ' '.join(tokens)

        for cid in self._comments:
            ts += ' ' + self._comments[cid].preprocess_thread()

        return ts

    def stat(self):
        print(f'Object: {self}\n')

        cnt = self.token_count()
        print(f'Token count (by white-space): {cnt} (10^{np.log10(cnt):.2f})\n')

        # number of comments
        direct, nested = self.comment_count()
        print(f'Direct comments: {direct}')
        print(f'Nested comments: {nested}')
        print(f'Total comments: {direct + nested}')
