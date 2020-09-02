import re
import json
import string

from abc import abstractmethod, ABC

import spacy

import numpy as np

from nltk.tokenize import word_tokenize

SP = spacy.load('en_core_web_sm')


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

    def token_count(self):
        """
        Basic token counting function.
        Splits text by 1 or more space charcters
        and counts all non-space chunks.
        Recursively computes the counts of child comments.
        """
        cnt = len(re.split('\s+', self._text))
        for comment in self._comments.values():
            cnt += comment.token_count()

        return cnt

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
        direct = [{'source': self.text, 'reply': comment.text} for comment in self._comments.values()]
        nested = []
        for comment in self._comments.values():
            nested.extend(comment.extract_post_reply_pairs())

        return direct + nested

    def generate_time_series(self, outpath):
        """
        Generates the full,
        linear time series of an event

        SHOULD only be called on source posts
        """
        try:
            zed = {
                'timestamp': self.created_at.timestamp() + 1,
                'comment_type': 0,
                'id': self.uid
            }
        except AttributeError:
            return

        points = [zed]
        for comment in self._comments.values():
            direct, nesteds = comment.extract_time_info()
            direct['comment_type'] = 1  # for direct
            for n in nesteds:
                n['comment_type'] = 2  # for indirect descendant

            points.append(direct)
            points.extend(nesteds)

        points.sort(key=lambda x: x['timestamp'])

        points[0]['delta'] = 1
        points[0]['index'] = 0
        for i in range(1, len(points)):
            points[i]['delta'] = points[i]['timestamp'] - points[i-1]['timestamp'] + 1
            points[i]['index'] = i

        with open(outpath, 'w+') as out:
            for point in points:
                out.write(f'{json.dumps(point)}\n')

    def extract_time_info(self):
        """
        Extracts the necessary time series information for itself,
        and its descendants.

        A 2-tuple is returned to specify the difference between
        top level and nested comments
        """
        series = []
        for cid, comment in self._comments.items():
            direct, nesteds = comment.extract_time_info()

            # add nested
            series.extend(nesteds)

            # append direct commnet
            series.append(direct)

        # datapoint for this comment
        this = {
            'timestamp': self.created_at.timestamp(),
            'id': self.uid
        }

        return this, series

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
