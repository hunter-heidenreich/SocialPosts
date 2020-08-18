from datetime import datetime
from abc import abstractmethod, ABC

import numpy as np


class Stream(ABC):

    """
    Base class for any streaming feed.
    This could be a Facebook page or a Twitter feed, a sub-reddit,
    or some other social media oriented stream.
    """

    def __init__(self, name, domain=None, uid=None, debug=True):

        self._name = name
        self._domain = domain
        self._uid = uid

        self._debug = debug

        # additional, domain-dependent meta information storage
        self._meta = {}

        self._posts = {}

    def __repr__(self):
        return f'Stream<{self._domain}::{self._name}({self._uid})>'

    def __hash__(self):
        return int(self._uid) if self._uid else hash(f'{self._domain}::{self._name}')

    def debug(self, msg):
        """Prints a message, based on whether debugging is enabled"""
        if self._debug:
            print(f'{datetime.now()}: {msg}')

    def set_meta(self, prop, value):
        """Given a property and a value, updates the meta info dict"""
        self._meta[prop] = value

    def get_meta(self, prop):
        """Gets current prop value from meta"""
        return self._meta.get(prop, None)

    def get_posts(self):
        return self._posts

    def get_post(self, post_id):
        """
        Retrieves a post by its unique identifier
        """
        try:
            return self._posts[post_id]
        except KeyError:
            msg = f'WARN: Property "{post_id}" not found in posts of {self}. Returning None.'
            self.debug(msg)

    def add_post(self, post):
        """
        Adds a post based on its unique identifier
        """
        self._posts[post.__hash__()] = post

    def get_property(self, prop):
        """
        Returns the requested property from this object
        :param prop:
        """
        if prop == 'name':
            return self._name
        elif prop == 'uid':
            return self._uid
        elif prop == 'domain':
            return self._domain
        else:
            return self.get_meta(prop)

    def post_count(self):
        """Counts the number of posts in this stream"""
        return len(self._posts)

    def comment_count(self):
        direct, nested = 0, 0
        for post in self._posts.values():
            d, n = post.comment_count()
            direct += d
            nested += n

        return direct, nested

    def token_count(self):
        tokens = 0
        for post in self._posts.values():
            tokens += post.token_count()
        return tokens

    def stat(self):
        print(f'Page: {self}\n')

        print(f'Posts: {len(self._posts)}\n')

        tokens = self.token_count()
        direct, nested = self.comment_count()

        print(f'Token count (by white-space): {tokens} (10^{np.log10(tokens):.2f})\n')

        print(f'Direct comments: {direct}')
        print(f'Nested comments: {nested}')
        print(f'Total comments: {direct + nested}\n')


if __name__ == '__main__':
    print(Stream('CNN', 'Facebook').__hash__())
    print(Stream('CNN', 'Facebook').__hash__())
