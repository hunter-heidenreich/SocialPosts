from datetime import datetime
from abc import ABC


class Stream(ABC):

    """
    Base class for any streaming feed.
    This could be a Facebook page or a Twitter feed, a sub-reddit,
    or some other social media oriented stream.
    """

    def __init__(self, name, domain=None, uid=None, debug=True):

        # text name
        self._name = name

        # Website domain
        self._domain = domain

        # unique identifier
        self._uid = uid

        # Whether debugging is enabled
        self._debug = debug

        # additional, domain-dependent meta information storage
        self._meta = {}

        # Dictionary of posts keyed by their uid
        self._posts = {}

    def __repr__(self):
        return f'Stream<{self._domain}::{self._name}({self._uid})>'

    def __hash__(self):
        return self._uid if self._uid else hash(f'{self._domain}::{self._name}')

    def debug(self, msg):
        """Prints a message, based on whether debugging is enabled"""
        if self._debug:
            print(f'{datetime.now()}: {msg}')

    @property
    def name(self):
        return self._name

    @property
    def domain(self):
        return self._domain

    @property
    def uid(self):
        return self._uid

    @property
    def posts(self):
        return self._posts

    @property
    def meta(self):
        return self._meta

    def post_count(self):
        """Counts the number of posts in this stream"""
        return len(self._posts)

    def comment_count(self):
        """
        Recursively computes all direct children counts (posts)
        and comments.
        """
        direct, nested = 0, 0
        for post in self._posts.values():
            d, n = post.comment_count()
            direct += d
            nested += n

        return direct, nested

    def stat(self):
        print(f'Page: {self}\n')

        print(f'Posts: {len(self._posts)}\n')

        direct, nested = self.comment_count()

        print(f'Direct comments: {direct}')
        print(f'Nested comments: {nested}')
        print(f'Total comments: {direct + nested}\n')

