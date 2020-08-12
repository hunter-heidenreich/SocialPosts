from abc import abstractmethod, ABC


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
        if self._debug:
            print(msg)

    def add_meta(self, prop, value):
        """
        Adds additional meta information to this stream object
        """
        self._meta[prop] = value

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
        self._posts[post.get_property('uid')] = post

    def get_property(self, prop):
        """
        Returns the requested property from this object
        :param prop:
        :return:
        """
        if prop == 'name':
            return self._name
        elif prop == 'uid':
            return self._uid
        elif prop == 'domain':
            return self._domain
        else:
            try:
                return self._meta[prop]
            except KeyError:
                msg = f'WARN: Property "{prop}" not found in {self}. Returning None.'
                self.debug(msg)
                return


if __name__ == '__main__':
    print(Stream('CNN', 'Facebook').__hash__())
    print(Stream('CNN', 'Facebook').__hash__())
