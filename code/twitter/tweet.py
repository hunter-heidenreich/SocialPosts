import json

from datetime import datetime

import sys
sys.path.append('code/')

from base.post import Post


class Tweet(Post):

    def __init__(self, uid, name=None):
        super().__init__(uid, name=name)

    @staticmethod
    def format_time(timestr):
        """
        Converts a timestring in a Facebook post object
        into a datetime format
        """
        return datetime.strptime(timestr, '%a %b %d %H:%M:%S +0000 %Y')

    def load_from_file(self, filename):
        raise NotImplementedError()

    def __repr__(self):
        return f'Tweet<@{self._name}::{self._uid}>'

    @staticmethod
    def from_json(data):
        tid = data['id']
        name = data['user']['screen_name']
        t = Tweet(tid, name=name)

        t.created_at = data['created_at']
        t.text = data['full_text']

        ents = data['entities']
        if ents:
            if 'media' in ents and ents['media']:
                for media in ents['media']:
                    t.text = t.text.replace(media['url'], media['media_url'])

            if 'urls' in ents and ents['urls']:
                for url in ents['urls']:
                    t.text = t.text.replace(url['url'], url['expanded_url'])

        return t


if __name__ == '__main__':
    tweets = json.load(open('/Users/hsh28/data/threads/2020_06_19_11_18_13-threads_1273938129129529345-tweets.json'))
    for t, v in tweets.items():
        tweet = Tweet.from_json(v)
        import pdb
        pdb.set_trace()
