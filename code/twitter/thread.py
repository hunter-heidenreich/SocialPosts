import json

import sys
sys.path.append('code/')

from base.stream import Stream
from twitter.tweet import Tweet


class TwitterThread(Stream):

    def __init__(self, name, uid=None):
        super().__init__(name, domain='Twitter', uid=uid)

    @staticmethod
    def construct_thread(tweets, topology, metas):

        tweet_objs = {tid: Tweet.from_json(data) for tid, data in tweets.items()}
        thread = TwitterThread.bottom_up(topology, tweet_objs, metas)

        assert len(thread) == 1
        thread = thread[0]

        out = TwitterThread(thread.name, uid=thread.uid)
        out.posts[thread.uid] = thread

        return out

    @staticmethod
    def bottom_up(topo, tweets, metas):
        ret = []
        for pid, children in topo.items():
            if pid not in tweets:
                print(f'Warn: Tweet {pid} not found... Using meta placeholder (ID and screen_name).')
                try:
                    m = metas[pid]
                    t = Tweet(m['id'], name=m['user']['screen_name'])
                except KeyError:
                    print(f'No meta information for {pid}')
                    t = Tweet(pid)
            else:
                t = tweets[pid]

            pchildren = TwitterThread.bottom_up(children, tweets, metas)
            for child in pchildren:
                t.add_comment(child)

            ret.append(t)

        return ret


if __name__ == '__main__':
    tweets = json.load(open('/Users/hsh28/data/threads/2020_06_19_11_18_13-threads_1273938129129529345-tweets.json'))
    topology = json.load(open('/Users/hsh28/data/threads/2020_06_19_11_18_13-threads_1273938129129529345-topo.json'))
    metas = json.load(open('/Users/hsh28/data/threads/2020_06_19_11_18_13-threads_1273938129129529345-meta.json'))
    thread = TwitterThread.construct_thread(tweets, topology, metas)
    thread.stat()
