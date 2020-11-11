import os
import re
import json

import pandas as pd

from collections import defaultdict
from tqdm import tqdm
from glob import glob

from base.dataset import Dataset
from twitter.thread import TwitterThread
from twitter.tweet import Tweet

import matplotlib.pyplot as plt


class TwitterData(Dataset):

    def load(self, quotes=False):
        reg = re.compile('_(\d+)-(\w{4,6})\.')
        paths = defaultdict(dict)
        for f in glob('/Users/hsh28/data/threads/*.json'):
            match = reg.search(f)
            if match:
                paths[match.group(1)][match.group(2)] = f

        print(len(paths), 'threads found.')
        for rid, ps in tqdm(paths.items()):
            topo = json.load(open(ps['topo']))
            meta = json.load(open(ps['meta']))
            tweets = json.load(open(ps['tweets']))

            thread = TwitterThread.construct_thread(tweets, topo, meta)

            self._data[thread.uid] = thread

        if quotes:
            with open('/Users/hsh28/data/quote_tweets/quote_tweets.json') as ff:
                print(f'Reading quote tweets.')
                for line in tqdm(ff.readlines()):
                    tweet = json.loads(line)
                    quoted = tweet['quoted_status']

                    t = Tweet.from_json(tweet)
                    q = Tweet.from_json(quoted)
                    q.name = 'quote'
                    q.add_comment(t)

                    out = TwitterThread(q.name, uid=q.uid)
                    out.posts[q.uid] = q

                    self._data[out.uid] = out

    def write_post_replies(self):
        total_pairs = 0
        total_posts = 0
        outpath = f'data/twitter/'

        for pagename, page in tqdm(self._data.items()):
            # print(page)
            path = outpath + str(page.name) + '/'
            os.makedirs(path, exist_ok=True)

            ts, ps = [], []
            for pid, post in page.posts.items():
                texts, pairs = post.extract_post_reply_pairs(source=True)
                ts.extend(texts.values())
                ps.extend(pairs.values())

                total_pairs += len(pairs)
                total_posts += len(texts)

            out = '\n'.join([json.dumps(p) for p in ps])
            with open(path + 'pairs.json', 'a+') as ff:
                ff.write(out + '\n')

            out = '\n'.join([json.dumps(p) for p in ts])
            with open(path + 'text.json', 'a+') as ff:
                ff.write(out + '\n')

        print(f'Wrote {total_pairs} post-reply pairs.')
        print(f'Wrote {total_posts} unique posts.')

    def post_distribution(self):
        sizes = []

        for pagename, page in self._data.items():

            assert len(page.posts) == 1

            for pid, post in tqdm(page.posts.items()):
                texts, _ = post.extract_post_reply_pairs()
                sizes.append(len(texts))

        plt.hist(sizes, bins=150)
        plt.show()

        print(pd.DataFrame(sizes).describe())


if __name__ == '__main__':
    data = TwitterData()
    data.load(quotes=True)
    # data.stat()
    data.write_post_replies()
    # data.post_distribution()

