import os
import json

from glob import glob
from tqdm import tqdm

from base.dataset import Dataset
from reddit.subreddit import SubReddit
from reddit.post import RedditPost

from datasets import load_dataset


class RedditData(Dataset):

    def load(self):
        sid = 'ChangeMyView'
        sub = SubReddit(f'r/{sid}')
        print(sub)

        # load data from "Before Name Calling" paper
        p = '/Users/hsh28/PycharmProjects/ah-stahp/naacl2018-before-name-calling-habernal-et-al/data/cmv-full-2017-09-22/'
        files = [f for f in os.listdir(p) if os.path.isfile(os.path.join(p, f))]

        for f in tqdm(files):
            comments = RedditPost.load_comments_from_file(os.path.join(p, f))
            root = RedditPost.reconstruct_threads_from_submission(comments)
            sub.posts[root.__hash__()] = root

        self._data[sid] = sub

    def load_eli5(self):
        ds = load_dataset('eli5')

        for split, data in ds.items():
            print(split)
            sid = split.split('_')[-1]
            if sid not in self._data:
                self._data[sid] = SubReddit(f'r/{sid}')

            for x in tqdm(data):
                post = RedditPost(x['q_id'])
                post.text = (x['title'] + ' ' + x['selftext']).strip()

                for (a, t) in zip(x['answers']['a_id'], x['answers']['text']):
                    r = RedditPost(a)
                    r.text = t
                    post.add_comment(r)

                self._data[sid].posts[post.__hash__()] = post

    def write_post_replies(self):
        total_pairs = 0
        total_posts = 0
        outpath = f'data/reddit/'

        for pagename, page in self._data.items():
            print(page)
            path = outpath + pagename + '/'
            os.makedirs(path, exist_ok=True)

            ts, ps = [], []
            for pid, post in tqdm(page.posts.items()):
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


if __name__ == '__main__':
    data = RedditData()

    # data.load()
    data.load_eli5()

    # data.stat()

    data.write_post_replies()

