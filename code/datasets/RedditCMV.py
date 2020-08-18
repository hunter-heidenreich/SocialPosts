import os
import json

from tqdm import tqdm

import sys
sys.path.append('code/')

from reddit.subreddit import SubReddit
from reddit.post import RedditPost


class RedditCMV:

    # load data from "Before Name Calling" paper (Habernal et al., 2018)
    ROOT = '/Users/hsh28/PycharmProjects/ah-stahp/naacl2018-before-name-calling-habernal-et-al/data/cmv-full-2017-09-22/'

    def __init__(self):
        self._sub = SubReddit('r/CMV')

        files = [f for f in os.listdir(self.ROOT) if os.path.isfile(os.path.join(self.ROOT, f))]

        for f in tqdm(files):
            comments = RedditPost.load_comments_from_file(os.path.join(self.ROOT, f))
            root = RedditPost.reconstruct_threads_from_submission(comments)
            self._sub.add_post(root)

    def extract_discourse_documents(self):
        return {post_id: post.preprocess_thread() for post_id, post in tqdm(self._sub.get_posts().items())}


if __name__ == '__main__':
    r = RedditCMV()
    docs = r.extract_discourse_documents()
    json.dump(docs, open('reddit_cmv_post_docs.json', 'w+'))
