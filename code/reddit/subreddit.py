import os

from tqdm import tqdm

import sys
sys.path.append('code/')

from base.stream import Stream
from reddit.post import RedditPost


class SubReddit(Stream):

    def __init__(self, name, uid=None):
        super().__init__(name, domain='Reddit', uid=uid)


if __name__ == '__main__':
    sub = SubReddit('r/CMV')
    print(sub)

    # load data from "Before Name Calling" paper
    p = '/Users/hsh28/PycharmProjects/ah-stahp/naacl2018-before-name-calling-habernal-et-al/data/cmv-full-2017-09-22/'
    files = [f for f in os.listdir(p) if os.path.isfile(os.path.join(p, f))]

    for f in tqdm(files):
        comments = RedditPost.load_comments_from_file(os.path.join(p, f))
        root = RedditPost.reconstruct_threads_from_submission(comments)
        sub.add_post(root)

    sub.stat()
