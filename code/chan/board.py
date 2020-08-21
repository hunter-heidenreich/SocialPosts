import json

from tqdm import tqdm

import sys
sys.path.append('code/')

from base.stream import Stream
from chan.post import ChanPost


class Board(Stream):

    def __init__(self, name, uid=None):
        super().__init__(name, domain='4chan', uid=uid)

    def load_from_json(self, path):
        posts = {}
        data = json.load(open(path))

        for k, datum in data.items():
            uid = int(k)

            post = ChanPost(uid)
            post.load_from_file(datum)

            posts[uid] = post

        for uid, post in tqdm(posts.items()):
            pid = post.pid
            if pid:
                posts[pid].comments[uid] = post
            else:
                self.posts[uid] = posts[uid]

        # for now keeping this as is...
        # TODO: Recovering the broader quote structure through ">>{UID}"


if __name__ == '__main__':
    board_path = 'data/'
    news = Board(name='Current News', uid='/news/')
    news.load_from_json(board_path)
    news.stat()
