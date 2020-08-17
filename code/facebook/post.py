import json

from datetime import datetime
from tqdm import tqdm

import sys
sys.path.append('code/')

from base.post import Post


class FBPost(Post):

    def __init__(self, name, uid):
        super().__init__(uid)

        self._meta['name'] = name

    @staticmethod
    def get_time(timestr):
        return datetime.strptime(timestr, '%Y-%m-%dT%H:%M:%S+0000')

    def set_time(self, timestr):
        self._created_at = self.get_time(timestr)

    def _load_comment(self, cs):
        comm = FBPost(self._meta['name'], cs['id'])
        comm.set_time(cs['created_time'])
        comm.set_text(cs['message'])

        #  recursively build nested reply structure
        if 'replies' in cs:
            for r in cs['replies']:
                comm._load_comment(r)

        self._comments[comm.__hash__()] = comm

    def load_from_file(self, filename):
        try:
            for k, v in json.load(open(filename + 'posts.json')).items():
                if k == 'created_time':
                    self.set_time(v)
                elif k in ['description', 'message', 'story']:
                    self.set_text(v)
        except FileNotFoundError:
            pass

        try:
            for comment in tqdm(json.load(open(filename + 'replies.json'))):
                self._load_comment(comment)
        except FileNotFoundError:
            pass
        except json.decoder.JSONDecodeError:
            pass

    def __repr__(self):
        return f'Post<{self._meta["name"]}::{self._uid}>'


if __name__ == '__main__':

    name = 'Occupy_Democrats'
    uid = '1244975748928810'
    file = f'/Users/hsh28/PycharmProjects/BuzzFace/data/{name}/{uid}/'

    p = FBPost(name, uid)
    p.load_from_file(file)
    p.stat()
