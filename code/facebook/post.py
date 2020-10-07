import json

from datetime import datetime
from tqdm import tqdm

import sys
sys.path.append('code/')

from base.post import Post


class FBPost(Post):

    def __init__(self, name, uid):
        super().__init__(uid, name=name)

    @staticmethod
    def format_time(timestr):
        """
        Converts a timestring in a Facebook post object
        into a datetime format
        """
        return datetime.strptime(timestr, '%Y-%m-%dT%H:%M:%S+0000')

    @staticmethod
    def comment_from_json(name, cs):
        """
        Given the JSON of a Facebook comment
        (already loaded as an object),
        converts it into a comment (to be embedded as a reply)
        :param name:
        :param cs:
        :return:
        """
        comm = FBPost(name, cs['id'])
        comm.created_at = cs['created_time']
        comm.text = cs['message']

        if 'replies' in cs:
            for r in cs['replies']:
                r_comm = FBPost.comment_from_json(name, r)
                comm.add_comment(r_comm)

        return comm

    def load_from_file(self, filename):
        # extract text/meta information
        try:
            for k, v in json.load(open(filename + 'posts.json')).items():
                if k == 'created_time':
                    self.created_at = v
                elif k in ['description', 'message', 'story']:
                    self.text = v

                self.meta[k] = v
        except FileNotFoundError:
            pass

        # check for replies, if they're available
        try:
            for comment in json.load(open(filename + 'replies.json')):
                c = FBPost.comment_from_json(self._name, comment)
                self.add_comment(c)
        except FileNotFoundError:
            try:
                data = json.load(open(filename + 'comments.json'))

                if type(data) == dict and data:
                    try:
                        data = data['data']
                    except KeyError:
                        print('data not found in comments.json')
                        import pdb
                        pdb.set_trace()
                for comment in data:
                    c = FBPost.comment_from_json(self._name, comment)
                    self.add_comment(c)
            except FileNotFoundError:
                pass
            except json.decoder.JSONDecodeError:
                pass
        except json.decoder.JSONDecodeError:
            pass

    def __repr__(self):
        return f'Post<{self._name}::{self._uid}>'


if __name__ == '__main__':

    name = 'Occupy_Democrats'
    uid = '1244975748928810'
    file = f'/Users/hsh28/PycharmProjects/BuzzFace/data/{name}/{uid}/'

    p = FBPost(name, uid)
    p.load_from_file(file)

    outpath = f'data/timeseries/buzzface/{name}_{uid}.json'
    p.stat()

