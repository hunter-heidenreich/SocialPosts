import re
from datetime import datetime

from bs4 import BeautifulSoup
from bs4 import MarkupResemblesLocatorWarning

import sys
sys.path.append('code/')

from base.post import Post

import warnings
# to suppress bs4 warnings about a URL
warnings.filterwarnings('ignore', message='.*looks like a URL.*')
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning, module='bs4')


class ChanPost(Post):

    def __init__(self, uid):
        super().__init__(uid)

    def __hash__(self):
        return self._uid

    @staticmethod
    def format_time(timestr):
        if type(timestr) == int:
            return datetime.fromtimestamp(timestr)
        else:
            raise ValueError()

    def load_from_file(self, raw_thread):
        # booleans
        prop = 'sticky'
        self.meta[prop] = raw_thread.get(prop, -1) == 1

        prop = 'closed'
        self.meta[prop] = raw_thread.get(prop, -1) == 1

        prop = 'bumplimit'
        self.meta[prop] = raw_thread.get(prop, -1) == 1

        prop = 'imagelimit'
        self.meta[prop] = raw_thread.get(prop, -1) == 1

        prop = 'archived'
        self.meta[prop] = raw_thread.get(prop, -1) == 1

        # time
        self.created_at = raw_thread['time']

        # text
        prop = 'semantic_tag'
        self.meta[prop] = raw_thread.get(prop, None)

        prop = 'sub'
        self.meta[prop] = raw_thread.get(prop, '')

        prop = 'com'
        self.meta[prop] = raw_thread.get(prop, '')

        self.author = raw_thread.get('name', 'Anonymous')

        text = (self.meta['sub'] + ' ' + self.meta['com']).strip()
        text = text.replace('<br>', '\n')
        text = text.replace('<br/>', '\n')
        text = text.replace('<wbr>', '')
        text = text.replace('<wbr/>', '')

        soup = BeautifulSoup(text, 'html.parser')
        self.text = soup.text

        # int
        prop = 'unique_ips'
        self.meta[prop] = raw_thread.get(prop, None)

        prop = 'replies'
        self.meta[prop] = raw_thread.get(prop, None)

        prop = 'resto'
        self.meta[prop] = raw_thread.get(prop, None)
        self.pid = int(raw_thread.get(prop, 0))

    def get_post_references(self):
        refs = re.findall('>>\d+\s', self.text)
        refs = [int(r.strip().replace('>', '')) for r in refs]
        refs = [r for r in refs if r != self.pid]
        return refs

    def comment_count(self, rec=True):
        """
        Recursively computes the number of comments
        that are descendants of this comment.
        Returns a 2-tuple of all the direct children
        of this post and all the nested comments.
        """
        direct, nested = 0, 0
        for comment in self._comments.values():
            direct += 1
            if rec:
                d, n = comment.comment_count(rec=False)
                nested += d + n

        return direct, nested

    def merge_copies(self, other):
        import pdb
        if self.created_at != other.created_at:
            print('Mismatch in created_at')
            pdb.set_trace()

        if self.text != other.text:
            if len(other.text) > len(self.text):
                self.text = other.text

        for comment_id, comment in other.comments.items():
            if comment_id in self.comments:
                continue
            else:
                self.comments[comment_id] = comment

        for k, v in other.meta.items():
            if k in self.meta and self.meta[k] != v:
                pass
            else:
                self.meta[k] = v
