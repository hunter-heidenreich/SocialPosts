from datetime import datetime

from bs4 import BeautifulSoup

import sys
sys.path.append('code/')

from base.post import Post


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

