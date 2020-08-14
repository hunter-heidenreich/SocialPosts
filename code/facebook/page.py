from glob import glob

import numpy as np

import sys
sys.path.append('code/')

from base.stream import Stream
from facebook.post import FBPost


class FBPage(Stream):

    def __init__(self, name, uid=None):
        super().__init__(name, domain='Facebook', uid=uid)


if __name__ == '__main__':
    from csv import DictReader

    pages = {}
    page_id_lookup = {}

    bf_path = '/Users/hsh28/PycharmProjects/BuzzFace/data/'

    # read all the FB data from buzzface
    reader = DictReader(open(bf_path + 'facebook-fact-check.csv'))
    for row in reader:

        page_id = int(row['account_id'])
        name = row['Page']
        if page_id not in pages:  # and pages[page_id] is None:
            page_id_lookup[name] = page_id
            p = FBPage(name, uid=page_id)
            p.add_meta('news_category', row['Category'])
            pages[page_id] = p

        p = pages[page_id]

        uid = int(row['post_id'])
        post = FBPost(name, uid)
        post.load_from_file(f'{bf_path}{name.replace(" ", "_")}/{uid}/')

        p.add_post(post)

    # summarize the Facebook data we have on disk
    tokens = 0
    direct, nested = 0, 0
    posts = 0
    for page in pages.values():
        # page.stat()

        tokens += page.token_count()

        d, n = page.comment_count()
        direct += d
        nested += n

        posts += page.post_count()

    print(f'Pages: {len(pages)}\n')
    print(f'Posts: {posts}\n')

    print(f'Token count (by white-space): {tokens} (10^{np.log10(tokens):.2f})\n')

    print(f'Direct comments: {direct}')
    print(f'Nested comments: {nested}')
    print(f'Total comments: {direct + nested}\n')
