import numpy as np

import sys
sys.path.append('code/')

from base.stream import Stream
from facebook.post import FBPost


class FBPage(Stream):

    def __init__(self, name, uid=None):
        super().__init__(name, domain='Facebook', uid=uid)


if __name__ == '__main__':
    # from csv import DictReader
    #
    # pages = {}
    # page_id_lookup = {}
    #
    # bf_path = '/Users/hsh28/PycharmProjects/BuzzFace/data/'
    #
    # # read all the FB data from buzzface
    # reader = DictReader(open(bf_path + 'facebook-fact-check.csv'))
    # for row in reader:
    #
    #     page_id = int(row['account_id'])
    #     name = row['Page']
    #     if page_id not in pages:  # and pages[page_id] is None:
    #         page_id_lookup[name] = page_id
    #         p = FBPage(name, uid=page_id)
    #         p.meta['news_category'] = row['Category']
    #         pages[page_id] = p
    #
    #     p = pages[page_id]
    #
    #     uid = int(row['post_id'])
    #     post = FBPost(name, uid)
    #     post.load_from_file(f'{bf_path}{name.replace(" ", "_")}/{uid}/')
    #
    #     p.posts[uid] = post

    from glob import glob
    from tqdm import tqdm

    pages = {}
    page_id_lookup = {}
    for d in ['data', 'data2', 'data3', 'datafull']:
        root = f'/Users/hsh28/data/BuzzFace/{d}/'
        print(root)

        for f in tqdm(glob(root + '*/*/')):
            pagename, pid = f.split('/')[-3:-1]
            if pagename not in pages:
                pages[pagename] = FBPage(pagename)

            page = pages[pagename]
            pid = int(pid)
            post = FBPost(pagename, pid)
            post.load_from_file(f)

            if pid not in page.posts:
                page.posts[pid] = post
            else:
                # page.posts[pid] = \
                page.posts[pid].merge_copies(post)

    # summarize the Facebook data we have on disk
    direct, nested = 0, 0
    posts = 0
    for page in pages.values():
        d, n = page.comment_count()
        direct += d
        nested += n

        posts += page.post_count()

    print(f'Pages: {len(pages)}\n')
    print(f'Posts: {posts}\n')

    print(f'Direct comments: {direct}')
    print(f'Nested comments: {nested}')
    print(f'Total comments: {direct + nested}\n')

