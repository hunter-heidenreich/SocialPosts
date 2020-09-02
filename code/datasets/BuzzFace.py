import json
from csv import DictReader

from tqdm import tqdm

import sys
sys.path.append('code/')

from facebook.page import FBPage
from facebook.post import FBPost


class BuzzFace:
    ROOT = '/Users/hsh28/PycharmProjects/BuzzFace/data/'

    def __init__(self):
        self._pages = {}
        self._page_id_lookup = {}

        # read all the FB data from buzzface
        reader = DictReader(open(self.ROOT + 'facebook-fact-check.csv'))
        for row in reader:

            page_id = int(row['account_id'])
            name = row['Page']
            if page_id not in self._pages:
                self._page_id_lookup[name] = page_id
                p = FBPage(name, uid=page_id)
                p.meta['news_category'] = row['Category']
                self._pages[page_id] = p

            p = self._pages[page_id]

            uid = int(row['post_id'])
            post = FBPost(name, uid)
            post.load_from_file(f'{self.ROOT}{name.replace(" ", "_")}/{uid}/')

            p.posts[uid] = post

    def generate_time_series(self):
        for pagename, pageid in self._page_id_lookup.items():
            page = self._pages[pageid]

            for pid, post in tqdm(page.posts.items()):
                outpath = f'data/timeseries/buzzface/{pagename}_{pid}.json'
                post.generate_time_series(outpath)

    def generate_post_reply_pairs(self):
        total = 0
        for pagename, pageid in self._page_id_lookup.items():
            page = self._pages[pageid]

            for pid, post in tqdm(page.posts.items()):
                outpath = f'data/post_reply/buzzface/{pagename}_{pid}.json'
                pairs = post.extract_post_reply_pairs()

                if pairs:
                    total += len(pairs)
                    out = '\n'.join([json.dumps(pair) for pair in pairs])
                    with open(outpath, 'w+') as ff:
                        ff.write(out + '\n')

        print(f'Wrote {total} post-reply pairs.')

    def extract_discourse_documents(self, group='page'):
        """
        Extracts a list of text documents based on pre-processing
        of threads (by post)

        Additional options can be added to group these documents
        e.g. By page instead of just generally lumping all our text
        """
        if group == 'page':
            docs = {pageid:
                        {postid: post.preprocess_thread() for postid, post in tqdm(page.posts.items())}
                    for pageid, page in self._pages.items()}
        else:
            docs = {}

        return docs


if __name__ == '__main__':
    # buzzface = BuzzFace()
    # buzzface.generate_post_reply_pairs()

    import re
    from glob import glob

    tokens = 0
    for f in tqdm(glob('data/post_reply/buzzface/*.json')):
        with open(f) as ff:
            for line in ff.readlines():
                data = json.loads(line)

                tokens += len(re.split('\s+', data['source']))
                tokens += len(re.split('\s+', data['reply']))

    print(f'{tokens} tokens observed.')

    # import pdb
    # pdb.set_trace()

    # docs = buzzface.extract_discourse_documents()
    # json.dump(docs, open('buzzface_post_docs.json', 'w+'))

    # json.dump(buzzface._page_id_lookup, open('buzzface_id_lookup.json', 'w+'))

