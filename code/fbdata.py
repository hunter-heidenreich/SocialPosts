import os
import json

from glob import glob
from tqdm import tqdm

from base.dataset import Dataset
from facebook.page import FBPage
from facebook.post import FBPost


class FBData(Dataset):

    def load(self):
        for d in [
            'Outlets/data1', 'Outlets/data2',
            'Outlets/data',
            'BuzzFace/data', 'BuzzFace/data2', 'BuzzFace/data3', 'BuzzFace/datafull',
        ]:
            root = f'/Users/hsh28/data/{d}/'
            print(root)
            for f in tqdm(glob(root + '*/*/')):
                pagename, pid = f.split('/')[-3:-1]
                if pagename not in self._data:
                    self._data[pagename] = FBPage(pagename)

                page = self._data[pagename]
                pid = int(pid)
                post = FBPost(pagename, pid)
                post.load_from_file(f)

                if pid not in page.posts:
                    page.posts[pid] = post
                else:
                    page.posts[pid].merge_copies(post)

    def write_post_replies(self):
        total_pairs = 0
        total_posts = 0
        outpath = f'data/fb/'

        for pagename, page in self._data.items():
            print(page)
            path = outpath + pagename + '/'
            os.makedirs(path, exist_ok=True)

            ts, ps = [], []
            for pid, post in tqdm(page.posts.items()):
                texts, pairs = post.extract_post_reply_pairs(source=True)
                ts.extend(texts.values())
                ps.extend(pairs.values())

                total_pairs += len(pairs)
                total_posts += len(texts)

            out = '\n'.join([json.dumps(p) for p in ps])
            with open(path + 'pairs.json', 'a+') as ff:
                ff.write(out + '\n')

            out = '\n'.join([json.dumps(p) for p in ts])
            with open(path + 'text.json', 'a+') as ff:
                ff.write(out + '\n')

        print(f'Wrote {total_pairs} post-reply pairs.')
        print(f'Wrote {total_posts} unique posts.')


if __name__ == '__main__':
    data = FBData()
    data.load()
    # data.stat()
    data.write_post_replies()

