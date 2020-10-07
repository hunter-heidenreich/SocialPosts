from glob import glob
from tqdm import tqdm

from base.dataset import Dataset
from facebook.page import FBPage
from facebook.post import FBPost


class FBData(Dataset):

    def load(self):
        for d in ['data', 'data2', 'data3', 'datafull']:
            root = f'/Users/hsh28/data/BuzzFace/{d}/'
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


if __name__ == '__main__':
    data = FBData()
    data.load()
    data.stat()

