import os
import json

from glob import glob
from tqdm import tqdm

from base.dataset import Dataset

from chan.board import Board
from chan.post import ChanPost


class ChanData(Dataset):

    def load(self, i=0, board='news'):
        root = '/Users/hsh28/data/4chan/'
        board_path = f'{root}{board}/{i:02d}.json'

        board_data = Board(uid=f'/{board}/', name=board)
        board_data.load_from_json(board_path)

        self._data[board] = board_data

    def write_post_replies(self):
        total_pairs = 0
        total_posts = 0
        outpath = f'data/4chan/'

        for pagename, page in self._data.items():
            print(page)
            path = outpath + pagename + '/'
            os.makedirs(path, exist_ok=True)

            ts, ps = [], []
            for pid, post in tqdm(page.posts.items()):
                texts, pairs = post.extract_post_reply_pairs()
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
    # board = 'news'
    # board = 'his'
    # board = 'sci'
    board = 'x'
    # board = 'g'

    for ix in tqdm(range(100)):
        data = ChanData()
        data.load(i=ix, board=board)
        data.stat()
        data.write_post_replies()

