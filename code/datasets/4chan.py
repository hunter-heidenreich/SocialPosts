import pdb
import json

from glob import glob

from tqdm import tqdm

import sys
sys.path.append('code/')

from chan.board import Board
from chan.post import ChanPost


class ChanStreamReader:
    ROOT = '/Users/hsh28/PycharmProjects/SocialPosts/data/'

    def __init__(self, board):
        board_path = f'{self.ROOT}{board}.json'

        self.board = Board(uid=f'/{board}/', name=board)
        self.board.load_from_json(board_path)

    def extract_discourse_documents(self):
        return {post_id: post.preprocess_thread() for post_id, post in tqdm(self.board.get_posts().items())}

    def stat(self):
        self.board.stat()

    def stat_subsets(self):
        for f in glob('4chan_bf_ids*.json'):
            ids = json.load(open(f))

            print(f)
            sub_board = Board(uid=f'/{board}/', name=board)
            for ix in ids:
                sub_board.add_post(self.board.get_post(int(ix)))

            sub_board.stat()
            pdb.set_trace()


if __name__ == '__main__':
    board = 'his'
    chan = ChanStreamReader(board)
    chan.stat_subsets()

    # chan.stat()
    # json.dump(chan.extract_discourse_documents(), open(f'4chan_{board}_post_docs.json', 'w+'))
