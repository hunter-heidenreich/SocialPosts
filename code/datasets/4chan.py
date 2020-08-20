import pdb
import json

from glob import glob

from argparse import ArgumentParser
from tqdm import tqdm

import sys
sys.path.append('code/')

from chan.board import Board


class ChanStreamReader:
    ROOT = 'data/'

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
    parser = ArgumentParser('Dataset Analyzer for 4ChanStreams')
    parser.add_argument('-b', '--board', dest='board', type=str, default='news')
    args = parser.parse_args()

    chan = ChanStreamReader(args.board)
    json.dump(chan.extract_discourse_documents(), open(f'4chan_{args.board}_post_docs.json', 'w+'))

    # chan.stat_subsets()
    # chan.stat()
