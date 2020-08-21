import pdb
import json

from pathlib import Path
from glob import glob

from argparse import ArgumentParser
from tqdm import tqdm

import sys
sys.path.append('code/')

from chan.board import Board


def assert_dir(dir_path):
    """
    create directory if it does not exist
    """
    dir_path = Path(dir_path)
    if not dir_path.exists():
        dir_path.mkdir(parents=True)


class ChanStreamReader:
    ROOT = 'data/stream/'

    def __init__(self, board, file='00.json'):
        board_path = f'{self.ROOT}{board}/{file}'

        self.board = Board(uid=f'/{board}/', name=board)
        self.board.load_from_json(board_path)

    def extract_discourse_documents(self):
        return {post_id: post.preprocess_thread() for post_id, post in tqdm(self.board.posts.items())}

    def stat(self):
        self.board.stat()

    def stat_subsets(self, board):
        for f in glob('4chan_bf_ids*.json'):
            ids = json.load(open(f))

            print(f)
            sub_board = Board(uid=f'/{board}/', name=board)
            for ix in ids:
                sub_board.posts[int(ix)] = self.board.posts[int(ix)]

            sub_board.stat()
            pdb.set_trace()


if __name__ == '__main__':
    parser = ArgumentParser('Dataset Analyzer for 4ChanStreams')
    parser.add_argument('-b', '--board', dest='board', type=str, default='news')
    args = parser.parse_args()

    out = 'data/docs/'
    assert_dir(out)
    for i in range(100):
        chan = ChanStreamReader(args.board, file=f'{i:02d}.json')
        json.dump(chan.extract_discourse_documents(), open(f'{out}4chan_{args.board}_post_docs_{i:02d}.json', 'w+'))

    # chan.stat_subsets(args.board)
    # chan.stat()
