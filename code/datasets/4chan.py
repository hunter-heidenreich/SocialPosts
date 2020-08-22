import pdb
import json

from collections import defaultdict
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

    @staticmethod
    def stat_subsets(board):
        # maintain a default dict to store counts (maybe multiple)
        comments = defaultdict(int)
        tokens = defaultdict(int)

        # iterate through each subset of the data (00 - 99)
        for i in tqdm(range(100)):
            # construct chan stream
            board_path = f'{ChanStreamReader.ROOT}{board}/{i:02d}.json'
            chan = Board(uid=f'/{board}/', name=board)
            chan.load_from_json(board_path)

            # from 0.00 to 0.90 keep subsetting the data
            threshes = [i / 100 for i in range(0, 90, 5)]
            for thresh in threshes:
                path = f'out/4chan_{board}_{i:02d}_bf_ids_{thresh:0.2f}.json'
                ids = json.load(open(path))

                sub_board = Board(uid=f'/{board}/', name=board)
                for ix in ids:
                    sub_board.posts[int(ix)] = chan.posts[int(ix)]

                d, n = sub_board.comment_count()
                comments[thresh] += d + n

                tokens[thresh] += sub_board.token_count()
        # print total
        for thresh in [i / 100 for i in range(0, 90, 5)]:
            print(thresh)
            print('comments:', comments[thresh])
            print('tokens:', tokens[thresh])
            pdb.set_trace()


if __name__ == '__main__':
    parser = ArgumentParser('Dataset Analyzer for 4ChanStreams')
    parser.add_argument('-b', '--board', dest='board', type=str, default='news')
    args = parser.parse_args()

    # for generating thread documents
    # out = f'data/docs/{args.board}/'
    # assert_dir(out)
    # for i in range(100):
    #     print(f'Processing {i:02d}.json')
    #     chan = ChanStreamReader(args.board, file=f'{i:02d}.json')
    #     json.dump(chan.extract_discourse_documents(), open(f'{out}4chan_{args.board}_post_docs_{i:02d}.json', 'w+'))

    # compute stats of different subsets
    ChanStreamReader.stat_subsets(args.board)

    # chan.stat()
