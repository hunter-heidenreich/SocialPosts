import re
import html
import json

from datetime import datetime
from tqdm import tqdm

from post import UniversalPost
from board import Board
from dataset import ConversationalDataset


class ChanPost(UniversalPost):
    def _string_to_creation(self, x):
        pass

    @staticmethod
    def exclude_replies(comment):
        """
        Function to remove quotes from a reply
        and return reference to the posts that
        were replied to
        """
        refs = re.findall('>>(\d+)', comment)

        lines = comment.split("\n")
        lines = filter(lambda x: not bool(re.match(">>(\d+)", x.strip())), lines)
        comment = "\n".join(lines)
        comment = re.sub(">>(\d+) ", " ", comment)

        return comment, refs

    @staticmethod
    def clean_text(comment):
        """
        Cleans the raw HTML of a cached 4chan post,
        returning both the references and teh comment itself
        """
        comment = html.unescape(comment)
        comment = re.sub("<w?br/?>", "\n", comment)
        comment = re.sub("<a href=\".+\" class=\"(\w+)\">", " ", comment)
        comment = re.sub("</a>", " ", comment)
        comment = re.sub("<span class=\"(\w+)\">", " ", comment)
        comment = re.sub("</span>", " ", comment)
        comment = re.sub("<pre class=\"(\w+)\">", " ", comment)
        comment = re.sub("</pre>", " ", comment)

        comment, rfs = ChanPost.exclude_replies(comment)

        comment = re.sub("[^\x00-\x7F]", " ", comment)

        comment = re.sub("&(amp|lt|gt|ge|le)(;|)", " ", comment)

        comment = re.sub("\\s\\s+", " ", comment)
        comment = re.sub("\n", " ", comment)
        comment = str(comment).strip()

        return comment, rfs


class Chan(ConversationalDataset):

    # BOARD = 'news'
    # BOARD = 'sci'
    # BOARD = 'his'
    # BOARD = 'x'
    # BOARD = 'g'
    BOARD = 'pol'

    CACHE_PATH = '4chan'

    def load(self):
        super(Chan, self).load()

        board = Board(self.BOARD)
        for i in tqdm(range(100)):
            board.merge_board(Chan.load_chunk(Chan.BOARD, i))
        self._boards[self.BOARD] = board

    def load_batch(self):
        for i in tqdm(range(100)):
            self._boards[Chan.BOARD] = Chan.load_chunk(Chan.BOARD, i)
            self.dump_conversation(filepath=Chan.CACHE_PATH, board_suffix=i)

    def cache(self):
        self.dump_conversation(filepath=Chan.CACHE_PATH)

    def load_cache(self):
        self.load_conversation(filepath=Chan.CACHE_PATH, board_cons=Board, post_cons=ChanPost,
                               filepattern=f'{self.BOARD}_*')

    def stat(self, filepattern='*', label='conversational'):
        return super(Chan, self).stat(Chan.CACHE_PATH, Board, ChanPost, filepattern=filepattern, label=label)

    @staticmethod
    def load_chunk(board_name, chunk):
        board = Board(board_name)

        posts = json.load(open(f'{ConversationalDataset.DATA_ROOT}/4chan/{board_name}/{chunk:02d}.json'))
        for k, post in posts.items():
            if 'com' not in post:
                continue

            txt, rfs = ChanPost.clean_text(post['com'])
            reps = {int(post['resto'])} | set([int(x) for x in rfs])

            if 0 in reps:
                reps.remove(0)

            if int(post['no']) in reps:
                reps.remove(int(post['no']))

            p = ChanPost(**{
                'post_id':    int(post['no']),
                'created_at': datetime.fromtimestamp(post['time']),
                'text':       txt,
                'author':     post['name'] if 'name' in post else None,
                'board_id':   board_name,
                'platform':   '4Chan',
                'reply_to':   reps
            })

            board.add_post(p)

        return board


if __name__ == '__main__':
    import numpy as np
    import matplotlib.pyplot as plt

    dataset = Chan()
    # dataset.stat(label='topological')

    # df = dataset.stat(label='tokenizer_roberta')

    # col = 'token_len'
    # col = 'log_token_len'

    # df['log_token_len'] = np.log10(df['token_len'])

    # bins = 250
    # df.hist(column=col, grid=False, bins=bins)
    # plt.show()

    dataset.scan_tokenizer('4chan')

