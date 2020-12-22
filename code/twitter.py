import re
import json

from datetime import datetime
from glob import glob
from tqdm import tqdm

from post import UniversalPost
from board import Board
from dataset import ConversationalDataset


class Tweet(UniversalPost):

    """
    Twitter post object with additional Twitter-specific features
    """

    def __init__(self, **args):
        super(Tweet, self).__init__(**args)

        if type(self.created_at) == str:
            self._string_to_creation(self.created_at)

    def _string_to_creation(self, timestr):
        self.created_at = Tweet.parse_twitter_datestr(timestr)

    def get_mentions(self):
        # twitter mention regex
        names = re.findall(r'@([^\s:]+)', self.text)

        return super(Tweet, self).get_mentions() | set(names)

    @staticmethod
    def parse_twitter_datestr(datestr):
        return datetime.strptime(datestr, '%a %b %d %H:%M:%S +0000 %Y')

    @staticmethod
    def load_raw(data):
        """
        Takes a raw tweet and returns
        :param data:
        :return:
        """

        cons_vals = {
            'platform': 'Twitter',
            'reply_to': set()
        }
        out = []

        ignore_keys = {
            'id_str', 'truncated', 'display_text_range', 'entities', 'source',
            'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str',
            'in_reply_to_screen_name', 'geo', 'coordinates', 'place', 'contributors',
            'is_quote_status', 'retweet_count', 'favorite_count', 'favorited',
            'retweeted', 'metadata', 'extended_entities', 'possibly_sensitive',
            'quoted_status_id_str', 'quoted_status_permalink', 'withheld_in_countries',
            'in_reply_to_status_created_at', 'possibly_sensitive_appealable', 'scopes',
            'withheld_scope', 'withheld_copyright'
        }
        for key, value in data.items():
            if key in ignore_keys:
                continue

            if not value:
                continue

            if key == 'created_at':
                cons_vals['created_at'] = Tweet.parse_twitter_datestr(value)
            elif key == 'id':
                cons_vals['post_id'] = value
            elif key == 'full_text':
                if 'text' not in cons_vals:
                    cons_vals['text'] = value
                else:
                    print(f'Text already present:\n\n{cons_vals}\n\n{key}\t{value}')
                    import pdb
                    pdb.set_trace()
            elif key == 'text':
                if 'text' not in cons_vals:
                    cons_vals['text'] = value
                else:
                    print(f'Text already present:\n\n{cons_vals}\n\n{key}\t{value}')
                    import pdb
                    pdb.set_trace()
            elif key == 'lang':
                cons_vals['lang'] = value
            elif key == 'in_reply_to_status_id':
                cons_vals['reply_to'].add(value)
            elif key == 'quoted_status_id':
                cons_vals['reply_to'].add(value)
            elif key == 'user':
                cons_vals['author'] = value['screen_name']
            elif key == 'quoted_status':
                out.extend(Tweet.load_raw(value))
            else:
                print(f'Unrecognized key: {key} --> {value}')
                import pdb
                pdb.set_trace()

        # Do entities last
        if 'entities' in data:
            ignore_keys = {
                'hashtags', 'symbols', 'user_mentions'
            }
            for key, value in data['entities'].items():
                if key in ignore_keys:
                    continue

                if key == 'media':
                    for v in value:
                        cons_vals['text'] = re.sub(v['url'], v['display_url'], cons_vals['text'])
                elif key == 'urls':
                    for v in value:
                        cons_vals['text'] = re.sub(v['url'], v['expanded_url'], cons_vals['text'])
                else:
                    print(f'Unrecognized key: {key} --> {value}')
                    import pdb
                    pdb.set_trace()

        if 'text' in cons_vals:
            out.append(Tweet(**cons_vals))

        return out


class Slush(ConversationalDataset):

    """
    A slush of all available Twitter data
    """

    CACHE_PATH = 'Twitter/slush'

    def __init__(self, include_news_tweet_threads=True, include_coordinated_targeting=True, full_slush=True):
        super(Slush, self).__init__()

        self._NTT = include_news_tweet_threads
        self._CTQ = include_coordinated_targeting
        self._slush = full_slush

    def load(self):
        if self._slush:
            self._boards['Twitter'] = Board('Twitter')

        if self._NTT:
            if not self._slush:
                self._boards['NTT'] = Board('NTT')
                bid = 'NTT'
            else:
                bid = 'Twitter'

            for f in tqdm(glob(f'{self.DATA_ROOT}threads/*tweets.json')):
                self._boards[bid].merge_board(NewstweetThreads.load_thread(f, board_id=bid))

        if self._CTQ:
            if not self._slush:
                self._boards['CTQ'] = Board('CTQ')
                bid = 'CTQ'
            else:
                bid = 'Twitter'

            paths = sorted(glob(f'{self.DATA_ROOT}quote_tweets/quotes/*.json'))
            for ix, f in enumerate(paths):
                print(f'{f} {ix+1}/{len(paths)}')
                self._boards[bid].merge_board(CoordinatedTargetingQuotes.load_quote_month(f, board_id=bid))

    def cache(self):
        self.dump_conversation(filepath=Slush.CACHE_PATH)

    def load_cache(self):
        self.load_conversation(filepath=Slush.CACHE_PATH, board_cons=Board, post_cons=Tweet)

    def stat(self, filepattern='*', label='conversational', stats=None, latex=False):
        return super(Slush, self).stat(Slush.CACHE_PATH, Board, Tweet, filepattern=filepattern,
                                       label=label, stats=stats, latex=latex)


class NewstweetThreads(ConversationalDataset):

    CACHE_PATH = 'Twitter/NTT'

    @staticmethod
    def load_thread(filepath, board_id=None):
        src = filepath.split('_')[-1].replace('-tweets.json', '')
        tweets = json.load(open(filepath))

        # extract user
        if not board_id:
            board_id = tweets[src]['user']['screen_name']

        board = Board(board_id)
        for tid, tweet in tweets.items():
            xs = Tweet.load_raw(tweet)
            for x in xs:
                board.add_post(x)

        return board

    def load(self):
        for f in tqdm(glob(f'{self.DATA_ROOT}threads/*tweets.json')):
            board = NewstweetThreads.load_thread(f)
            if board.board_id in self._boards:
                self._boards[board.board_id].merge_board(board)
            else:
                self._boards[board.board_id] = board

    def cache(self):
        self.dump_conversation(filepath=NewstweetThreads.CACHE_PATH)

    def load_cache(self):
        self.load_conversation(filepath=NewstweetThreads.CACHE_PATH, board_cons=Board, post_cons=Tweet)

    def stat(self, filepattern='*', label='conversational'):
        super(NewstweetThreads, self).stat(NewstweetThreads.CACHE_PATH, Board, Tweet,
                                           filepattern=filepattern, label=label)


class CoordinatedTargetingQuotes(ConversationalDataset):
    def load(self):
        self._boards['CTQ'] = Board('CTQ')
        for f in tqdm(glob(f'{self.DATA_ROOT}quote_tweets/quotes/*.json')):
            self._boards['CTQ'].merge_board(CoordinatedTargetingQuotes.load_quote_month(f))

        print(f'Loaded {len(self._boards)} user conversations')

    def cache(self):
        self.dump_conversation(filepath=f'Twitter/CTQ')

    def load_cache(self):
        self.load_conversation(filepath=f'Twitter/CTQ', board_cons=Board, post_cons=Tweet)

    @staticmethod
    def load_quote_month(filepath, board_id='CTQ'):
        board = Board(board_id)
        with open(filepath) as fp:
            for line in tqdm(fp.readlines()):
                data = json.loads(line)
                xs = Tweet.load_raw(data)
                for x in xs:
                    board.add_post(x)
        return board


if __name__ == '__main__':
    import matplotlib.pyplot as plt
    import numpy as np
    import seaborn as sns

    # data = NewstweetThreads()
    # data = CoordinatedTargetingQuotes()
    dataset = Slush(include_news_tweet_threads=True)

    # dataset.load()
    # dataset.cache()

    # data.load_cache()

    # df = dataset.stat(label='conversational', latex=True)
    # dataset.stat(label='token', latex=True)
    # dataset.stat(label='topological', latex=True)

    # dataset.round_robin_chunk(Slush.CACHE_PATH, 'data/batched/twitter/', Board, Tweet)

    # sns.displot(data=df, x="posts")
    # plt.show()

    # df = dataset.stat(label='tokenizer_roberta')

    # col = 'token_len'
    # col = 'log_token_len'

    # df['log_token_len'] = np.log10(df['token_len'])

    # bins = 250
    # df.hist(column=col, grid=False, bins=bins)
    # plt.show()

    # import pdb
    # pdb.set_trace()

    dataset.scan_tokenizer(dataset.CACHE_PATH)


