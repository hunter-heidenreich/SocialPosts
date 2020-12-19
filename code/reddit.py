import os
import re
import json

from datetime import datetime
from glob import glob
from tqdm import tqdm

from post import UniversalPost
from board import Board
from dataset import ConversationalDataset
from utils import display_num


class RedditPost(UniversalPost):
    """
    Reddit post object
    """
    def _string_to_creation(self, x):
        self.created_at = datetime.fromtimestamp(float(x))

    def get_mentions(self):
        # Reddit user regex
        names = re.findall(r'/?u/([A-Za-z0-9_-]+)', self.text)

        return super(RedditPost, self).get_mentions() | set(names)

    @staticmethod
    def load_raw(data, board_id=None):

        post_cons = {
            'reply_to': set(),
            'platform': 'Reddit'
        }

        if board_id:
            post_cons['board_id'] = board_id

        ignore_keys = {
            'archived', 'body_html', 'id', 'link_id', 'gilded',
            'ups', 'downs', 'edited', 'controversiality', 'user_reports', 'mod_reports'
        }

        for key, value in data.items():
            if key in ignore_keys:
                continue

            if not value:
                continue

            if key == 'author_name':
                post_cons['author'] = value
            elif key == 'body':
                if 'text' in post_cons:
                    post_cons['text'] += ' ' + value
                else:
                    post_cons['text'] = value
            elif key == 'title':
                if 'text' in post_cons:
                    post_cons['text'] = value + ' ' + post_cons['text']
                else:
                    post_cons['text'] = value
            elif key == 'created':
                if 'created_at' not in post_cons:
                    post_cons['created_at'] = datetime.fromtimestamp(value)
            elif key == 'created_utc':
                if 'created_at' not in post_cons:
                    post_cons['created_at'] = datetime.fromtimestamp(value)
            elif key == 'name':
                post_cons['post_id'] = value
            elif key == 'parent_id':
                post_cons['reply_to'].add(value)
            else:
                print(f'Unrecognized key: {key} --> {value}')

                import pdb
                pdb.set_trace()

        return RedditPost(**post_cons)


class RedditCMV(ConversationalDataset):
    CACHE_PATH = 'Reddit/CMV'

    def load(self):
        board = RedditCMV.load_cmv_dump()
        self._boards[board.board_id] = board

    def cache(self):
        self.dump_conversation(filepath=RedditCMV.CACHE_PATH)

    def load_cache(self):
        self.load_conversation(filepath=RedditCMV.CACHE_PATH, board_cons=Board, post_cons=RedditPost)

    def stat(self, filepattern='*', label='conversational'):
        return super(RedditCMV, self).stat(RedditCMV.CACHE_PATH, Board, RedditPost, filepattern=filepattern, label=label)

    @staticmethod
    def load_cmv_dump(file_path='/Users/hsh28/PycharmProjects/ah-stahp/naacl2018-before-name-calling-habernal-et-al/data/cmv-full-2017-09-22/'):
        board = Board('changemyview')
        for f in tqdm(glob(file_path + '*.json')):
            with open(f) as fp:
                for line in fp.readlines():
                    x = RedditPost.load_raw(json.loads(line), board_id='changemyview')
                    board.add_post(x)

        return board


class RedditExtractor(ConversationalDataset):
    CACHE_PATH = 'Reddit/RD'

    INCLUDE_SUBS = {
        'reddit.com',

        # top 100 subreddits
        'announcements',
        'funny',
        'askreddit',
        'gaming',
        'awww',
        'pics',
        'music',
        'science',
        'worldnews',
        'videos',
        'todayilearned',
        'movies',
        'news',
        'showerthoughts',
        'earthporn',
        'gifs',
        'iama',
        'food',
        'askscience',
        'jokes',
        'explainlikeimfive',
        'lifeprotips',
        'art',
        'books',
        'mildlyinteresting',
        'diy',
        'nottheonion',
        'sports',
        'space',
        'gadgets',
        'documentaries',
        'getmotivated',
        'photoshopbattles',
        'television',
        'tifu',
        'upliftingnews',
        'internetisbeautiful',
        'philosophy',
        'history',
        'dataisbeautiful',
        'futurology',
        'writingprompts',
        'oldschoolcool',
        'nosleep',
        'personalfinance',
        'creepy',
        'memes',
        'twoxchromosomes',
        'technology',
        'adviceanimals',
        'wholesomememes',
        'fitness',
        'interestingasfuck',
        'politics',
        'wtf',
        'travel',
        'bestof',
        'blackpeopletwitter',
        'oddlysatisfying',
        'leagueoflegends',
        'facepalm',
        'me_irl',
        'lifehacks',
        'pcmasterrace',
        'relationship_advice',
        'natureisfuckinglit',
        'minecraft',
        'whatcouldgowrong',
        'dankmemes',
        'tinder',
        'bikinibottomtwitter',
        'trippingthroughtime',
        'ps4',
        'animalsbeingbros',
        'tattoos,'
        'nba',
        'photography',
        'animalsbeingjerks',
        'whoadude',
        'reactiongifs',
        'dadjokes',
        'foodporn',
        'overwatch',
        'unexpected',
        'pewdiepiesubmissions',
        'nextfuckinglevel',
        'gardening',
        'boardgames',
        'buildapc',
        'instant_regret',
        'watchpeopledieinside',
        'mildlyinfuriating',
        'contagiouslaughter',
        'pokemon',
        'relationships',
        'programming',
        'animalsbeingderps',
        'parenting',
        'pokemongo',
        'publicfreakout',

        # any interesting political?
        'libertarian',
        'anarchism',
        'socialism',
        'progressive',
        'conservative',
        'democrats',
        'liberal',
        'republican',
        'republicanism',
        'conservatism',
        'new_right',
        'demsocialist',
        'egalitarian',
        'democracy',
        'capitalism',
        'communist'
    }
    BLACKLIST_SUBS = {}

    TAG_COMMENT = 't1_'
    TAG_SUBMISSION = 't3_'
    fields_subm = [
        'id', 'author', 'created_utc', 'domain', 'permalink', 'score', 'selftext', 'subreddit', 'subreddit_id',
        'title', 'url'
    ]
    fields_comm = ["id", "author", 'created_utc', "parent_id", "link_id", "score", "body", 'subreddit']

    @staticmethod
    def preprocess_extract(file_path):
        """
        The RedditExtractor process creates a series of .tsv files,
        split by month.
        This is not amenable to our purposes, so we must first pre-process
        the .tsvs to generate board-specific JSON line files
        that can then be processed by board.
        """
        bids = set()
        boards = {}
        for f in glob(file_path + '*'):
            if 'stat' in f:
                os.remove(f)
                continue

            xs = f.split('/')[-1].split('.')
            file_name, file_ext = '.'.join(xs[:-1]), xs[-1]
            if file_ext == 'json':
                bids.add(file_name)
            elif file_ext == 'tsv':
                with open(f) as fp:
                    lines = fp.readlines()

                if 'rs' in file_name:
                    fields = RedditExtractor.fields_subm
                    tag = RedditExtractor.TAG_SUBMISSION
                else:
                    fields = RedditExtractor.fields_comm
                    tag = RedditExtractor.TAG_COMMENT

                for line in tqdm(lines):
                    post = {k: v for k, v in zip(fields, line.split('\t'))}
                    post['name'] = tag + post['id']

                    name = post['subreddit'].strip()
                    if name not in RedditExtractor.INCLUDE_SUBS:
                        continue

                    x = RedditPost.load_raw(post, board_id=name)
                    b = boards.get(name, Board(name))
                    b.add_post(x)
                    boards[name] = b

                # remove the file here, we've absorbed it
                os.remove(f)
            else:
                print(f'Unrecognized file extension: {file_ext}')
                import pdb
                pdb.set_trace()

        for bix, board in boards.items():
            # write as json lines of posts
            out = []
            for post in board.posts.values():
                out.append(json.dumps(post.to_json()) + '\n')

            with open(file_path + f'{bix}.json', 'w+') as fp:
                fp.writelines(out)

            bids.add(bix)

        return bids

    def dump_batch_by_date(self, filepath, date_str, thresh=180):
        os.makedirs(self.DATA_ROOT + 'conversations/' + filepath, exist_ok=True)
        for bid, board in self._boards.items():
            print(f'Building board: {bid}')
            board.construct_conversations(full_rebuild=False)

            print(f'Extracting conversations')
            convos = board.conversations
            print(f'Found {display_num(len(board.posts))} posts, {display_num(len(convos))} conversations')

            dt = datetime.strptime(date_str, '%Y-%m')

            batch = 0
            cur = 0
            lines = []
            for convo_id, posts in tqdm(convos.items()):
                # Do not write if not archived yet!
                if (dt - board.posts[convo_id].created_at).days < thresh:
                    continue

                lines.append(json.dumps({
                    'convo_id': convo_id,
                    'posts':    posts
                }) + '\n')

                # remove all posts that are about to be written to disk
                board.delete_conversation(convo_id)

                cur += len(posts)
                if cur > ConversationalDataset.CONVO_SIZE:
                    path = self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}{date_str}_{batch:04d}.json'
                    with open(path, 'w+') as fp:
                        fp.writelines(lines)
                    # print(f'Wrote batch {batch}: {display_num(cur)} posts, {display_num(len(lines))} conversations')
                    batch += 1
                    cur = 0
                    lines = []

            board.prune_singletons(dt, thresh)

            if lines:
                path = self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}{date_str}_{batch:04d}.json'
                with open(path, 'w+') as fp:
                    fp.writelines(lines)
                # print(f'Wrote batch {batch}: {display_num(cur)} posts, {display_num(len(lines))} conversations')

                print(f'Wrote {batch + 1} conversational chunks')
            else:
                print(f'Wrote {batch} conversational chunks')

    def load_batch(self):
        thresh = 180

        # assure correct format and gather board names
        board_names = set()

        # Pre-process all files
        months = glob(f'{self.DATA_ROOT}DialoGPTdata/*/')
        for month in tqdm(sorted(months)):
            print(month)
            names = RedditExtractor.preprocess_extract(month)
            board_names |= names

        print(f'Found {len(board_names)} boards')

        for bid in board_names:
            print(f'Loading board: {bid}')
            board = Board(bid)

            paths = sorted(glob(f'{self.DATA_ROOT}DialoGPTdata/*/{bid}.json'))

            for ix, f in enumerate(paths):
                print(f'{f} -- {ix+1}/{len(paths)}')
                date_str = f.split('/')[-2]
                with open(f) as fp:
                    for line in tqdm(fp.readlines()):
                        post = RedditPost(post_id=0)

                        try:
                            post.from_json(json.loads(line))
                        except json.JSONDecodeError:
                            continue

                        board.add_post(post)

                    self._boards[bid] = board

                    print(f'Posts in memory: {display_num(len(board.posts))}')

                    # Only dump in 6 month increments
                    dt = datetime.strptime(date_str, '%Y-%m')
                    if dt.month in {1, 4, 7, 10}:
                        print(f'Dumping posts older than {thresh} days!')
                        self.dump_batch_by_date(RedditExtractor.CACHE_PATH, date_str, thresh=thresh)

            self.dump_batch_by_date(RedditExtractor.CACHE_PATH, datetime.today().strftime('%Y-%m'), thresh=0)
            del self._boards[bid]

    def cache(self):
        self.dump_conversation(filepath=RedditExtractor.CACHE_PATH)

    def load_cache(self):
        self.load_conversation(filepath=RedditExtractor.CACHE_PATH, board_cons=Board, post_cons=RedditPost)


if __name__ == '__main__':
    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy as np

    dataset = RedditCMV()
    # dataset = RedditExtractor()

    # dataset.load_batch()

    # dataset.load()
    # dataset.cache()

    df = dataset.stat(label='conversational')

    df['log_posts'] = np.log10(df['posts'])

    sns.displot(data=df, x="log_posts")
    plt.show()

    import pdb
    pdb.set_trace()



