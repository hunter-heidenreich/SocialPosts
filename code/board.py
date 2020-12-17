import re
import os
import json

from datetime import datetime
from glob import glob
from tqdm import tqdm
from collections import defaultdict

from post import FBPost, RedditPost, ChanPost

import gcld3

# Language detection module with reliability flag
detector = gcld3.NNetLanguageIdentifier(min_num_bytes=0, max_num_bytes=1000)


class Board:

    """
    The general wrapper of a collection of posts.
    This class is titled `Board` but can generalize to:
    - An entire platform
    - A Reddit sub-reddit
    - A 4Chan board
    - A Facebook page / group (?)
    - A Twitter user timeline or list
    """

    RETAIN_LANGS = {'en', 'und'}

    def __init__(self, board_id):
        # unique board id (or page or originator)
        self._board_id = board_id

        # dictionary mapping from post_id to Post object
        self._posts = {}

        # Given a post_id, maps to its conversation id
        self._pid_to_convo_id = {}

        # Given a conversation id, maps to a set of associated post_ids
        self._convo_id_to_pids = defaultdict(set)

        # An object to cache the conversations when a board has been chunked
        self._conversations = {}

    @property
    def board_id(self):
        return self._board_id

    @property
    def posts(self):
        return self._posts

    def chunk_conversations(self, force_refresh=False, min_path_len=2):
        """
        Given a set of posts loaded into this board,
        this function chunks them into independent conversations.

        The minimum path length parameter is set to 2 by default
        to filter out singleton posts.
        If singletons are desired, change this to 1 or 0.
        """
        if self._conversations and not force_refresh:
            return

        self._conversations = {
            convo_id: [self.posts[pid].to_json() for pid in pids]
            for convo_id, pids in tqdm(self._convo_id_to_pids.items())
            if len(pids) >= min_path_len
        }

    @property
    def conversations(self):
        if not self._conversations:
            self.chunk_conversations()

        return self._conversations

    def load_conversations(self, data, post_cons):
        """
        Ingests raw conversation data,
        read from JSON files
        """
        self._conversations = {}
        for convo_id, posts in data.items():
            for post in posts:
                p = post_cons(post_id=post['post_id'])
                p.from_json(post)

                self.add_post(p, check=False)
                self._pid_to_convo_id[p.post_id] = convo_id
                self._convo_id_to_pids[convo_id].add(p.post_id)

    @staticmethod
    def filter_post(post):
        """
        Filters a post based on language detection
        of the text
        """
        res = detector.FindLanguage(text=post.text)

        if res.language not in Board.RETAIN_LANGS and res.is_reliable:
            return True

        post.lang = res.language if res.is_reliable else 'und'

        return False

    def add_post(self, post, check=True):
        """
        Adds a post to the board
        """
        # No language, check and set
        if check and not post.lang and self.filter_post(post):
            return
        elif check and post.lang and post.lang != 'en':
            return

        post.board = self._board_id
        self._posts[post.__hash__()] = post

    def remove_post(self, pid):
        """
        Removes a post from a board
        """
        if pid in self._posts:
            del self._posts[pid]
            self._conversations = None
        else:
            raise KeyError

    def generate_pairs(self):
        """
        Gemerates a list of structured
        post-reply pairs
        """
        pairs = []
        for _, post in self.posts.items():
            pairs.extend([{
                'post': self.posts[rid].text,
                'reply': post.text
            } for rid in post.reply_to if rid in self.posts])

        return pairs

    def redact(self):
        """
        Performs a conversationally-scoped redaction of user mentions
        """
        names_by_source = defaultdict(set)

        # gather name references
        print(f'Collecting user mentions / names')
        for post in tqdm(self._posts.values()):
            names_by_source[self._pid_to_convo_id[post.post_id]] |= post.get_mentions()

        # create maps
        name_map = {
            convo_id: {name: f'USER{ix}' for ix, name in enumerate(pids)} for convo_id, pids in names_by_source.items()
        }

        # redact with map
        print('REDACTION')
        for post in tqdm(self._posts.values()):
            mx = name_map[self._pid_to_convo_id[post.post_id]]
            post.redact(mx)

        return dict(name_map)

    def construct_conversations(self):
        """
        Reconstructs raw conversational trees
        """
        self._pid_to_convo_id = {}
        self._convo_id_to_pids = defaultdict(set)

        for pid, post in tqdm(self._posts.items()):
            if pid not in self._pid_to_convo_id:
                if post.platform == '4Chan':
                    # 4Chan data has weird cyclic issues at times where posts
                    # refer to themselves or to posts that have happened in the future
                    # (I found at least 2 examples of lists of prime numbers?)
                    post.reply_to = {rid for rid in post.reply_to if rid < pid}

                self.build_convo_path(post)

    def build_convo_path(self, post):
        """
        For a post, follows its conversational thread pointers
        to the highest post we have loaded into memory
        """
        breadth = len(post.reply_to)
        pid = post.post_id

        if breadth == 0:
            # set conversation to itself
            convo_id = pid
        elif breadth == 1:
            rid = list(post.reply_to)[0]
            if rid in self.posts:
                if rid not in self._pid_to_convo_id:
                    if post.platform == '4Chan':
                        post.reply_to = {rid for rid in post.reply_to if rid < pid}
                    # recurse
                    self.build_convo_path(self.posts[rid])
                convo_id = self._pid_to_convo_id[rid]
            else:
                # if we don't have a parent,
                # assign this post to a new thread
                convo_id = pid
        else:
            if post.platform == '4Chan':
                post.reply_to = {rid for rid in post.reply_to if rid < pid}
            else:
                for rid in post.reply_to:
                    if rid >= post.post_id:
                        print(f'Bad IDs:  {post.post_id} --> {rid}')
                        import pdb
                        pdb.set_trace()

            avail = [rid for rid in post.reply_to if rid in self.posts]
            convo_id = min(avail) if avail else pid

            if convo_id not in self._pid_to_convo_id and convo_id != pid:
                # recurse
                self.build_convo_path(self.posts[convo_id])

        self._pid_to_convo_id[pid] = convo_id
        self._convo_id_to_pids[convo_id].add(pid)

    def merge_board(self, board):
        """
        Merges two boards together
        """
        assert self.board_id == board.board_id

        # absorb posts
        try:
            if 0.8 < len(board.posts) / len(self.posts) < 1.2:
                self._posts = {**self._posts, **board.posts}
            elif len(board.posts) < len(self.posts):
                for post in board.posts.values():
                    self._posts[post.post_id] = post
            else:
                for post in self.posts.values():
                    board.posts[post.post_id] = post
                self._posts = board.posts
        except ZeroDivisionError:
            self._posts = {**self._posts, **board.posts}

        # Reset other pointers
        self._pid_to_convo_id = {}
        self._convo_id_to_pids = defaultdict(set)
        self._conversations = {}


class FBPage(Board):

    @staticmethod
    def load_page(page_path):
        pagename = page_path.split('/')[-1].replace('_', ' ').replace('%', '\\%')
        page = FBPage(pagename)

        for post_path in glob(f'{page_path}/*'):
            pid = int(post_path.split('/')[-1])

            post = None
            comments = None
            scraped_comments = None
            replies = None
            disqus = None

            for f in glob(f'{post_path}/*.json'):
                if 'post' in f:
                    post = json.load(open(f))
                elif 'comments' in f:
                    try:
                        comments = json.load(open(f))
                    except json.JSONDecodeError:
                        # File is corrupted, skip
                        pass
                elif 'attach' in f:
                    pass
                elif 'react' in f:
                    pass
                elif 'replies' in f:
                    try:
                        replies = json.load(open(f))
                    except json.JSONDecodeError:
                        # File is corrupt, skip
                        pass
                elif 'scrape' in f:
                    scrape = json.load(open(f))
                    if 'tweets' in scrape and scrape['tweets']:
                        # print(f'Found tweets: {scrape["tweets"]}')
                        # this could be useful later if not used already
                        pass

                    if 'comments' in scrape and scrape['comments']:
                        scraped_comments = scrape['comments']

                    if 'DisqComm' in scrape and scrape['DisqComm']:
                        disqus = scrape['DisqComm']

                    found = set(scrape.keys())
                    remove = {'body', 'links', 'pictures',
                              'tweets', 'comments', 'DisqComm'}
                    found -= remove
                    if found:
                        print('new scrape keys:')
                        print(scrape)
                        import pdb
                        pdb.set_trace()
                else:
                    print(f)
                    import pdb
                    pdb.set_trace()

            if post:
                keys = set(post.keys())
                page.add_post(FBPost(**{
                    'post_id': post['id'],
                    'text': FBPost.find_text(post),
                    'created_at': post['created_time'],
                    'board_id': pagename,
                    'platform': 'Facebook',
                    'root_id': post['id']
                }))

                ignore_keys = {
                    'updated_time', 'shares',
                    'picture', 'link', 'first_party',
                    'type', 'source',
                    'place'
                }

                keys.remove('id')
                keys.remove('created_time')
                keys -= set(FBPost.TEXT)
                keys -= ignore_keys

                if keys:
                    print('New post keys:')
                    print(keys)
                    import pdb
                    pdb.set_trace()

            if comments:
                if type(comments) == dict:
                    comments = comments['data']

                for comm in comments:
                    page.add_post(FBPost(**{
                        'post_id':    comm['id'],
                        'text':       comm['message'],
                        'created_at': comm['created_time'],
                        'author':     comm['userID'] if 'userID' in comm else None,
                        'board_id':   pagename,
                        'platform':   'Facebook',
                        'reply_to':   [post['id'] if post else pid],
                        'root_id':    post['id'] if post else pid
                    }))

                    keys = set(comm.keys())

                    ignore_keys = {'from', 'response'}
                    used_keys = {'userID', 'id', 'message', 'created_time'}
                    keys -= ignore_keys
                    keys -= used_keys

                    if keys:
                        print('New comment keys:')
                        print(keys)
                        import pdb
                        pdb.set_trace()

            if replies:
                for rep in replies:
                    page.add_post(FBPost(**{
                        'post_id':    rep['id'],
                        'text':       rep['message'],
                        'created_at': rep['created_time'],
                        'board_id':   pagename,
                        'platform':   'Facebook',
                        'author':     rep['userID'] if 'userID' in rep else None,
                        'reply_to':   [post['id'] if post else pid],
                        'root_id':    post['id'] if post else pid
                    }))
                    for nest in rep['replies']:
                        try:
                            page.add_post(FBPost(**{
                                'post_id':    nest['id'],
                                'text':       nest['message'],
                                'created_at': nest['created_time'],
                                'board_id':   pagename,
                                'author':     nest['userID'] if 'userID' in nest else None,
                                'platform':   'Facebook',
                                'reply_to':   [rep['id']],
                                'root_id':    post['id'] if post else pid
                            }))
                        except TypeError:
                            print(nest)
                            import pdb
                            pdb.set_trace()

                    keys = set(rep.keys())

                    ignore_keys = {'from', 'userID'}
                    keys -= ignore_keys
                    keys.remove('id')
                    keys.remove('message')
                    keys.remove('created_time')
                    keys.remove('replies')

                    if keys:
                        print('New comment keys:')
                        print(keys)
                        import pdb
                        pdb.set_trace()

            if scraped_comments:
                for comm in scraped_comments:
                    page.add_post(FBPost(**{
                        'post_id':    comm['id'],
                        'text':       comm['message'],
                        'created_at': post['created_time'],
                        'board_id':   pagename,
                        'platform':   'Facebook',
                        'reply_to':   [post['id'] if post else pid],
                        'root_id':    post['id'] if post else pid
                    }))

                    keys = set(comm.keys())

                    ignore_keys = {'from'}
                    keys -= ignore_keys
                    keys.remove('id')
                    keys.remove('message')
                    keys.remove('created_time')

                    if keys:
                        print('New scraped comment keys:')
                        print(keys)
                        import pdb
                        pdb.set_trace()

            if disqus:
                for comm in disqus:
                    auth = None
                    if 'author' in comm:
                        if 'username' in comm['author']:
                            auth = comm['author']['username']
                        elif 'name' in comm['author']:
                            auth = comm['author']['name']

                    page.add_post(FBPost(**{
                        'post_id':    int(comm['id']),
                        'author':     auth,
                        'created_at': datetime.strptime(comm['createdAt'], '%Y-%m-%dT%H:%M:%S'),
                        'text':       comm['message'],
                        'board_id':   pagename,
                        'platform':   'Facebook',
                        'reply_to':   [int(comm['parent'])] if 'parent' in comm and comm['parent'] else [
                            post['id'] if post else pid],
                        'root_id':    post['id'] if post else pid
                    }))

                    keys = set(comm.keys())

                    ignore_keys = {
                        'canVote', 'dislikes',
                        'forum',  # might be able to break this out into another "platform"
                        'isApproved', 'isDeleted', 'isDeletedByAuthor',
                        'isEdited', 'isFlagged', 'isHighlighted',
                        'isSpam', 'likes', 'media', 'moderationLabels',
                        'numReports', 'points', 'raw_message',
                        'sb', 'thread'
                    }
                    used_keys = {'id', 'author', 'createdAt', 'parent', 'message'}
                    keys -= ignore_keys
                    keys -= used_keys

                    if keys:
                        print('New disqus comment keys:')
                        print(keys)
                        import pdb
                        pdb.set_trace()

        return page


class SubReddit(Board):

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
    def load_cmv(file_path='/Users/hsh28/PycharmProjects/ah-stahp/naacl2018-before-name-calling-habernal-et-al/data/cmv-full-2017-09-22/'):
        board = SubReddit('CMV')
        for f in tqdm(glob(file_path + '*.json')):
            with open(f) as fp:
                for line in fp.readlines():
                    post = json.loads(line)

                    ps = RedditPost(**{
                        'post_id':    post.get('name'),
                        'created_at': datetime.fromtimestamp(post.get('created')),
                        'text':       post.get('title') + ' ' + post.get('body'),  # + ' ' + post.get(''),
                        'author':     post.get('author_name'),
                        'board_id':   'CMV',
                        'platform':   'Reddit',
                        'reply_to':   [post.get('parent_id')] if post.get('parent_id') else None
                    })

                    if board.add_post(ps):
                        # board.roots.add(src_id)
                        pass

        return board

    @staticmethod
    def preprocess_extract(file_path):
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
                    fields = SubReddit.fields_subm
                    tag = SubReddit.TAG_SUBMISSION
                    reply_to = False
                else:
                    fields = SubReddit.fields_comm
                    tag = SubReddit.TAG_COMMENT
                    reply_to = True

                for line in tqdm(lines):
                    post = {k: v for k, v in zip(fields, line.split('\t'))}
                    try:
                        name = post['subreddit'].strip()

                        if name not in SubReddit.INCLUDE_SUBS:
                            continue

                        if tag == SubReddit.TAG_SUBMISSION:
                            text = post['title'] + ' ' + post['selftext'] + ' ' + post['url']
                        else:
                            text = post['body']

                        b = boards.get(name, SubReddit(name))
                        b.add_post(RedditPost(**{
                            'post_id':    tag + post['id'],
                            'text':       text,
                            'created_at': datetime.fromtimestamp(float(post['created_utc'])),
                            'board_id':   name,
                            'author':     post['author'],
                            'root_id':    tag + post['id'],
                            'platform':   'Reddit',
                            'reply_to':   {post['parent_id']} if reply_to else None
                        }))
                        boards[name] = b
                    except KeyError:
                        # print(f'Error: Ill-formed sub {sub}')
                        continue
                    except ValueError:
                        continue

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

    @staticmethod
    def load_extract(file_path):
        boards = {}
        with open(file_path) as fp:
            for line in tqdm(fp.readlines()):
                header, post_text, rep_text = line.split('\t')

                ps = json.loads(post_text)
                rs = json.loads(rep_text)
                tid, pid, cid = header.split(',')

                subreddit = ps['originator'].split()[0].lower()
                board = boards.get(subreddit, SubReddit(subreddit))
                if subreddit in SubReddit.INCLUDE_SUBS:
                    if pid not in board._posts:
                        board.add_post(RedditPost(**{
                            'post_id': pid,
                            'text': ps['text'],
                            'author': ps['user'],
                            'platform': 'Reddit',
                            'board_id': subreddit,
                            'root_id': tid
                        }))

                    if cid not in board._posts:
                        board.add_post(RedditPost(**{
                            'post_id':  cid,
                            'text':     rs['text'],
                            'author':   rs['user'],
                            'platform': 'Reddit',
                            'board_id':  subreddit,
                            'reply_to': [pid],
                            'root_id':  tid
                        }))
                    boards[subreddit] = board
                elif subreddit in SubReddit.BLACKLIST_SUBS:
                    pass
                else:
                    # print(subreddit)

                    # import pdb
                    # pdb.set_trace()
                    pass

        return boards

    @staticmethod
    def load_extract_month(filepath):
        boards = {}
        for subf in glob(filepath + 'rs_*.tsv'):
            with open(subf) as fp:
                lines = fp.readlines()

            for line in tqdm(lines):
                sub = {k: v for k, v in zip(fields_subm, line.split('\t'))}
                try:
                    name = sub['subreddit'].strip()

                    if name not in SubReddit.INCLUDE_SUBS:
                        continue

                    b = boards.get(name, SubReddit(name))
                    b.add_post(RedditPost(**{
                        'post_id':    TAG_SUBMISSION + sub['id'],
                        'text':       sub['title'] + ' ' + sub['selftext'] + ' ' + sub['url'],
                        'created_at': datetime.fromtimestamp(float(sub['created_utc'])),
                        'board_id':   name,
                        'author':     sub['author'],
                        'root_id':    TAG_SUBMISSION + sub['id'],
                        'platform':   'Reddit'
                    }))
                    boards[name] = b
                except KeyError:
                    # print(f'Error: Ill-formed sub {sub}')
                    continue
                except ValueError:
                    continue

        for subf in glob(filepath + 'rc_*.tsv'):
            with open(subf) as fp:
                lines = fp.readlines()

            for line in tqdm(lines):
                com = {k: v for k, v in zip(fields_comm, line.split('\t'))}

                name = com['subreddit'].strip()

                if name not in SubReddit.INCLUDE_SUBS:
                    continue

                b = boards.get(name, SubReddit(name))
                b.add_post(RedditPost(**{
                    'post_id':    TAG_COMMENT + com['id'],
                    'text':       com['body'],
                    'author':     com['author'],
                    'created_at': datetime.fromtimestamp(float(com['created_utc'])),
                    'board_id':   name,
                    'platform':   'Reddit',
                    'reply_to':   {com['parent_id']}
                }))
                boards[name] = b

        return boards


class ChanBoard(Board):

    @staticmethod
    def load_chunk(board_name, chunk, data_root='/Users/hsh28/data'):
        board = ChanBoard(board_name)

        posts = json.load(open(f'{data_root}/4chan/{board_name}/{chunk:02d}.json'))
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
