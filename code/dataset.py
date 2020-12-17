import os
import re
import json
import random

import pandas as pd
import numpy as np
import networkx as nx

from datetime import datetime
from tqdm import tqdm
from glob import glob


from post import RedditPost, FBPost, ChanPost
from board import FBPage, SubReddit, ChanBoard
from utils import display_num


class ConversationalDataset:

    """
    An interface for a conversational, social media dataset
    """

    DATA_ROOT = '/Users/hsh28/data/'
    # DATA_ROOT = '/local-data/socialtransformer/'

    # # of posts in a conversation chunk
    CONVO_SIZE = 1e6

    def __init__(self):
        self._boards = {}

    def dump_conversation(self, filepath, board_suffix=''):
        """
        Given a chunk of boards, this function iterates through the boards,
        builds their conversations,
        and writes smaller, more manageable conversational chunks
        """
        for bid, board in self._boards.items():
            os.makedirs(self.DATA_ROOT + 'conversations/' + filepath, exist_ok=True)

            print(f'Building board: {bid}')
            board.construct_conversations()

            print(f'Extracting conversations')
            convos = board.conversations
            print(f'Found {display_num(len(board.posts))} posts, {display_num(len(convos))} conversations')

            batch = 0
            cur = 0
            lines = []
            for convo_id, posts in tqdm(convos.items()):
                lines.append(json.dumps({
                    'convo_id': convo_id,
                    'posts':    posts
                }) + '\n')

                cur += len(posts)
                if cur > ConversationalDataset.CONVO_SIZE:
                    path = self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}_{board_suffix:04d}.json' \
                        if type(board_suffix) == int \
                        else self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}{board_suffix}_{batch:04d}.json'
                    with open(path, 'w+') as fp:
                        fp.writelines(lines)
                    # print(f'Wrote batch {batch}: {display_num(cur)} posts, {display_num(len(lines))} conversations')
                    batch += 1
                    cur = 0
                    lines = []

            if lines:
                path = self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}_{board_suffix:04d}.json' \
                    if type(board_suffix) == int \
                    else self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}{board_suffix}_{batch:04d}.json'
                with open(path, 'w+') as fp:
                    fp.writelines(lines)
                # print(f'Wrote batch {batch}: {display_num(cur)} posts, {display_num(len(lines))} conversations')

            print(f'Wrote {batch+1} conversational chunks')

    def load_conversation(self, filepath, board_cons, post_cons, filepattern='*',):
        """
        Loads a chunk of cached conversational objects
        """
        for f in glob(self.DATA_ROOT + 'conversations/' + filepath + f'/{filepattern}.json'):
            bid = f.split('/')[-1].split('_')[0]
            board = self._boards.get(bid, board_cons(bid))

            convs = {}
            with open(f) as fp:
                for line in fp.readlines():
                    cx = json.loads(line)
                    convs[cx['convo_id']] = cx['posts']
            board.load_conversations(convs, post_cons)

            self._boards[bid] = board

        for bid, board in self._boards.items():
            print(f'Loaded board: {bid} ({display_num(len(board.posts))} posts, {display_num(len(board.conversations))} conversations)')

    def conversation_iterator(self, filepath, board_cons, post_cons, filepattern='*'):
        """
        Produces an iterator that will iterate over
        cached conversational data,
        at a lower memory foot print than loading
        all data into memory directly
        """
        search_path = self.DATA_ROOT + 'conversations/' + filepath + f'/{filepattern}.json'
        print(search_path)

        paths = sorted(glob(search_path))
        for ix, f in enumerate(paths):
            print(f'{f} {ix+1}/{len(paths)}')

            bid = '_'.join(f.split('/')[-1].split('_')[:-1])
            board = board_cons(bid)

            convs = {}
            with open(f) as fp:
                for line in fp.readlines():
                    cx = json.loads(line)
                    convs[cx['convo_id']] = cx['posts']
            board.load_conversations(convs, post_cons)

            yield bid, board

    @staticmethod
    def _stat_conversational(conv):
        """
        Returns the conversational stats
        for a conversation chunk
        """
        pids = {post['post_id'] for post in conv}
        return {
            'sources':       1 if any([len(post['reply_to']) == 0 for post in conv]) else 0,
            'conversations': 1,
            'posts':         len(conv),
            'pairs':         len([1 for post in conv if len([1 for rid in post['reply_to'] if rid in pids]) > 0]),
            'voices':        '\t\t\t'.join({post['author'] for post in conv if post['author']})
        }

    @staticmethod
    def _stat_tokens(conv):
        """
        Produces stats about the space-separated tokens
        of a conversation chunk
        """
        tokens = re.split('\s+', ' '.join([post['text'] for post in conv]))
        normal = set(tokens)
        lower = {n.lower() for n in normal}
        return {
            'unique':       '\t\t\t'.join(normal),
            'unique_lower': '\t\t\t'.join(lower),
            'tokens':       len(tokens)
        }

    @staticmethod
    def _stat_topo(conv):
        """
        Produces topological stats about a conversation
        chunk based on treating it like a
        directed acyclic graph (DAG)
        """
        pids = [post['post_id'] for post in conv]

        # create DAG
        graph = nx.DiGraph()
        graph.add_nodes_from(pids)

        # add edges
        for pid, post in zip(pids, conv):
            for rid in post['reply_to']:
                if post['platform'] == '4Chan' and rid >= pid:
                    continue

                if rid in graph.nodes:
                    graph.add_edge(pid, rid)

        return {
            'nodes':        graph.number_of_nodes(),
            'density':      nx.density(graph),
            'longest_path': nx.algorithms.dag_longest_path_length(graph),
            'degrees':      np.average([graph.in_degree[n] + graph.out_degree[n] for n in graph.nodes]),
            # 'in_degrees':   np.average([graph.in_degree[n] for n in graph.nodes]),
            # 'out_degrees':  np.average([graph.out_degree[n] for n in graph.nodes])
        }

    @staticmethod
    def _stat_conversation(conv, label='conversational'):
        """
        High-level interface for extracting stats and insights
        about conversational chunks
        """
        if label == 'conversational':
            return ConversationalDataset._stat_conversational(conv)
        elif label == 'token':
            return ConversationalDataset._stat_tokens(conv)
        elif label == 'topological':
            return ConversationalDataset._stat_topo(conv)
        else:
            raise ValueError(f'Unrecognized label value: {label}')

    def stat(self, filepath, board_cons, post_cons, filepattern='*', label='conversational'):
        """
        Given a file pattern, this function will compute
        stats about the cached conversations found
        in a manner that is more memory efficient
        than loading all chached conversations
        into memory directly.
        This function will print a latex table, by default.
        """
        df = None
        for bid, board_chunk in self.conversation_iterator(filepath, board_cons, post_cons, filepattern):
            chunk_stats = []
            for conv in board_chunk.conversations.values():
                s = self._stat_conversation(conv, label)
                s['board_id'] = bid
                chunk_stats.append(s)

            if df is not None:
                df = df.append(chunk_stats, ignore_index=True)
            else:
                df = pd.DataFrame(chunk_stats)

        if df is not None:
            self._latex_table(df, label)

    @staticmethod
    def _latex_table(df, label):
        """
        Method for printing out a latex table with
        the appropriate stats
        (including filtering of data
        that makes up less than 1% of the stats)
        """
        table = []
        desc_map = {
            'mean': 'Avg.',
            'std':  'Std. Dev.',
            'min':  'Min.',
            '25%':  '25%',
            '50%':  '50%',
            '75%':  '75%',
            'max':  'Max.'
        }
        if label == 'conversational':
            ft_map = {
                'sources': 'Sources',
                'conversations': 'Conversations',
                'posts': 'Posts',
                'pairs': 'Pairs',
                'vox_cnt': 'Voices',
            }

            total_vox = set()
            vox_cnts = []
            for vs in df.voices.values:
                vox = set()
                for v in vs.split('\t\t\t'):
                    vox.add(v)
                vox_cnts.append(len(vox))
                total_vox |= vox

            df['vox_cnt'] = vox_cnts

            desc = df.describe()
            for row in desc_map:
                out = f'{desc_map[row]}'
                for col in ft_map:
                    out += f' & {display_num(desc[col][row])}'
                out += ' \\\\'

                table.append(out)

            table.append('\\hline')

            out = 'Total & '
            out += display_num(df.sources.sum()) + ' & '
            out += display_num(df.conversations.sum()) + ' & '
            out += display_num(df.posts.sum()) + ' & '
            out += display_num(df.pairs.sum()) + ' & '
            out += display_num(len(total_vox)) + ' \\\\ '

            table.append(out)
        elif label == 'token':
            ft_map = {
                'tokens':       'Tokens',
                'unique':       'Unique',
                'lower': 'Unique Lower',
            }

            total_unique = set()
            total_lower = set()

            u_cnts = []
            for vs in df.unique.values:
                unique = set()
                for v in vs.split('\t\t\t'):
                    unique.add(v)

                u_cnts.append(len(unique))
                total_unique |= unique

            l_cnts = []
            for vs in df.unique_lower.values:
                lower = set()
                for v in vs.split('\t\t\t'):
                    lower.add(v)

                l_cnts.append(len(lower))
                total_lower |= lower

            df['unique'] = u_cnts
            df['lower'] = l_cnts

            desc = df.describe()
            for row in desc_map:
                out = f'{desc_map[row]}'
                for col in ft_map:
                    out += f' & {display_num(desc[col][row])}'
                out += ' \\\\'

                table.append(out)

            table.append('\\hline')

            out = 'Total & '
            out += display_num(df.tokens.sum()) + ' & '
            out += display_num(len(total_unique)) + ' & '
            out += display_num(len(total_lower)) + ' \\\\ '

            table.append(out)
        elif label == 'topological':
            ft_map = {
                # 'nodes':
                'density': 'Density',
                'longest_path': 'Longest Path',
                'degrees': 'Degree',
            }

            desc = df.describe()
            for row in desc_map:
                out = f'{desc_map[row]}'
                for col in ft_map:
                    out += f' & {display_num(desc[col][row])}'
                out += ' \\\\'

                table.append(out)
        else:
            print(label)
            raise ValueError

        print('\n'.join(table))
        print()

    def redact(self, root):
        """
        Function for redacting a dataset
        in a conversationally scoped manner,
        but with caching of the true data

        TODO: This function needs to be updated to work with an iterator
        """
        with open(f'{self.DATA_ROOT}conversations/{root}.json', 'w+') as fp:
            for board in tqdm(self._boards):
                ns = self._boards[board].redact()
                line = f'{board}\t{json.dumps(ns)}\n'
                fp.write(line)

    def batch_chunk(self, pattern, outpath, board_cons, post_cons,
                    seed=42, batch_size=2048 * 2048, dev_ratio=0.01):
        """
        Given a pattern, will generate structured
        post-reply text pairs in a batched file format,
        with a default ratio of 1% dev holdout
        """
        np.random.seed(seed)
        random.seed = seed

        tr_batch = 0
        tr_cur = []
        dv_cur = []

        for bid, board in self.conversation_iterator(pattern, board_cons, post_cons):
            print(f'Batching: {bid}')
            pairs = board.generate_pairs()
            dev_size = int(np.ceil(dev_ratio * len(pairs)))
            print(f'Found {len(pairs)}, holding {dev_size} for dev!')

            random.shuffle(pairs)
            train_ps, dev_ps = pairs[dev_size:], pairs[:dev_size]

            for pair in tqdm(train_ps):
                post = pair['post']
                reply = pair['reply']
                if len(post) > 0 or len(reply) > 0:
                    tr_cur.append(json.dumps({
                        'post':  post,
                        'reply': reply
                    }) + '\n')

                if len(tr_cur) == batch_size:
                    with open(f'{outpath}train_{tr_batch}.json', 'w+') as fp:
                        fp.writelines(tr_cur)

                    tr_cur = []
                    tr_batch += 1

            for pair in tqdm(dev_ps):
                post = pair['post']
                reply = pair['reply']
                if len(post) > 0 or len(reply) > 0:
                    dv_cur.append(json.dumps({
                        'post':  post,
                        'reply': reply
                    }) + '\n')

                if len(dv_cur) == batch_size:
                    with open(f'{outpath}dev.json', 'a+') as fp:
                        fp.writelines(dv_cur)

                    dv_cur = []

        if tr_cur:
            with open(f'{outpath}train_{tr_batch}.json', 'a+') as fp:
                fp.writelines(tr_cur)

        if dv_cur:
            with open(f'{outpath}dev.json', 'a+') as fp:
                fp.writelines(dv_cur)


class BuzzFace(ConversationalDataset):
    def load(self):
        super(BuzzFace, self).load()

        for suffix in ['', '2', '3', 'full']:
            for f in tqdm(glob(f'{self.DATA_ROOT}BuzzFace/data{suffix}/*/')):
                page = FBPage.load_page(f[:-1])
                if page.board_id in self._boards:
                    self._boards[page.board_id].merge_board(page)
                else:
                    self._boards[page.board_id] = page

        print(f'Loaded {len(self._boards)} Facebook pages')

        for k in tqdm(self._boards):
            self._boards[k].build_roots()

    def write_jsons(self, filepath):
        super(BuzzFace, self).write_jsons(filepath)

    def load_jsons(self, filepath, board_obj=None, post_obj=None):
        super(BuzzFace, self).load_jsons(filepath, FBPage, FBPost)

    def cache(self):
        self.write_jsons(filepath='Facebook/BF/')

    def load_cache(self):
        self.load_jsons(filepath='Facebook/BF/')


class Outlets(ConversationalDataset):
    def load(self):
        super(Outlets, self).load()

        for suffix in ['', '1', '2']:
            for f in tqdm(glob(f'{self.DATA_ROOT}Outlets/data{suffix}/*/')):
                page = FBPage.load_page(f[:-1])
                if page.board_id in self._boards:
                    self._boards[page.board_id].merge_board(page)
                else:
                    self._boards[page.board_id] = page

        print(f'Loaded {len(self._boards)} Facebook pages')

        for k in tqdm(self._boards):
            self._boards[k].build_roots()

    def write_jsons(self, filepath):
        super(Outlets, self).write_jsons(filepath)

    def load_jsons(self, filepath, board_obj=None, post_obj=None):
        super(Outlets, self).load_jsons(filepath, FBPage, FBPost)

    def cache(self):
        self.write_jsons(filepath='Facebook/Outlets/')

    def load_cache(self):
        self.load_jsons(filepath='Facebook/Outlets/')


class RedditCMV(ConversationalDataset):
    def load(self):
        super(RedditCMV, self).load()
        board = SubReddit.load_cmv()
        self._boards[board.board_id] = board
        self._boards[board.board_id].build_roots()

    def write_jsons(self, filepath):
        super(RedditCMV, self).write_jsons(filepath)

    def load_jsons(self, filepath, board_obj=None, post_obj=None):
        super(RedditCMV, self).load_jsons(filepath, SubReddit, RedditPost)

    def cache(self):
        self.write_jsons(filepath='Reddit/CMV/')

    def load_cache(self):
        self.load_jsons(filepath='Reddit/CMV/')


class RedditExtractor(ConversationalDataset):

    def dump_batch_by_date(self, filepath, date_str, thresh=180):
        for bid, board in self._boards.items():
            print(f'Building board: {bid}')
            board.construct_conversations()

            os.makedirs(self.DATA_ROOT + 'conversations/' + filepath, exist_ok=True)

            print(f'Extracting conversations')
            convos = board.conversations
            print(f'Found {display_num(len(convos))} conversations')

            dt = datetime.strptime(date_str, '%Y-%m')
            batch = 0
            cur = 0
            lines = []
            for convo_id, posts in tqdm(convos.items()):
                if (dt - board.posts[convo_id].created_at).days < thresh:
                    continue

                lines.append(json.dumps({
                    'convo_id': convo_id,
                    'posts':    posts
                }) + '\n')

                for post in posts:
                    # remove all posts
                    self._boards[bid].remove_post(post['post_id'])

                cur += len(posts)
                if cur > Chan.CONVO_SIZE:
                    path = self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}{date_str}_{batch:04d}.json'
                    with open(path, 'w+') as fp:
                        fp.writelines(lines)
                    print(f'Wrote batch {batch}: {display_num(cur)} posts, {display_num(len(lines))} conversations')
                    batch += 1
                    cur = 0
                    lines = []

            if lines:
                path = self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}{date_str}_{batch:04d}.json'
                with open(path, 'w+') as fp:
                    fp.writelines(lines)
                print(f'Wrote batch {batch}: {display_num(cur)} posts, {display_num(len(lines))} conversations')

    def load_batch(self, filepath):
        super(RedditExtractor, self).load()

        thresh = 180 + 20  # dump posts older that 180 days (thus archived)
        # jobs = mp.cpu_count() // 4

        # assure correct format and gather board names
        board_names = set()

        # pool = mp.Pool(processes=jobs)

        # print(f'Created pool with {jobs} workers')

        months = glob(f'{self.DATA_ROOT}DialoGPTdata/*/')

        # for names in tqdm(pool.imap_unordered(func=SubReddit.preprocess_extract,
        #                                       iterable=months), total=len(months)):
        for month in tqdm(sorted(months)):
            print(month)
            names = SubReddit.preprocess_extract(month)
            board_names |= names

        print(f'Found {len(board_names)} boards')

        for bid in board_names:
            print(f'Loading board: {bid}')
            board = SubReddit(bid)
            for f in tqdm(sorted(glob(f'{self.DATA_ROOT}DialoGPTdata/*/{bid}.json'))):
                with open(f) as fp:
                    for line in fp.readlines():
                        post = RedditPost(post_id=0)
                        post.from_json(json.loads(line))
                        board.add_post(post)
            
            self._boards[bid] = board
            self.dump_conversation(filepath='Reddit/RD')
            del self._boards[bid]

            # for bid, board in SubReddit.preprocess_extract(f).items():
            #     if bid in self._boards:
            #         self._boards[bid].merge_board(board)
            #     else:
            #         self._boards[bid] = board
            #
            # if len(self._boards):
            #     posts = sum([len(b.posts) for b in self._boards.values()])
            #     print(f'{display_num(posts)} posts in memory')
            #
            # date_str = f.split('/')[-2]
            # self.dump_batch_by_date(filepath, date_str, thresh=thresh)

    def cache(self):
        self.dump_conversation(filepath='Reddit/RD')

    def load_cache(self):
        self.load_conversation(filepath='Reddit/RD', board_cons=SubReddit, post_cons=RedditPost)


class Chan(ConversationalDataset):

    # BOARD = 'news'
    # BOARD = 'sci'
    # BOARD = 'his'
    # BOARD = 'x'
    # BOARD = 'g'
    BOARD = 'pol'

    CONVO_SIZE = 1e6

    def load(self):
        super(Chan, self).load()

        board = ChanBoard(self.BOARD)
        for i in tqdm(range(100)):
            board.merge_board(ChanBoard.load_chunk(Chan.BOARD, i))
        self._boards[self.BOARD] = board

    def batch_load(self):
        for i in tqdm(range(100)):
            self._boards[Chan.BOARD] = ChanBoard.load_chunk(Chan.BOARD, i)
            self.dump_conversation(filepath=f'4chan', board_suffix=i)

    def cache(self):
        self.dump_conversation(filepath=f'4chan')

    def load_cache(self):
        self.load_conversation(filepath=f'4chan', board_cons=ChanBoard, post_cons=ChanPost,
                               filepattern=f'{self.BOARD}_*')


if __name__ == '__main__':
    # Facebook

    # BF = BuzzFace()

    # BF.load()
    # BF.cache()

    # BF.load_cache()

    # BF.latex_table(sel='conversational')
    # BF.latex_table(sel='token')
    # BF.latex_table(sel='graph')

    # outlets = Outlets()

    # outlets.load()
    # outlets.cache()

    # outlets.load_cache()

    # outlets.latex_table(sel='conversational')
    # outlets.latex_table(sel='token')
    # outlets.latex_table(sel='graph')

    # cmv = RedditCMV()

    # complete raw rebuild
    # cmv.load()
    # cmv.cache()

    # Redact from cache and update cache
    # cmv.load_cache()
    # cmv.redact('Reddit/CMV')
    # cmv.cache()

    # Just read
    # cmv.load_cache()

    # cmv.latex_table(sel='conversational')
    # cmv.latex_table(sel='token')
    # cmv.latex_table(sel='graph')

    # cmv.load()
    # cmv.latex_table(sel='conversational')
    # cmv.latex_table(sel='token')
    # cmv.latex_table(sel='graph')

    # dialog = RedditExtractor()
    # dialog.batch_chunk('Reddit/RD/*/', 'data/batched/', SubReddit, RedditPost, batch_load=True)

    # complete raw rebuild
    # dialog.load_batch('Reddit/RD')

    # dialog.load()
    # dialog.cache()

    # Redact from cache and update cache
    # dialog.load_cache()
    # dialog.redact('Reddit/RD')
    # dialog.cache()

    # Just read
    # dialog.load_cache()

    # dialog.latex_table(sel='conversational')
    # dialog.latex_table(sel='token')
    # dialog.latex_table(sel='graph')

    # 4chan

    chan = Chan()
    # chan.stat('4chan', ChanBoard, ChanPost, filepattern='*', label='conversational')
    chan.stat('4chan', ChanBoard, ChanPost, filepattern='*', label='token')
    # chan.stat('4chan', ChanBoard, ChanPost, filepattern='*', label='topological')

    # chan.batch_load()
    # chan.load()  # loads the raw form
    # chan.cache()  # cache as conversation form
    # chan.load_cache()  # load conversation directly

    # chan.dump_conversation('4chan')  # dump in conversational chunks
    # chan.load_conversation('4chan')

    # chan.batch_chunk('4chan/', 'data/batched/chan/', ChanBoard, ChanPost)
    
    # chan.cache()
    # chan.load_cache()
    # chan.dump_old(datetime.today(), 0)

    # chan.latex_table(sel='conversational')
    # chan.latex_table(sel='token')
    # chan.latex_table(sel='graph')

    # import pdb
    # pdb.set_trace()

