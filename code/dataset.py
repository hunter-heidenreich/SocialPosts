import os
import re
import json
import random

import multiprocessing as mp
import pandas as pd
import numpy as np
import networkx as nx

from datetime import datetime
from collections import defaultdict
from tqdm import tqdm
from glob import glob

from torch.utils.data import Dataset

from post import Tweet, RedditPost, FBPost, ChanPost
from board import TwitterUser, FBPage, SubReddit, ChanBoard
from utils import display_num


class ConversationalDataset(Dataset):

    DATA_ROOT = '/Users/hsh28/data/'

    def __init__(self):
        self._boards = {}
        self._ids = []

    def load(self):
        self._ids = list(self._boards.keys())

    def dump_conversation(self, filepath, board_suffix=''):
        for bid, board in self._boards.items():
            print(f'Building board: {bid}')
            board.construct_conversations()

            os.makedirs(self.DATA_ROOT + 'conversations/' + filepath, exist_ok=True)

            print(f'Extracting conversations')
            convos = board.conversations
            print(f'Found {display_num(len(convos))} conversations')

            batch = 0
            cur = 0
            lines = []
            for convo_id, posts in tqdm(convos.items()):
                lines.append(json.dumps({
                    'convo_id': convo_id,
                    'posts':    posts
                }) + '\n')

                cur += len(posts)
                if cur > Chan.CONVO_SIZE:
                    path = self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}_{board_suffix:04d}.json' \
                        if type(board_suffix) == int \
                        else self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}{board_suffix}_{batch:04d}.json'
                    with open(path, 'w+') as fp:
                        fp.writelines(lines)
                    print(f'Wrote batch {batch}: {display_num(cur)} posts, {display_num(len(lines))} conversations')
                    batch += 1
                    cur = 0
                    lines = []

            if lines:
                path = self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}_{board_suffix:04d}.json' \
                    if type(board_suffix) == int \
                    else self.DATA_ROOT + 'conversations/' + filepath + f'/{bid}{board_suffix}_{batch:04d}.json'
                with open(path, 'w+') as fp:
                    fp.writelines(lines)
                print(f'Wrote batch {batch}: {display_num(cur)} posts, {display_num(len(lines))} conversations')

    def load_conversation(self, filepath, board_cons, post_cons, filepattern='*',):
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
            print(f'Loaded board: {bid} ({display_num(len(board.posts))} posts, {display_num(len(board._convo_id_to_pids))} conversations)')

    def conversation_iterator(self, filepath, board_cons, post_cons, filepattern='*'):
        for f in tqdm(sorted(glob(self.DATA_ROOT + 'conversations/' + filepath + f'/{filepattern}.json'))):
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
    def stat_conversation(conv, label='conversational'):
        if label == 'conversational':
            pids = {post['post_id'] for post in conv}
            return {
                'sources': 1 if any([len(post['reply_to']) == 0 for post in conv]) else 0,
                'conversations': 1,
                'posts': len(conv),
                'pairs': len([1 for post in conv if len([1 for rid in post['reply_to'] if rid in pids]) > 0]),
                'voices': '\t\t\t'.join({post['author'] for post in conv if post['author']})
            }
        elif label == 'token':
            tokens = re.split('\s+', ' '.join([post['text'] for post in conv]))
            normal = set(tokens)
            lower = {n.lower() for n in normal}
            return {
                'unique': '\t\t\t'.join(normal),
                'unique_lower': '\t\t\t'.join(lower),
                'tokens': len(tokens)
            }
        elif label == 'topological':
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
                'in_degrees':   np.average([graph.in_degree[n] for n in graph.nodes]),
                'out_degrees': np.average([graph.out_degree[n] for n in graph.nodes])
            }
        else:
            raise ValueError(f'Unrecognized label value: {label}')

    def stat(self, filepath, board_cons, post_cons, filepattern='*', label='conversational'):
        df = None
        for bid, board_chunk in self.conversation_iterator(filepath, board_cons, post_cons, filepattern):
            chunk_stats = []
            for conv in board_chunk.conversations.values():
                s = self.stat_conversation(conv, label)
                s['board_id'] = bid
                chunk_stats.append(s)

            if df is not None:
                df = df.append(chunk_stats, ignore_index=True)
            else:
                df = pd.DataFrame(chunk_stats)

        self.latex_table(df, label)

    def latex_table(self, df, label):
        table = []
        if label == 'conversational':
            #  calculate total first...
            total_vox = set()
            total_posts = df.posts.sum()
            for group, dfi in df.groupby('board_id'):
                vox = set()
                for vs in dfi.voices.values:
                    for v in vs.split('\t\t\t'):
                        vox.add(v)
                total_vox |= vox

                post_cnt = dfi.posts.sum()
                if post_cnt / total_posts < 0.01:
                    continue

                out = '\t'
                out += group.replace('_', '\\_').replace('%', '\\%') + ' & '
                out += display_num(int(dfi.sources.sum())) + ' & '
                out += display_num(int(dfi.conversations.sum())) + ' & '
                out += display_num(int(post_cnt)) + f' ({100 * post_cnt / total_posts:.2f}\\%)' + ' & '
                out += display_num(int(dfi.pairs.sum())) + ' & '
                out += display_num(len(vox)) + ' \\\\ '

                table.append(out)

            table.append('\t\\hline')

            # group voices by conversation
            df.voices = [len(v.split('\t\t\t')) for v in df.voices.values]

            out = '\t'
            out += 'Avg. Conversation & '
            out += display_num(df.sources.mean()) + ' $\\pm$ ' + display_num(df.sources.std()) + ' & '
            out += display_num(df.conversations.mean()) + ' $\\pm$ ' + display_num(df.conversations.std()) + ' & '
            out += display_num(df.posts.mean()) + ' $\\pm$ ' + display_num(df.posts.std()) + ' & '
            out += display_num(df.pairs.mean()) + ' $\\pm$ ' + display_num(df.pairs.std()) + ' & '
            out += display_num(df.voices.mean()) + ' $\\pm$ ' + display_num(df.voices.std()) + ' \\\\ '

            table.append(out)

            out = '\t'
            out += 'Total & '
            out += display_num(df.sources.sum()) + ' & '
            out += display_num(df.conversations.sum()) + ' & '
            out += display_num(total_posts) + ' & '
            out += display_num(df.pairs.sum()) + ' & '
            out += display_num(len(total_vox)) + ' \\\\ '

            table.append(out)
        elif label == 'token':
            total_unique = set()
            total_lower = set()
            total_tokens = df.tokens.sum()
            for group, dfi in df.groupby('board_id'):
                unique = set()
                for vs in dfi.unique.values:
                    for v in vs.split('\t\t\t'):
                        unique.add(v)
                total_unique |= unique

                lower = set()
                for vs in dfi.unique_lower.values:
                    for v in vs.split('\t\t\t'):
                        lower.add(v)
                total_lower |= lower

                token_cnt = dfi.tokens.sum()
                if token_cnt / total_tokens < 0.01:
                    continue

                out = '\t'
                out += group.replace('_', '\\_').replace('%', '\\%') + ' & '
                out += display_num(token_cnt) + f' ({100 * token_cnt / total_tokens:.2f}\\%)' + ' & '
                out += display_num(len(unique)) + ' & '
                out += display_num(len(lower)) + ' \\\\ '

                table.append(out)

            table.append('\t\\hline')

            # group tokens by conversation
            df.unique = [len(v.split('\t\t\t')) for v in df.unique.values]
            df.unique_lower = [len(v.split('\t\t\t')) for v in df.unique_lower.values]

            out = '\t'
            out += 'Avg. Conversation & '
            out += display_num(df.tokens.mean()) + ' $\\pm$ ' + display_num(df.tokens.std()) + ' & '
            out += display_num(df.unique.mean()) + ' $\\pm$ ' + display_num(df.unique.std()) + ' & '
            out += display_num(df.unique_lower.mean()) + ' $\\pm$ ' + display_num(df.unique_lower.std()) + ' \\\\ '

            table.append(out)

            out = '\t'
            out += 'Total & '
            out += display_num(total_tokens) + ' & '
            out += display_num(len(total_unique)) + ' & '
            out += display_num(len(total_lower)) + ' \\\\ '

            table.append(out)
        elif label == 'topological':
            total_degree = df.nodes.sum()
            for group, dfi in df.groupby('board_id'):
                degree = dfi.nodes.sum()
                if degree / total_degree < 0.01:
                    continue

                out = '\t'
                out += group.replace('_', '\\_').replace('%', '\\%') + ' & '
                out += display_num(dfi.in_degrees.mean()) + ' $\\pm$ ' + display_num(dfi.in_degrees.std()) + ' & '
                out += display_num(dfi.out_degrees.mean()) + ' $\\pm$ ' + display_num(dfi.out_degrees.std()) + ' & '
                out += display_num(dfi.longest_path.mean()) + ' $\\pm$ ' + display_num(dfi.longest_path.std()) + ' & '
                out += display_num(dfi.density.mean()) + ' $\\pm$ ' + display_num(dfi.density.std()) + ' \\\\ '

                table.append(out)

            table.append('\t\\hline')

            out = '\t'
            out += 'Avg. Conversation & '
            out += display_num(df.in_degrees.mean()) + ' $\\pm$ ' + display_num(df.in_degrees.std()) + ' & '
            out += display_num(df.out_degrees.mean()) + ' $\\pm$ ' + display_num(df.out_degrees.std()) + ' & '
            out += display_num(df.longest_path.mean()) + ' $\\pm$ ' + display_num(df.longest_path.std()) + ' & '
            out += display_num(df.density.mean()) + ' $\\pm$ ' + display_num(df.density.std()) + ' \\\\ '

            table.append(out)
        else:
            print(label)
            raise ValueError

        print('\n'.join(table))
        print()

    def redact(self, root):
        with open(f'{self.DATA_ROOT}conversations/{root}.json', 'w+') as fp:
            for board in tqdm(self._boards):
                ns = self._boards[board].redact()
                line = f'{board}\t{json.dumps(ns)}\n'
                fp.write(line)

    def batch_chunk(self, pattern, outpath, board_cons, post_cons,
                    seed=42, batch_size=2048 * 2048, dev_ratio=0.01):
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

    def __len__(self):
        return len(self._ids)

    def __getitem__(self, item):
        return self._ids[item]


class NewstweetThreads(ConversationalDataset):

    def load(self):
        super(NewstweetThreads, self).load()

        for f in tqdm(glob(f'{self.DATA_ROOT}threads/*tweets.json')):
            user = TwitterUser.load_thread(f)
            if user.board_id in self._boards:
                self._boards[user.board_id].merge_board(user)
            else:
                self._boards[user.board_id] = user

        print(f'Loaded {len(self._boards)} user conversations')

    def cache(self):
        self.dump_conversation(filepath=f'Twitter/NTT')

    def load_cache(self):
        self.load_conversation(filepath=f'Twitter/NTT', board_cons=TwitterUser, post_cons=Tweet)


class CoordinatedTargetingQuotes(ConversationalDataset):
    def load(self):
        super(CoordinatedTargetingQuotes, self).load()

        self._boards['CTQ'] = TwitterUser('CTQ')
        for f in tqdm(glob(f'{self.DATA_ROOT}quote_tweets/quotes/*.json')):
            self._boards['CTQ'].merge_board(TwitterUser.load_quote_month(f))

        print(f'Loaded {len(self._boards)} user conversations')

    def cache(self):
        self.dump_conversation(filepath=f'Twitter/CTQ')

    def load_cache(self):
        self.load_conversation(filepath=f'Twitter/CTQ', board_cons=TwitterUser, post_cons=Tweet)


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
        jobs = mp.cpu_count()

        # assure correct format and gather board names
        board_names = set()

        pool = mp.Pool(processes=jobs)

        print(f'Created pool with {jobs} workers')

        months = glob(f'{self.DATA_ROOT}DialoGPTdata/*/')

        for names in tqdm(pool.imap_unordered(func=SubReddit.preprocess_extract,
                                              iterable=months), total=len(months)):
            board_names |= names

        print(f'Found {len(board_names)} boards')

        import pdb
        pdb.set_trace()

        for bid in board_names:
            pass

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
    # Twitter

    # NTT = NewstweetThreads()

    # complete raw rebuild
    # NTT.load()
    # NTT.cache()

    # Redact from cache and update cache
    # NTT.load_cache()
    # NTT.redact('Twitter/NewsTweetThreads')
    # NTT.cache()

    # Just read
    # NTT.load_cache()

    # NTT.stat('Twitter/NTT', TwitterUser, Tweet, label='conversational')
    # NTT.stat('Twitter/NTT', TwitterUser, Tweet, label='token')
    # NTT.stat('Twitter/NTT', TwitterUser, Tweet, label='topological')

    # CTQ = CoordinatedTargetingQuotes()

    # complete raw rebuild
    # CTQ.load()
    # CTQ.cache()

    # Redact from cache and update cache
    # CTQ.load_cache()
    # CTQ.redact('Twitter/CoordinatedTargetingQuotes')
    # CTQ.cache()

    # Just read
    # CTQ.load_cache()

    # CTQ.stat('Twitter/CTQ', TwitterUser, Tweet, label='conversational')
    # CTQ.stat('Twitter/CTQ', TwitterUser, Tweet, label='token')
    # CTQ.stat('Twitter/CTQ', TwitterUser, Tweet, label='topological')

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
    chan.stat('4chan', ChanBoard, ChanPost, filepattern='*', label='conversational')
    chan.stat('4chan', ChanBoard, ChanPost, filepattern='*', label='token')
    chan.stat('4chan', ChanBoard, ChanPost, filepattern='*', label='topological')

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

