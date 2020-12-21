import os
import re
import json
import random

import pandas as pd
import numpy as np
import networkx as nx

from tqdm import tqdm
from glob import glob

from transformers import RobertaTokenizerFast


from utils import display_num

TOKENIZERS = {
    'roberta': RobertaTokenizerFast.from_pretrained('roberta-base')
}


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
    def _stat_conversational(conv, stats=None):
        """
        Returns the conversational stats
        for a conversation chunk
        """
        pids = {post['post_id'] for post in conv}

        out = {
            'sources':       1 if any([len(post['reply_to']) == 0 for post in conv]) else 0,
            'conversations': 1,
            'posts':         len(conv),
            'pairs':         len([1 for post in conv if len([1 for rid in post['reply_to'] if rid in pids]) > 0]),
            'voices':        '\t\t\t'.join({post['author'] for post in conv if post['author']})
        }

        if stats:
            out = {k: v for k, v in out.items() if k in stats}

        return out

    @staticmethod
    def _stat_tokens(conv, stats=None):
        """
        Produces stats about the space-separated tokens
        of a conversation chunk
        """
        tokens = re.split('\s+', ' '.join([post['text'] for post in conv]))
        normal = set(tokens)
        lower = {n.lower() for n in normal}

        out = {
            'unique':       '\t\t\t'.join(normal),
            'unique_lower': '\t\t\t'.join(lower),
            'tokens':       len(tokens)
        }

        if stats:
            out = {k: v for k, v in out.items() if k in stats}

        return out

    @staticmethod
    def _stat_topo(conv, stats=None):
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

        out = {
            'nodes':        graph.number_of_nodes(),
            'density':      nx.density(graph),
            'longest_path': nx.algorithms.dag_longest_path_length(graph),
            'degrees':      np.average([graph.in_degree[n] + graph.out_degree[n] for n in graph.nodes]),
        }

        if stats:
            out = {k: v for k, v in out.items() if k in stats}

        return out

    @staticmethod
    def _stat_tokenizer(conv, tokenizer='roberta'):
        if tokenizer == 'roberta':
            tok = TOKENIZERS[tokenizer]
        else:
            raise ValueError(f'Unrecognized tokenizer: {tokenizer}')

        pids = [p['post_id'] for p in conv]

        txts = [p['text'] for p in conv]
        txts = tok(txts)['input_ids']
        txts = [len(ts) for ts in txts]

        out = [{'post_id': pid, 'token_len': ts} for (pid, ts) in zip(pids, txts)]

        return out

    @staticmethod
    def _stat_conversation(conv, label='conversational', stats=None):
        """
        High-level interface for extracting stats and insights
        about conversational chunks
        """
        if label == 'conversational':
            return ConversationalDataset._stat_conversational(conv, stats=stats)
        elif label == 'token':
            return ConversationalDataset._stat_tokens(conv, stats=stats)
        elif label == 'topological':
            return ConversationalDataset._stat_topo(conv, stats=stats)
        elif 'tokenizer' in label:
            return ConversationalDataset._stat_tokenizer(conv, tokenizer=label.split('_')[-1])
        else:
            raise ValueError(f'Unrecognized label value: {label}')

    @staticmethod
    def scan_tokenizer(filepath, label='tokenizer_roberta', threshes=(32, 64, 128, 256)):
        try:
            df = pd.read_pickle(f'{ConversationalDataset.DATA_ROOT}conversations/{filepath}_stat_{label}.pkl')

            print(f'Mean size: {np.mean(df.token_len):.2f}')
            print(f'Std. Dev.: {np.std(df.token_len):.2f}')

            for thresh in threshes:
                soft = np.mean([thresh / max(thresh, x) for x in df.token_len])
                hard = df[df.token_len <= thresh].shape[0] / df.shape[0]
                print(f'{thresh} >=: {100 * hard:.2f} / {100 * soft:.2f}')

        except FileNotFoundError:
            print(f'Not generated {label} sizes for {filepath}.\n\nCache stat and try again')

    def stat(self, filepath, board_cons, post_cons, filepattern='*', label='conversational', stats=None, latex=False,
             load_cache=True):
        """
        Given a file pattern, this function will compute
        stats about the cached conversations found
        in a manner that is more memory efficient
        than loading all chached conversations
        into memory directly.
        This function will print a latex table, by default.
        """
        df = None

        if load_cache:
            try:
                df = pd.read_pickle(f'{ConversationalDataset.DATA_ROOT}conversations/{filepath}_stat_{label}.pkl')
            except FileNotFoundError:
                pass

        if df is None:
            for bid, board_chunk in self.conversation_iterator(filepath, board_cons, post_cons, filepattern):
                chunk_stats = []
                for conv in tqdm(board_chunk.conversations.values()):
                    s = self._stat_conversation(conv, label, stats=stats)

                    if type(s) == list:
                        for s_ in s:
                            s_['board_id'] = bid
                        chunk_stats.extend(s)
                    else:
                        s['board_id'] = bid
                        chunk_stats.append(s)

                if df is not None:
                    df = df.append(chunk_stats, ignore_index=True)
                else:
                    df = pd.DataFrame(chunk_stats)

        if df is not None and latex:
            self._latex_table(df, label)

        df.to_pickle(f'{ConversationalDataset.DATA_ROOT}conversations/{filepath}_stat_{label}.pkl')

        return df

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
