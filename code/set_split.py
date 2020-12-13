import json
import random
import numpy as np

from glob import glob
from tqdm import tqdm

from dataset import RedditExtractor


if __name__ == '__main__':
    seed = 42
    np.random.seed(seed)
    random.seed = seed

    lang = 'en'

    # platform = 'twitter'
    # platform = 'fb'
    platform = 'reddit'

    # percent of pairs that will be dev
    dev_p = 0.01

    batch_size = 2048 * 2048  # allowing multiple batches to be loaded into memory at once (amortization)

    tr_batch = 0
    tr_cur = []
    dv_cur = []

    outpath = f'data/batched/{platform}_{lang}/'

    for f in glob(f'data/{platform}/*/'):
        print(f)

        text = {}
        with open(f + f'text_{lang}.json') as fp:
            for line in tqdm(fp.readlines()):
                if line.strip():
                    dat = json.loads(line)
                    text[dat['id']] = dat

        pairs = []
        with open(f + f'pairs_{lang}.json') as fp:
            for line in tqdm(fp.readlines()):
                if line.strip():
                    pairs.append(json.loads(line))

        dev_size = int(np.ceil(dev_p * len(pairs)))
        print(f'Found {len(pairs)}, holding {dev_size} for dev!')

        random.shuffle(pairs)
        train_ps, dev_ps = pairs[dev_size:], pairs[:dev_size]

        for pair in tqdm(train_ps):
            post = text[pair['post']]['text']
            reply = text[pair['reply']]['text']
            if len(post) > 0 or len(reply) > 0:
                tr_cur.append(json.dumps({
                    'post':  post,
                    'reply': reply
                }) + '\n')

            if len(tr_cur) == batch_size:
                with open(f'{outpath}train_{tr_batch}.json', 'w+') as fp:
                    fp.write(''.join(tr_cur))

                tr_cur = []
                tr_batch += 1

        for pair in tqdm(dev_ps):
            post = text[pair['post']]['text']
            reply = text[pair['reply']]['text']
            if len(post) > 0 or len(reply) > 0:
                dv_cur.append(json.dumps({
                    'post':  post,
                    'reply': reply
                }) + '\n')

            if len(dv_cur) == batch_size:
                with open(f'{outpath}dev.json', 'a+') as fp:
                    fp.write(''.join(dv_cur))

                dv_cur = []

    if tr_cur:
        with open(f'{outpath}train_{tr_batch}.json', 'a+') as fp:
            fp.write(''.join(tr_cur))

    if dv_cur:
        with open(f'{outpath}dev.json', 'a+') as fp:
            fp.write(''.join(dv_cur))
