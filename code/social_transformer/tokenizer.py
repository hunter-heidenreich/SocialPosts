import json

from glob import glob
from tqdm import tqdm

import numpy as np
import sentencepiece as spm

from collections import defaultdict
from transformers import AlbertTokenizer, RobertaTokenizer


class SocialTokenizer:

    def __init__(self, filepath='sp_model.model'):
        self.sp = spm.SentencePieceProcessor(model_file=filepath)

    def encode(self, text, t=int):
        return self.sp.encode(text, out_type=t)

    def decode(self, pieces):
        return self.sp.decode(pieces)

    @staticmethod
    def init_training(source_dir='data/', outname='raw_sents,txt', restrictions=None):
        with open(source_dir + outname, 'w+') as ff:

            # iterate over sub-directories
            for f in glob(source_dir + '*/*/text_en.json'):
                plat = f.split('/')[-3]
                if not restrictions:
                    pass
                else:
                    check = False
                    for r in restrictions:
                        check = check or (r in plat)
                    if not check:
                        continue

                print(f)
                with open(f) as inp:
                    lines = inp.readlines()

                text = []
                for line in tqdm(lines):
                    data = json.loads(line)
                    for snip in data['text'].split('\n'):
                        if snip:
                            text.append(snip)

                    if len(text) > 5000:
                        out = '\n'.join(text)
                        ff.write(out + '\n')
                        text = []

                if text:
                    out = '\n'.join(text)
                    ff.write(out + '\n')

    @staticmethod
    def train(input_file='data/raw_sents.txt', model_prefix='sp_model', vocab_size=30522):
        spm.SentencePieceTrainer.train(input=input_file, model_prefix=model_prefix, vocab_size=vocab_size,
                                       input_sentence_size=2 ** 16, shuffle_input_sentence=True)


def get_coverage(source_dir, model, threshes=(32, 64, 128, 256)):
    if mod == 'roberta':
        tokenizer = RobertaTokenizer.from_pretrained('roberta-base')
    else:
        tokenizer = AlbertTokenizer(model, do_lower_case=False, keep_accents=True)

    sizes = []
    for f in tqdm(glob(source_dir + '/*/text_en.json')):
        with open(f) as ff:
            for line in ff.readlines():
                txt = json.loads(line)['text']
                sizes.append(len(tokenizer.tokenize(txt)))

    print(f'Tuned size: {np.mean(sizes):.2f} +/- {np.std(sizes):.2f}')
    for thresh in threshes:
        hard = 0
        soft = 0
        for s in sizes:
            if s <= thresh:
                hard += 1
                soft += 1
            else:
                soft += thresh / s

        print(f'Hard-threshold of {thresh}: {100 * hard / len(sizes):.2f}\\%')
        print(f'Soft-threshold of {thresh}: {100 * soft / len(sizes):.2f}\\%')
        print()


if __name__ == '__main__':
    # tokenizer = SocialTokenizer('sp_model.model')

    # raw_sents = '4chan_sents.txt'
    # doms = {'4chan'}

    # raw_sents = 'twitter_sents.txt'
    # doms = {'twitter'}

    # raw_sents = 'fb_sents.txt'
    # doms = {'fb'}

    # raw_sents = 'reddit_sents.txt'
    # doms = {'reddit'}

    # SocialTokenizer.init_training(outname=raw_sents, restrictions=doms)

    # inp = 'data/twitter_sents.txt'
    # prefix = 'twitter_model'

    # inp = 'data/fb_sents.txt'
    # prefix = 'fb_model'

    # inp = 'data/reddit_sents.txt'
    # prefix = 'reddit_model'

    # inp = 'data/all_sents.txt'
    # prefix = 'all_model'

    # vocab = 2**15

    # vocab = 50257
    # vocab = 48813  # for FB

    # SocialTokenizer.train(input_file=inp, model_prefix=prefix, vocab_size=vocab)

    # checking custom tokenizer coverage
    # mod = 'all_model.model'
    mod = 'roberta'

    plat = 'data/twitter'
    # mod = 'twitter_model.model'
    get_coverage(plat, mod)

    plat = 'data/fb'
    # mod = 'fb_model.model'
    get_coverage(plat, mod)

    plat = 'data/reddit'
    # mod = 'reddit_model.model'
    get_coverage(plat, mod)
