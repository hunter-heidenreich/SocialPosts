import json

from glob import glob
from tqdm import tqdm

import sentencepiece as spm


class SocialTokenizer:

    def __init__(self, filepath='sp_model.model'):
        self.sp = spm.SentencePieceProcessor(model_file=filepath)

    def encode(self, text, t=int):
        return self.sp.encode(text, out_type=t)

    def decode(self, pieces):
        return self.sp.decode(pieces)

    @staticmethod
    def init_training(source_dir='data/'):
        with open(source_dir + 'raw_sents.txt', 'w+') as ff:

            # iterate over sub-directories
            for f in glob(source_dir + '*/*/text.json'):
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


if __name__ == '__main__':
    # tokenizer = SocialTokenizer('sp_model.model')

    # SocialTokenizer.init_training()
    SocialTokenizer.train(model_prefix='sp2')
