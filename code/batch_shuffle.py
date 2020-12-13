import random

from glob import glob
from tqdm import tqdm


if __name__ == '__main__':

    # plat = 'fb'
    # lang = 'en'

    # outpath = f'data/batched/{plat}_{lang}/'
    outpath = f'data/batched/chan/'
    # outpath = f'data/batched/reddit/'

    import pdb

    for fi in glob(outpath + 'train_*.json'):

        with open(fi) as fp:
            outer = fp.readlines()
        print(fi)
        for fj in tqdm(glob(outpath + 'train_*.json')):
            if fi == fj:
                continue

            with open(fj) as fp:
                inner = fp.readlines()

            mix = outer + inner
            random.shuffle(mix)

            half = len(mix) // 2
            outer = mix[:half]
            inner = mix[half:]

            with open(fj, 'w+') as fp:
                fp.writelines(inner)

        with open(fi, 'w+') as fp:
            fp.writelines(outer)
