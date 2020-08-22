import pdb
import json

from collections import defaultdict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

from tqdm import tqdm

from scipy.sparse import vstack
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def intra_buzzface_analysis():
    bf = json.load(open('buzzface_post_docs.json'))
    page_to_id = json.load(open('buzzface_id_lookup.json'))
    id_to_page = {str(pid): name for name, pid in page_to_id.items()}

    # Similarities across BuzzFace pages
    raw = [text for page in bf.values() for text in page.values()]
    vec = TfidfVectorizer()
    vec.fit(raw)

    bf_page_vec = {page_id: {
        post_id: vec.transform([text]) for post_id, text in page.items()
    } for page_id, page in tqdm(bf.items())}

    sims = defaultdict(dict)
    sim_df = []
    for pid0, page0 in bf_page_vec.items():
        for pid1, page1 in bf_page_vec.items():
            # skip sims that we already have
            # if pid0 in sims and pid1 in sims[pid0]:
            #     continue

            # split features
            X = vstack(page0.values())
            Y = vstack(page1.values())
            scores = cosine_similarity(X, Y)

            # save sims
            sims[pid0][pid1] = scores
            sims[pid1][pid0] = scores.transpose()
            sim_df.append({
                'x':   id_to_page[pid1],
                'y':   id_to_page[pid0],
                'avg': np.average(scores),
                'max': min(np.max(scores), 1.0)
            })

            # _ = sns.heatmap(sims[pid0][pid1])
            # plt.xlabel(f'{id_to_page[pid0]}')
            # plt.ylabel(f'{id_to_page[pid1]}')

            # fig = heat_map.get_figure()
            # plt.savefig(f'figs/bf_tfidf_sims/{pid0}_{pid1}.png')
            # plt.clf()

    sim_df = pd.DataFrame(sim_df)
    # sim = sim_df.pivot("x", "y", "max")
    # sns.heatmap(sim, annot=True)
    # plt.savefig(f'figs/bf_tfidf_sims_max.png')
    # plt.show()

    for name in page_to_id:
        s = sim_df[sim_df.x == name]
        maxs = s.sort_values('max', ascending=False)
        avgs = s.sort_values('avg', ascending=False)

        print(f'{name} -- by max similarity')
        for i in range(3):
            print(f'\t{i + 1}: {maxs.iloc[i]["y"]} ({maxs.iloc[i]["max"]})')

        print(f'{name} -- by avg similarity')
        for i in range(3):
            print(f'\t{i + 1}: {avgs.iloc[i]["y"]} ({avgs.iloc[i]["avg"]})')


def sample_cmv_by_bf():
    # load and extract buzzface text
    bf = json.load(open('buzzface_post_docs.json'))
    bf_text = [text for page in tqdm(bf.values()) for text in page.values()]

    # load and extract r/cmv text
    cmv = json.load(open('reddit_cmv_post_docs.json'))

    # create post_id locators
    cmv_ids, cmv_text = [], []
    for pid, text in tqdm(cmv.items()):
        cmv_ids.append(pid)
        cmv_text.append(text)

    # combine full raw
    raw = bf_text + cmv_text

    # create vectorizer
    vec = TfidfVectorizer()
    vec.fit(raw)

    cmv_vecs = vec.transform(cmv_text)
    bf_vecs = vec.transform(bf_text)
    sims = cosine_similarity(bf_vecs, cmv_vecs)

    for thresh in np.arange(0.0, 0.95, 0.05):
        mask = np.max(sims, axis=0) > thresh
        ids = np.where(mask)[0]

        cmv_subset = [cmv_ids[idx] for idx in ids]
        json.dump(list(cmv_subset), open(f'cmv_bf_ids_{thresh:.2f}.json', 'w+'))


def sample_4chan_by_bf(board):
    # load and extract buzzface text
    bf = json.load(open('out/buzzface_post_docs.json'))
    bf_text = [text for page in tqdm(bf.values()) for text in page.values()]

    # load and extract r/cmv text
    # chan = json.load(open(f'4chan_{board}_post_docs.json'))

    for i in tqdm(range(100)):
        chan = {}
        for k, v in json.load(open(f'data/docs/{board}/4chan_pol_post_docs_{i:02d}.json')).items():
            chan[k] = v

        # create post_id locators
        chan_ids, chan_text = [], []
        for pid, text in tqdm(chan.items()):
            chan_ids.append(pid)
            chan_text.append(text)

        # combine full raw
        raw = bf_text + chan_text

        # create vectorizer
        vec = TfidfVectorizer()
        vec.fit(raw)

        chan_vecs = vec.transform(chan_text)
        bf_vecs = vec.transform(bf_text)

        sims = cosine_similarity(bf_vecs, chan_vecs)
        maxs = np.max(sims, axis=0)

        for thresh in np.arange(0.0, 0.95, 0.05):
            mask = maxs > thresh
            ids = np.where(mask)[0]

            chan_subset = [chan_ids[idx] for idx in ids]
            json.dump(list(chan_subset), open(f'out/4chan_{board}_{i:02d}_bf_ids_{thresh}.json', 'w+'))


def main():
    # for understanding what similar pages look like
    # intra_buzzface_analysis()

    # sample_cmv_by_bf()
    sample_4chan_by_bf('pol')


if __name__ == '__main__':
    main()
