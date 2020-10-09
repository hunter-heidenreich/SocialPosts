from collections import defaultdict
from glob import glob

from tqdm import tqdm

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def facebook_data_stats():
    stats = {}
    for f in tqdm(glob('data/fb/*/')):
        page = f.split('/')[-2]
        stats[page] = defaultdict(int)

        with open(f + 'text.json') as ff:
            stats[page]['text'] += len(ff.readlines())

        with open(f + 'pairs.json') as ff:
            stats[page]['pairs'] += len(ff.readlines())

    totals = defaultdict(int)
    for vs in stats.values():
        for k, v in vs.items():
            totals[k] += v

    return stats, totals


def facebook_histogram():
    stats, totals = facebook_data_stats()

    df = []
    for k, v in stats.items():
        df.append({
            'title': k,
            'text': v['text'] / totals['text'],
            'pairs': v['pairs'] / totals['pairs']
        })

    key = 'pairs'
    thresh = 0.01

    df = sorted(df, key=lambda x: x[key], reverse=True)
    df = [d for d in df if d[key] > thresh]
    df = pd.DataFrame(df)

    chart = sns.barplot(x=key, y='title', data=df)
    # chart.set_xticklabels(chart.get_xticklabels(), rotation=75)
    chart.set_title(f'Facebook data by {key}')
    plt.show()


def gen_facebook_table():
    stats, totals = facebook_data_stats()

    latex = ''
    latex += '\t\\hline\n'

    for ix, (k, v) in enumerate(sorted(stats.items(), key=lambda ks: ks[0])):
        if ix % 2 == 0:
            latex += '\t'
        else:
            latex += '& '

        latex += k.replace('_', ' ').replace('%', '\\%')
        latex += ' & '

        p = 100 * v['text'] / totals['text']
        if p > 2:
            latex += '\\textbf{' + f"{v['text']} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{v['text']} ({p:.2f}\\%)"

        latex += ' & '
        p = 100 * v['pairs'] / totals['pairs']
        if p > 2:
            latex += '\\textbf{' + f"{v['pairs']} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{v['pairs']} ({p:.2f}\\%)"

        if ix % 2 == 1:
            latex += '\\\\ \n'

    latex += '\t\\hline\n'
    latex += '\t'
    latex += 'Total'
    latex += ' & '
    latex += f"{totals['text']}"
    latex += ' & '
    latex += f"{totals['pairs']}"
    latex += ' & '
    latex += ''
    latex += ' & '
    latex += f""
    latex += ' & '
    latex += f""
    latex += '\\\\ \n'
    latex += '\t\\hline\n'

    print(latex)


if __name__ == '__main__':
    # facebook_data_stats()
    facebook_histogram()
