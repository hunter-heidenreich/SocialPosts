from collections import defaultdict
from glob import glob

from tqdm import tqdm

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def chan_data_stats():
    stats = {}
    for f in tqdm(glob('data/4chan/*/')):
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


def gen_chan_table():
    stats, totals = chan_data_stats()
    scale = 1_000_000

    uniform = 100 / len(stats)
    mult = 4
    double = mult * uniform
    half = uniform / mult

    latex = ''
    latex += '\t\\hline\n'

    for ix, (k, v) in enumerate(sorted(stats.items(), key=lambda ks: ks[0])):
        latex += '\t'
        latex += k.replace('_', ' ').replace('%', '\\%')
        latex += ' & '

        p = 100 * v['text'] / totals['text']
        if p > double:
            latex += '\\textbf{' + f"{v['text']/scale:.1f} M ({p:.2f}\\%)" + '}'
        elif p < half:
            latex += '\\underline{' + f"{v['text'] / scale:.1f} M ({p:.2f}\\%)" + '}'
        else:
            latex += f"{v['text']/scale:.1f} M ({p:.2f}\\%)"

        latex += ' & '
        p = 100 * v['pairs'] / totals['pairs']
        if p > double:
            latex += '\\textbf{' + f"{v['pairs']/scale:.1f} M ({p:.2f}\\%)" + '}'
        elif p < half:
            latex += '\\underline{' + f"{v['pairs'] / scale:.1f} M ({p:.2f}\\%)" + '}'
        else:
            latex += f"{v['pairs']/scale:.1f} M ({p:.2f}\\%)"

        latex += '\\\\ \n'

    latex += '\t\\hline\n'
    latex += '\t'
    latex += 'Total'
    latex += ' & '
    latex += f"{totals['text']/scale:.1f}"
    latex += ' & '
    latex += f"{totals['pairs']/scale:.1f}"
    latex += '\\\\ \n'
    latex += '\t\\hline\n'

    print(latex)


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


def twitter_data_stats():
    stats = {}
    for f in tqdm(glob('data/twitter/*/')):
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

    uniform = 100 / len(stats)
    double = 2 * uniform
    half = 0.5 * uniform

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
        if v['text'] > 100_000:
            label = f"{v['text'] / 1_000_000:.1f} M"
        elif v['text'] > 100:
            label = f"{v['text'] / 1_000:.1f} K"
        else:
            label = f"{v['text']}"

        if p > double:
            latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
        elif p < half:
            latex += '\\underline{' + f"{label} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{label} ({p:.2f}\\%)"

        latex += ' & '
        p = 100 * v['pairs'] / totals['pairs']
        if v['text'] > 100_000:
            label = f"{v['pairs'] / 1_000_000:.1f} M"
        elif v['text'] > 100:
            label = f"{v['pairs'] / 1_000:.1f} K"
        else:
            label = f"{v['pairs']}"

        if p >double:
            latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
        elif p < half:
            latex += '\\underline{' + f"{label} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{label} ({p:.2f}\\%)"

        if ix % 2 == 1:
            latex += '\\\\ \n'

    if totals['text'] > 100_000:
        t_label = f"{totals['text'] / 1_000_000:.1f} M"
    elif totals['text'] > 100:
        t_label = f"{totals['text'] / 1_000:.1f} K"
    else:
        t_label = f"{totals['text']}"

    if totals['pairs'] > 100_000:
        p_label = f"{totals['pairs'] / 1_000_000:.1f} M"
    elif totals['pairs'] > 100:
        p_label = f"{totals['pairs'] / 1_000:.1f} K"
    else:
        p_label = f"{totals['pairs']}"

    latex += '\t\\hline\n'
    latex += '\t'
    latex += 'Total'
    latex += ' & '
    latex += f"{t_label}"
    latex += ' & '
    latex += f"{p_label}"
    latex += ' & '
    latex += ''
    latex += ' & '
    latex += f""
    latex += ' & '
    latex += f""
    latex += '\\\\ \n'
    latex += '\t\\hline\n'

    print(latex)


def gen_twitter_table():
    stats, totals = twitter_data_stats()

    double = 5

    # filter step
    thresh = 0.001
    stats = {k: stats[k] for k in stats if stats[k]['text'] / totals['text'] > thresh}

    latex = ''
    latex += '\t\\hline\n'

    for ix, (k, v) in enumerate(sorted(stats.items(), key=lambda ks: ks[0])):
        latex += '\t'
        latex += k
        latex += ' & '

        p = 100 * v['text'] / totals['text']
        if v['text'] > 100_000:
            label = f"{v['text'] / 1_000_000:.2f} M"
        elif v['text'] > 100:
            label = f"{v['text'] / 1_000:.2f} K"
        else:
            label = f"{v['text']}"

        if p > double:
            latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{label} ({p:.2f}\\%)"

        latex += ' & '
        p = 100 * v['pairs'] / totals['pairs']
        if v['text'] > 100_000:
            label = f"{v['pairs'] / 1_000_000:.2f} M"
        elif v['text'] > 100:
            label = f"{v['pairs'] / 1_000:.2f} K"
        else:
            label = f"{v['pairs']}"

        if p > double:
            latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{label} ({p:.2f}\\%)"

        latex += '\\\\ \n'

    if totals['text'] > 100_000:
        t_label = f"{totals['text'] / 1_000_000:.2f} M"
    elif totals['text'] > 100:
        t_label = f"{totals['text'] / 1_000:.2f} K"
    else:
        t_label = f"{totals['text']}"

    if totals['pairs'] > 100_000:
        p_label = f"{totals['pairs'] / 1_000_000:.2f} M"
    elif totals['pairs'] > 100:
        p_label = f"{totals['pairs'] / 1_000:.2f} K"
    else:
        p_label = f"{totals['pairs']}"

    latex += '\t\\hline\n'
    latex += '\t'
    latex += 'Total'
    latex += ' & '
    latex += f"{t_label}"
    latex += ' & '
    latex += f"{p_label}"
    latex += '\\\\ \n'
    latex += '\t\\hline\n'

    print(latex)


if __name__ == '__main__':
    # facebook_data_stats()
    # gen_facebook_table()
    # facebook_histogram()

    # gen_chan_table()
    gen_twitter_table()
