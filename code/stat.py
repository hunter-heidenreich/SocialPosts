import re
import json

import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from collections import defaultdict, Counter
from glob import glob

from tqdm import tqdm

from transformers import RobertaTokenizer


def format_num(num):
    """
    Transforms an integer into a smaller
    symbol representation
    """
    if num > 1_000_000_000_000:
        return f"{num / 1_000_000_000_000:.1f} T"
    elif num > 1_000_000_000:
        return f"{num / 1_000_000_000:.1f} B"
    elif num > 1_000_000:
        return f"{num / 1_000_000:.1f} M"
    elif num > 1_000:
        return f"{num / 1_000:.1f} K"
    else:
        return f"{num}"


def twitter_data_stats():
    stats = {}

    authors = {
        'source': defaultdict(set),
        'voice': defaultdict(set)
    }
    for f in tqdm(glob('data/twitter/*/')):
        page = f.split('/')[-2]
        if page not in stats:
            stats[page] = defaultdict(int)

        if page == 'quote':
            with open(f + 'text_en.json') as ff:
                lines = ff.readlines()
                # stats[page]['text'] += len(lines)

                for line in lines:
                    dat = json.loads(line)

                    # if dat['originator'] not in stats:
                    #     stats[dat['originator']] = defaultdict(int)

                    stats[page]['text'] += 1

                    if dat['is_source']:
                        authors['source'][page].add(dat['id'])

                    # else:
                    #     stats[dat['originator']]['pairs'] += 1

                    # authors['voice'][dat['originator']].add(dat['user'])
                    authors['voice'][page].add(dat['user'])
        else:
            with open(f + 'text_en.json') as ff:
                lines = ff.readlines()
                stats[page]['text'] += len(lines)

                for line in lines:
                    dat = json.loads(line)

                    if dat['is_source']:
                        authors['source'][dat['user']].add(dat['id'])

                    authors['voice'][page].add(dat['user'])

        with open(f + 'pairs_en.json') as ff:
            stats[page]['pairs'] += len([line for line in ff.readlines() if line.strip()])

    totals = defaultdict(int)
    for vs in stats.values():
        for k, v in vs.items():
            totals[k] += v

    return stats, totals, authors


def facebook_data_stats():
    stats = {}
    srcs = defaultdict(int)
    for f in tqdm(glob('data/fb/*/')):
        page = f.split('/')[-2]
        stats[page] = defaultdict(int)

        with open(f + 'text_en.json') as ff:
            lines = ff.readlines()
            stats[page]['text'] += len(lines)

            for line in lines:
                dat = json.loads(line)
                if dat['is_source']:
                    srcs[page] += 1

        with open(f + 'pairs_en.json') as ff:
            stats[page]['pairs'] += len([line for line in ff.readlines() if line.strip()])

    totals = defaultdict(int)
    for vs in stats.values():
        for k, v in vs.items():
            totals[k] += v

    return stats, totals, srcs


def reddit_data_stats():
    stats = {}
    vox = defaultdict(set)

    for f in tqdm(glob('data/reddit/*/')):
        page = f.split('/')[-2]
        stats[page] = defaultdict(int)

        with open(f + 'text_en.json') as ff:
            lines = ff.readlines()
            stats[page]['text'] += len(lines)

            for line in lines:
                dat = json.loads(line)
                vox[page].add(dat['user'])
                stats[page]['srcs'] += 1 if dat['is_source'] else 0

        with open(f + 'pairs_en.json') as ff:
            stats[page]['pairs'] += len(ff.readlines())

    totals = defaultdict(int)
    for vs in stats.values():
        for k, v in vs.items():
            totals[k] += v

    return stats, totals, vox


def chan_data_stats():
    stats = {}
    vox = defaultdict(set)

    for f in tqdm(glob('data/4chan/*/')):
        page = f.split('/')[-2]
        stats[page] = defaultdict(int)

        with open(f + 'text_en.json') as ff:
            lines = ff.readlines()
            stats[page]['text'] += len(lines)

            for line in lines:
                dat = json.loads(line)
                vox[page].add(dat['user'])
                stats[page]['srcs'] += 1 if dat['is_source'] else 0

        with open(f + 'pairs_en.json') as ff:
            stats[page]['pairs'] += len(ff.readlines())

    totals = defaultdict(int)
    for vs in stats.values():
        for k, v in vs.items():
            totals[k] += v

    return stats, totals, vox


def gen_twitter_table():
    stats, totals, authors = twitter_data_stats()

    # filter step
    thresh = 1.0  # 2.0
    double = 5
    # stats = {k: stats[k] for k in stats if stats[k]['text'] / totals['text'] > thresh}

    latex = ''
    latex += '\t\\hline\n'

    total_srcs = set()
    u_auths = set()
    for ix, (k, v) in enumerate(sorted(stats.items(), key=lambda ks: ks[0])):
        key = str(k)

        total_srcs |= authors['source'][key]
        u_auths |= authors['voice'][key]

        p = 100 * v['text'] / totals['text']
        if p > thresh:
            latex += '\t'
            latex += k.replace('_', '\\_')

            # Tally source tweets
            latex += ' & '
            latex += format_num(len(authors['source'][key]))

            # Tally unique posts
            latex += ' & '
            label = format_num(v['text'])

            if p > double:
                latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
            else:
                latex += f"{label} ({p:.2f}\\%)"

            # Tally conversational pairs
            latex += ' & '
            p = 100 * v['pairs'] / totals['pairs']
            label = format_num(v['pairs'])

            if p > double:
                latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
            else:
                latex += f"{label} ({p:.2f}\\%)"

            # Unique voices
            latex += ' & '
            latex += format_num(len(authors['voice'][key]))

            latex += ' \\\\ \n'

    latex += '\t\\hline\n'
    latex += '\t'
    latex += 'Total'

    # Tally source tweets
    latex += ' & '
    latex += format_num(len(total_srcs))

    # Tally unique posts
    latex += ' & '
    latex += format_num(totals['text'])

    # Tally conversational pairs
    latex += ' & '
    latex += format_num(totals['pairs'])

    # Unique voices
    latex += ' & '
    latex += format_num(len(u_auths))

    latex += '\\\\ \n'
    latex += '\t\\hline\n'

    print(latex)


def gen_facebook_table():
    stats, totals, srcs = facebook_data_stats()

    # filter step
    thresh = 0.01
    double = 5
    stats = {k: stats[k] for k in stats if stats[k]['text'] / totals['text'] > thresh}

    latex = ''
    latex += '\t\\hline\n'

    for ix, (k, v) in enumerate(sorted(stats.items(), key=lambda ks: ks[0])):
        key = str(k)

        latex += '\t'
        latex += k.replace('_', ' ').replace('%', '\\%')

        # Tally source tweets
        latex += ' & '
        latex += format_num(srcs[key])

        # Tally unique posts
        latex += ' & '
        p = 100 * v['text'] / totals['text']
        label = format_num(v['text'])

        if p > double:
            latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{label} ({p:.2f}\\%)"

        # Tally conversational pairs
        latex += ' & '
        p = 100 * v['pairs'] / totals['pairs']
        label = format_num(v['pairs'])

        if p > double:
            latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{label} ({p:.2f}\\%)"

        # Unique voices
        latex += ' & '
        latex += 'Unk.'

        latex += ' \\\\ \n'

    latex += '\t\\hline\n'
    latex += '\t'
    latex += 'Total'

    # Tally source tweets
    latex += ' & '
    latex += format_num(sum(srcs.values()))

    # Tally unique posts
    latex += ' & '
    latex += format_num(totals['text'])

    # Tally conversational pairs
    latex += ' & '
    latex += format_num(totals['pairs'])

    # Unique voices
    latex += ' & '
    latex += 'Unk.'

    latex += '\\\\ \n'
    latex += '\t\\hline\n'

    print(latex)


def gen_reddit_table():
    stats, totals, vox = reddit_data_stats()

    # filter step
    thresh = 1
    double = 5
    # stats = {k: stats[k] for k in stats if stats[k]['text'] / totals['text'] > thresh}

    latex = ''
    latex += '\t\\hline\n'

    vox_cnt = set()
    for ix, (k, v) in enumerate(sorted(stats.items(), key=lambda ks: ks[0])):
        key = str(k)
        vox_cnt |= vox[key]

        p = 100 * v['text'] / totals['text']
        if p < thresh:
            continue

        latex += '\t'
        latex += k

        # Tally source tweets
        latex += ' & '
        latex += format_num(v['srcs'])

        # Tally unique posts
        latex += ' & '
        label = format_num(v['text'])
        if p > double:
            latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{label} ({p:.2f}\\%)"

        # Tally conversational pairs
        latex += ' & '
        p = 100 * v['pairs'] / totals['pairs']
        label = format_num(v['pairs'])

        if p > double:
            latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{label} ({p:.2f}\\%)"

        # Unique voices
        latex += ' & '
        latex += f'{format_num(len(vox[key]))}'

        latex += ' \\\\ \n'

    latex += '\t\\hline\n'
    latex += '\t'
    latex += 'Total'

    # Tally source tweets
    latex += ' & '
    latex += format_num(totals['srcs'])

    # Tally unique posts
    latex += ' & '
    latex += format_num(totals['text'])

    # Tally conversational pairs
    latex += ' & '
    latex += format_num(totals['pairs'])

    # Unique voices
    latex += ' & '
    latex += f'{format_num(len(vox_cnt))}'

    latex += '\\\\ \n'
    latex += '\t\\hline\n'

    print(latex)


def gen_chan_table():
    stats, totals, vox = chan_data_stats()

    # filter step
    thresh = 0.0
    double = 5
    stats = {k: stats[k] for k in stats if stats[k]['text'] / totals['text'] > thresh}

    latex = ''
    latex += '\t\\hline\n'

    vox_lower = set()
    for ix, (k, v) in enumerate(sorted(stats.items(), key=lambda ks: ks[0])):
        key = str(k)

        latex += '\t'
        latex += k

        # Tally source tweets
        latex += ' & '
        latex += format_num(v['srcs'])

        # Tally unique posts
        latex += ' & '
        p = 100 * v['text'] / totals['text']
        label = format_num(v['text'])

        if p > double:
            latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{label} ({p:.2f}\\%)"

        # Tally conversational pairs
        latex += ' & '
        p = 100 * v['pairs'] / totals['pairs']
        label = format_num(v['pairs'])

        if p > double:
            latex += '\\textbf{' + f"{label} ({p:.2f}\\%)" + '}'
        else:
            latex += f"{label} ({p:.2f}\\%)"

        # Unique voices
        latex += ' & '
        latex += f'$\\Omega$({format_num(len(vox[key]))})'
        vox_lower |= vox[key]

        latex += ' \\\\ \n'

    latex += '\t\\hline\n'
    latex += '\t'
    latex += 'Total'

    # Tally source tweets
    latex += ' & '
    latex += format_num(totals['srcs'])

    # Tally unique posts
    latex += ' & '
    latex += format_num(totals['text'])

    # Tally conversational pairs
    latex += ' & '
    latex += format_num(totals['pairs'])

    # Unique voices
    latex += ' & '
    latex += f'$\\Omega$({format_num(len(vox_lower))})'

    latex += '\\\\ \n'
    latex += '\t\\hline\n'

    print(latex)


def token_cnt(plat):
    key = {
        'Facebook': 'fb',
        'Twitter': 'twitter',
        '4Chan': '4chan',
        'Reddit': 'reddit'
    }[plat]

    tokens = Counter()
    # for f in tqdm(glob(f'data/{key}/*/text.json')):
    for f in tqdm(glob(f'data/{key}/*/text_en.json')):
        # print(f)
        with open(f) as fp:
            for line in fp.readlines():
                tokens.update(re.split('\s+', json.loads(line)['text']))

    print(f'{plat} & {format_num(sum(tokens.values()))} & {format_num(len(tokens.keys()))} \\\\')


def chan_anon_table():
    anon = defaultdict(int)
    cnt = defaultdict(int)

    tot_anon = 0
    tot_cnt = 0
    for f in glob('data/4chan/*/'):
        page = f.split('/')[-2]
        print(page)
        with open(f + 'text_en.json') as ff:
            for line in tqdm(ff.readlines()):
                dat = json.loads(line)
                if dat['user'] == 'Anonymous':
                    anon[page] += 1
                    tot_anon += 1

                cnt[page] += 1
                tot_cnt += 1

    print('\\hline')
    for page in sorted(cnt.keys()):

        rat = anon[page] / cnt[page]
        print(f'{page} & {100 * rat:.2f}\\% & {100 * (1-rat):.2f}\\% \\\\')

    print('\\hline')
    rat = tot_anon / tot_cnt
    print(f'Total & {100 * rat:.2f}\\% & {100 * (1 - rat):.2f}\\% \\\\')
    print('\\hline')


def tokenizer_size(plat, model, force=False, log=False):
    if model == 'roberta':
        tok = RobertaTokenizer.from_pretrained('roberta-base')
        thresh = 256
    else:
        raise ValueError

    sizes = []
    try:
        if force:
            raise FileNotFoundError

        sizes = json.load(open(f'data/{plat}_{model}_sizes.json'))
    except FileNotFoundError:
        for f in tqdm(glob(f'data/{plat}/*/text_en.json')):
            with open(f) as fp:
                for line in fp.readlines():
                    sizes.append(len(tok.tokenize(json.loads(line)['text'])))
        json.dump(sizes, open(f'data/{plat}_{model}_sizes.json', 'w+'))

    print(f'{np.average(sizes):.2f} $\\pm$ {np.std(sizes):.2f} & {100 * len([s for s in sizes if s <= thresh]) / len(sizes):.2f}\\% \\\\')

    if log:
        sizes = [np.log10(s) if s else s for s in sizes]

    sns.distplot(sizes)
    plt.title(f'{"log-spaced " if log else ""}{model} sub-word length of post ({plat})')
    # plt.show()
    plt.savefig(f'{model}_{plat}_{"log" if log else ""}size.png')


if __name__ == '__main__':
    # gen_twitter_table()
    # gen_facebook_table()
    # gen_reddit_table()
    # gen_chan_table()

    # token_cnt('Twitter')
    # token_cnt('Facebook')
    # token_cnt('Reddit')
    # token_cnt('4Chan')

    # chan_anon_table()

    # p = 'twitter'
    # p = 'fb'
    p = 'reddit'
    # p = '4chan'

    m = 'roberta'
    tokenizer_size(p, m, log=True, force=True)
    tokenizer_size(p, m, log=False)
