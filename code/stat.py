import json

from collections import defaultdict
from glob import glob

from tqdm import tqdm


def format_num(num):
    """
    Transforms an integer into a smaller
    symbol representation
    """
    if num > 1_000_000:
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
        if page != 'quote' and page not in stats:
            stats[page] = defaultdict(int)

        if page == 'quote':
            with open(f + 'text.json') as ff:
                lines = ff.readlines()
                # stats[page]['text'] += len(lines)

                for line in lines:
                    dat = json.loads(line)

                    if dat['originator'] not in stats:
                        stats[dat['originator']] = defaultdict(int)

                    stats[dat['originator']]['text'] += 1

                    if dat['is_source']:
                        authors['source'][dat['user']].add(dat['id'])
                    else:
                        stats[dat['originator']]['pairs'] += 1

                    authors['voice'][dat['originator']].add(dat['user'])
        else:
            with open(f + 'text.json') as ff:
                lines = ff.readlines()
                stats[page]['text'] += len(lines)

                for line in lines:
                    dat = json.loads(line)

                    if dat['is_source']:
                        authors['source'][dat['user']].add(dat['id'])

                    authors['voice'][page].add(dat['user'])

            with open(f + 'pairs.json') as ff:
                stats[page]['pairs'] += len(ff.readlines())

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

        with open(f + 'text.json') as ff:
            lines = ff.readlines()
            stats[page]['text'] += len(lines)

            for line in lines:
                dat = json.loads(line)
                if dat['is_source']:
                    srcs[page] += 1

        with open(f + 'pairs.json') as ff:
            stats[page]['pairs'] += len(ff.readlines())

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

        with open(f + 'text.json') as ff:
            lines = ff.readlines()
            stats[page]['text'] += len(lines)

            for line in lines:
                dat = json.loads(line)
                vox[page].add(dat['user'])
                stats[page]['srcs'] += 1 if dat['is_source'] else 0

        with open(f + 'pairs.json') as ff:
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

        with open(f + 'text.json') as ff:
            lines = ff.readlines()
            stats[page]['text'] += len(lines)

            for line in lines:
                dat = json.loads(line)
                vox[page].add(dat['user'])
                stats[page]['srcs'] += 1 if dat['is_source'] else 0

        with open(f + 'pairs.json') as ff:
            stats[page]['pairs'] += len(ff.readlines())

    totals = defaultdict(int)
    for vs in stats.values():
        for k, v in vs.items():
            totals[k] += v

    return stats, totals, vox


def gen_twitter_table():
    stats, totals, authors = twitter_data_stats()

    # filter step
    thresh = 0.005
    double = 5
    stats = {k: stats[k] for k in stats if stats[k]['text'] / totals['text'] > thresh}

    latex = ''
    latex += '\t\\hline\n'

    total_srcs = set()
    u_auths = set()
    for ix, (k, v) in enumerate(sorted(stats.items(), key=lambda ks: ks[0])):
        key = str(k)

        latex += '\t'
        latex += k.replace('_', '\\_')

        # Tally source tweets
        latex += ' & '
        latex += format_num(len(authors['source'][key]))
        total_srcs |= authors['source'][key]

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
        latex += format_num(len(authors['voice'][key]))
        u_auths |= authors['voice'][key]

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
    thresh = 0.0
    double = 5
    stats = {k: stats[k] for k in stats if stats[k]['text'] / totals['text'] > thresh}

    latex = ''
    latex += '\t\\hline\n'

    vox_cnt = set()
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
        latex += f'{format_num(len(vox[key]))}'
        vox_cnt |= vox[key]

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


if __name__ == '__main__':
    # gen_twitter_table()
    # gen_facebook_table()
    gen_reddit_table()
    # gen_chan_table()
