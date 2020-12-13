import json
from glob import glob

from tqdm import tqdm

import gcld3

if __name__ == '__main__':
    t_lang = 'en'

    # platform = 'twitter'
    # platform = 'fb'
    platform = 'reddit'

    detector = gcld3.NNetLanguageIdentifier(min_num_bytes=0, max_num_bytes=1000)

    for f in glob(f'data/{platform}/*/text.json'):
        print(f)

        keep_ids = set()
        keep = []
        total = 0
        with open(f) as fp:
            for line in tqdm(fp.readlines()):
                total += 1
                post = json.loads(line)

                text = post['text']
                uid = post['id']

                if text:
                    res = detector.FindLanguage(text=text)
                    p_lang = res.language
                    reliable = res.is_reliable

                    # Skip, reliably belongs to another lang
                    if t_lang != p_lang and reliable:
                        continue

                keep_ids.add(uid)
                keep.append(json.dumps(post))

        print(f'Retained {len(keep_ids)}/{total} ({100 * len(keep_ids) / total:.2f}%)')

        with open(f.replace('text.json', f'text_{t_lang}.json'), 'w+') as fp:
            fp.write('\n'.join(keep))

        keep_pairs = []
        with open(f.replace('text.json', 'pairs.json')) as fp:
            for line in tqdm(fp.readlines()):
                p = json.loads(line)
                if p['post'] in keep_ids and p['reply'] in keep_ids:
                    keep_pairs.append(line)

        with open(f.replace('text.json', f'pairs_{t_lang}.json'), 'w+') as fp:
            fp.write(''.join(keep_pairs))
