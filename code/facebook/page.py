import sys
sys.path.append('code/')

from base.stream import Stream


class FBPage(Stream):

    def __init__(self, name, uid=None):
        super().__init__(name, domain='Facebook', uid=uid)


if __name__ == '__main__':
    from csv import DictReader

    pages = {}
    bf_path = '/Users/hsh28/PycharmProjects/BuzzFace/data/facebook-fact-check.csv'

    reader = DictReader(open(bf_path))
    for row in reader:

        page_id = int(row['account_id'])
        if page_id not in pages:  # and pages[page_id] is None:
            p = FBPage(row['Page'], uid=page_id)
            p.add_meta('news_category', row['Category'])
            pages[page_id] = p

    import pdb
    pdb.set_trace()
