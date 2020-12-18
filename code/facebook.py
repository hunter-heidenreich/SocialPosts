import json

from datetime import datetime
from glob import glob
from tqdm import tqdm

from post import UniversalPost
from board import Board
from dataset import ConversationalDataset


class FBPost(UniversalPost):

    """
    Facebook post object
    """

    # List of keys that may contain text data for a Facebook post
    TEXT = ['name', 'message', 'story', 'description', 'caption']

    def __init__(self, **args):
        super(FBPost, self).__init__(**args)

        if type(self.created_at) == str:
            self._string_to_creation(self.created_at)

    def _string_to_creation(self, x):
        self.created_at = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S+0000')

    @staticmethod
    def parse_facebook_datestr(datestr):
        return datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S+0000')


class FBPages(ConversationalDataset):

    CACHE_PATH = 'Facebook'

    def load_batch(self, skip_cached=True):
        # gather all page names
        pagenames = set()

        for f in glob(f'{ConversationalDataset.DATA_ROOT}BuzzFace/data*/*/'):
            pgname = f.split('/')[-2]
            pagenames.add(pgname)

        for f in glob(f'{ConversationalDataset.DATA_ROOT}Outlets/data*/*/'):
            pgname = f.split('/')[-2]
            pagenames.add(pgname)

        for pagename in pagenames:
            if skip_cached and glob(f'{ConversationalDataset.DATA_ROOT}conversations/{FBPages.CACHE_PATH}/{pagename}*.json'):
                print(f'Skipping cached page: {pagename}')
                continue

            print(f'Parsing page: {pagename}')
            board = FBPages.load_raw_page(pagename)
            self._boards[board.board_id] = board
            self.cache()

            del self._boards[board.board_id]

    @staticmethod
    def load_raw_post(data, post_id, board_id=None):
        post_cons = {
            'post_id':  post_id,
            'platform': 'Facebook',
        }

        if board_id:
            post_cons['board_id'] = board_id
            post_cons['author'] = board_id

        ignore_keys = {
            'caption', 'id', 'link', 'name', 'picture',
            'shares', 'updated_time', 'replies', 'story',
            'source', 'type', 'first_party', 'place'
        }
        for key, value in data.items():
            if key in ignore_keys:
                continue

            if not value:
                continue

            if key == 'created_time':
                post_cons['created_at'] = FBPost.parse_facebook_datestr(value)
            elif key == 'description':
                post_cons['text'] = (post_cons['text'] if 'text' in post_cons else '') + value
            elif key == 'message':
                post_cons['text'] = (post_cons['text'] if 'text' in post_cons else '') + value
            else:
                print(f'Unrecognized key in FB raw post: {key} --> {value}')
                import pdb
                pdb.set_trace()

        return FBPost(**post_cons)

    @staticmethod
    def load_raw_comments(data, in_reply_to=None, board_id=None):
        out = []

        if not data:
            return out

        ignore_keys = {
            'response'
        }

        if type(data) == dict:
            data = data['data']

        for comment in data:
            post_cons = {
                'platform': 'Facebook'
            }

            if in_reply_to:
                post_cons['reply_to'] = {in_reply_to}

            if board_id:
                post_cons['board_id'] = board_id

            try:
                for key, value in comment.items():
                    if key in ignore_keys:
                        continue

                    if not value:
                        continue

                    if key == 'id':
                        post_cons['post_id'] = value
                    elif key == 'message':
                        post_cons['text'] = value
                    elif key == 'created_time':
                        post_cons['created_at'] = FBPost.parse_facebook_datestr(value)
                    elif key == 'from':
                        post_cons['author'] = value['name']
                    elif key == 'userID':
                        if 'author' not in post_cons:
                            post_cons['author'] = value
                    else:
                        print(f'Unrecognized key in FB raw comment: {key} --> {value}')
                        import pdb
                        pdb.set_trace()
            except AttributeError:
                import pdb
                pdb.set_trace()

            out.append(FBPost(**post_cons))

        return out

    @staticmethod
    def load_raw_replies(data, in_reply_to=None, board_id=None):
        out = []

        if not data:
            return out

        ignore_keys = {
            'response'
        }

        if type(data) == dict:
            data = data['data']

        for comment in data:
            post_cons = {
                'platform': 'Facebook'
            }

            if in_reply_to:
                post_cons['reply_to'] = {in_reply_to}

            if board_id:
                post_cons['board_id'] = board_id

            for key, value in comment.items():
                if key in ignore_keys:
                    continue

                if not value:
                    continue

                if key == 'id':
                    post_cons['post_id'] = value
                elif key == 'message':
                    post_cons['text'] = value
                elif key == 'created_time':
                    post_cons['created_at'] = FBPost.parse_facebook_datestr(value)
                elif key == 'from':
                    post_cons['author'] = value['name']
                elif key == 'userID':
                    if 'author' not in post_cons:
                        post_cons['author'] = value
                elif key == 'replies':
                    continue
                else:
                    print(f'Unrecognized key in FB raw reply: {key} --> {value}')
                    import pdb
                    pdb.set_trace()

            if 'replies' in comment and comment['replies']:
                out.extend(FBPages.load_raw_replies(comment['replies'], board_id=board_id, in_reply_to=post_cons['post_id']))

            out.append(FBPost(**post_cons))
        return out

    @staticmethod
    def load_raw_scrape(data, in_reply_to=None, board_id=None):
        out = []

        if not out:
            return out

        ignore_keys = {

        }

        for key, value in data.items():
            if key in ignore_keys:
                continue

            if not value:
                continue

            print(f'Unrecognized key in FB raw scrape: {key} --> {value}')
            import pdb
            pdb.set_trace()

        return out

    @staticmethod
    def load_raw_page(pagename):
        page = Board(pagename.replace('_', ' ').replace('%', '\\%'))

        # for post_path in glob(f'{page_path}/*'):
        paths = glob(f'{ConversationalDataset.DATA_ROOT}BuzzFace/data*/{pagename}/*') + \
                glob(f'{ConversationalDataset.DATA_ROOT}Outlets/data*/{pagename}/*')
        for post_path in tqdm(paths):
            pid = int(post_path.split('/')[-1])

            for f in glob(f'{post_path}/*.json'):
                if 'post' in f:
                    try:
                        page.add_post(FBPages.load_raw_post(json.load(open(f)), pid, board_id=pagename))
                    except json.JSONDecodeError:
                        continue
                elif 'comments' in f:
                    try:
                        for x in FBPages.load_raw_comments(json.load(open(f)), in_reply_to=pid, board_id=pagename):
                            page.add_post(x)
                    except json.JSONDecodeError:
                        continue
                elif 'attach' in f:
                    pass
                elif 'react' in f:
                    pass
                elif 'replies' in f:
                    try:
                        for x in FBPages.load_raw_replies(json.load(open(f)), in_reply_to=pid, board_id=pagename):
                            page.add_post(x)
                    except json.JSONDecodeError:
                        # File is corrupt, skip
                        pass
                elif 'scrape' in f:
                    try:
                        for x in FBPages.load_raw_scrape(json.load(open(f)), in_reply_to=pid, board_id=pagename):
                            page.add_post(x)
                    except json.JSONDecodeError:
                        continue
                else:
                    print(f)
                    import pdb
                    pdb.set_trace()

        return page

    def cache(self):
        self.dump_conversation(filepath=FBPages.CACHE_PATH)

    def load_cache(self):
        self.load_conversation(filepath=FBPages.CACHE_PATH, board_cons=Board, post_cons=FBPost)


if __name__ == '__main__':
    dataset = FBPages()
    dataset.load_batch()
