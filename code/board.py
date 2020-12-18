from tqdm import tqdm
from collections import defaultdict

import gcld3

# Language detection module with reliability flag
detector = gcld3.NNetLanguageIdentifier(min_num_bytes=0, max_num_bytes=1000)


class Board:

    """
    The general wrapper of a collection of posts.
    This class is titled `Board` but can generalize to:
    - An entire platform
    - A Reddit sub-reddit
    - A 4Chan board
    - A Facebook page / group (?)
    - A Twitter user timeline or list
    """

    RETAIN_LANGS = {'en', 'und'}

    def __init__(self, board_id):
        # unique board id (or page or originator)
        self._board_id = board_id

        # dictionary mapping from post_id to Post object
        self._posts = {}

        # Given a post_id, maps to its conversation id
        self._pid_to_convo_id = {}

        # Given a conversation id, maps to a set of associated post_ids
        self._convo_id_to_pids = defaultdict(set)

        # An object to cache the conversations when a board has been chunked
        self._conversations = {}

    @property
    def board_id(self):
        return self._board_id

    @property
    def posts(self):
        return self._posts

    def chunk_conversations(self, force_refresh=False, min_path_len=2):
        """
        Given a set of posts loaded into this board,
        this function chunks them into independent conversations.

        The minimum path length parameter is set to 2 by default
        to filter out singleton posts.
        If singletons are desired, change this to 1 or 0.
        """
        if self._conversations and not force_refresh:
            return

        self._conversations = {
            convo_id: [self.posts[pid].to_json() for pid in pids]
            for convo_id, pids in tqdm(self._convo_id_to_pids.items())
            if len(pids) >= min_path_len
        }

    @property
    def conversations(self):
        if not self._conversations:
            self.chunk_conversations()

        return self._conversations

    def load_conversations(self, data, post_cons):
        """
        Ingests raw conversation data,
        read from JSON files
        """
        self._conversations = {}
        for convo_id, posts in data.items():
            for post in posts:
                p = post_cons(post_id=post['post_id'])
                p.from_json(post)

                self.add_post(p, check=False)
                self._pid_to_convo_id[p.post_id] = convo_id
                self._convo_id_to_pids[convo_id].add(p.post_id)

    def delete_conversation(self, convo_id):
        for pid in self._convo_id_to_pids[convo_id]:
            self.remove_post(pid)
            del self._pid_to_convo_id[pid]

        del self._convo_id_to_pids[convo_id]

    def prune_singletons(self, dt, thresh):
        to_delete = set()
        for convo_id, pids in self._convo_id_to_pids.items():
            if len(pids) == 1:
                if (dt - self.posts[convo_id].created_at).days < thresh:
                    continue

                to_delete.add(convo_id)

        for convo_id in to_delete:
            del self._posts[convo_id]
            del self._convo_id_to_pids[convo_id]
            del self._pid_to_convo_id[convo_id]

    @staticmethod
    def filter_post(post):
        """
        Filters a post based on language detection
        of the text
        """
        res = detector.FindLanguage(text=post.text)

        if res.language not in Board.RETAIN_LANGS and res.is_reliable:
            return True

        post.lang = res.language if res.is_reliable else 'und'

        return False

    def add_post(self, post, check=True):
        """
        Adds a post to the board
        """
        # No language, check and set
        if check and not post.lang and self.filter_post(post):
            return
        elif check and post.lang and post.lang != 'en':
            return

        post.board = self._board_id
        self._posts[post.__hash__()] = post

    def remove_post(self, pid):
        """
        Removes a post from a board
        """
        if pid in self._posts:
            del self._posts[pid]
            self._conversations = None
        else:
            raise KeyError

    def generate_pairs(self):
        """
        Gemerates a list of structured
        post-reply pairs
        """
        pairs = []
        for _, post in self.posts.items():
            pairs.extend([{
                'post': self.posts[rid].text,
                'reply': post.text
            } for rid in post.reply_to if rid in self.posts])

        return pairs

    def redact(self):
        """
        Performs a conversationally-scoped redaction of user mentions
        """
        names_by_source = defaultdict(set)

        # gather name references
        print(f'Collecting user mentions / names')
        for post in tqdm(self._posts.values()):
            names_by_source[self._pid_to_convo_id[post.post_id]] |= post.get_mentions()

        # create maps
        name_map = {
            convo_id: {name: f'USER{ix}' for ix, name in enumerate(pids)} for convo_id, pids in names_by_source.items()
        }

        # redact with map
        print('REDACTION')
        for post in tqdm(self._posts.values()):
            mx = name_map[self._pid_to_convo_id[post.post_id]]
            post.redact(mx)

        return dict(name_map)

    def construct_conversations(self, full_rebuild=True):
        """
        Reconstructs raw conversational trees
        """
        if full_rebuild:
            self._pid_to_convo_id = {}
            self._convo_id_to_pids = defaultdict(set)

        self._conversations = None

        for pid, post in tqdm(self._posts.items()):
            if pid not in self._pid_to_convo_id:
                if post.platform == '4Chan':
                    # 4Chan data has weird cyclic issues at times where posts
                    # refer to themselves or to posts that have happened in the future
                    # (I found at least 2 examples of lists of prime numbers?)
                    post.reply_to = {rid for rid in post.reply_to if rid < pid}

                self.build_convo_path(post)

    def build_convo_path(self, post):
        """
        For a post, follows its conversational thread pointers
        to the highest post we have loaded into memory
        """
        breadth = len(post.reply_to)
        pid = post.post_id

        if breadth == 0:
            # set conversation to itself
            convo_id = pid
        elif breadth == 1:
            rid = list(post.reply_to)[0]
            if rid in self.posts:
                if rid not in self._pid_to_convo_id:
                    if post.platform == '4Chan':
                        post.reply_to = {rid for rid in post.reply_to if rid < pid}
                    # recurse
                    self.build_convo_path(self.posts[rid])
                convo_id = self._pid_to_convo_id[rid]
            else:
                # if we don't have a parent,
                # assign this post to a new thread
                convo_id = pid
        else:
            if post.platform == '4Chan':
                post.reply_to = {rid for rid in post.reply_to if rid < pid}
            else:
                for rid in post.reply_to:
                    if rid >= post.post_id:
                        print(f'Bad IDs:  {post.post_id} --> {rid}')
                        import pdb
                        pdb.set_trace()

            avail = [rid for rid in post.reply_to if rid in self.posts]
            convo_id = min(avail) if avail else pid

            if convo_id not in self._pid_to_convo_id and convo_id != pid:
                # recurse
                self.build_convo_path(self.posts[convo_id])

        self._pid_to_convo_id[pid] = convo_id
        self._convo_id_to_pids[convo_id].add(pid)

    def merge_board(self, board):
        """
        Merges two boards together
        """
        assert self.board_id == board.board_id

        # absorb posts
        try:
            if 0.8 < len(board.posts) / len(self.posts) < 1.2:
                self._posts = {**self._posts, **board.posts}
            elif len(board.posts) < len(self.posts):
                for post in board.posts.values():
                    self._posts[post.post_id] = post
            else:
                for post in self.posts.values():
                    board.posts[post.post_id] = post
                self._posts = board.posts
        except ZeroDivisionError:
            self._posts = {**self._posts, **board.posts}

        # Reset other pointers
        self._pid_to_convo_id = {}
        self._convo_id_to_pids = defaultdict(set)
        self._conversations = {}
