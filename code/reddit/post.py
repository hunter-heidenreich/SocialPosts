import re
import json

import sys
sys.path.append('code/')

from base.post import Post


class RedditPost(Post):

    def __init__(self, uid):
        super().__init__(uid)

    def load_from_file(self, filename):
        raise NotImplementedError()

    @staticmethod
    def format_time(timestr):
        raise NotImplementedError()

    @staticmethod
    def load_comments_from_file(filename):
        comments = {}
        with open(filename) as inf:
            for line in inf.readlines():
                parsed = RedditPost.create_from_json(line)
                comments[parsed.__hash__()] = parsed

        return comments

    @staticmethod
    def create_from_json(json_str):
        """
        Re-creates an instance from a JSON string
        :param json_str: JSON string
        :return: new instance of RedditComment
        """
        data = json.loads(json_str)

        uid = str(data.get('id', ''))
        result = RedditPost(uid)

        # add meta features from data
        result.set_meta('parent_id', str(data.get('parent_id', '')))
        result.set_meta('title', str(data.get('title', '')))
        result.set_meta('controversiality', int(data.get('controversiality', 0)))
        result.set_meta('gilded', int(data.get('gilded', 0)))
        result.set_meta('edited', bool(data.get('edited', False)))
        result.set_meta('created_utc', float(data.get('created_utc', 0.0)))
        result.set_meta('downs', int(data.get('downs', 0)))
        result.set_meta('link_id', str(data.get('link_id', '')))
        result.set_meta('name', str(data.get('name', '')))
        result.set_meta('archived', bool(data.get('archived', False)))
        result.set_meta('created', float(data.get('created', 0.0)))
        result.set_meta('body_html', str(data.get('body_html', '')))
        result.set_meta('ups', int(data.get('ups', 0)))
        result.set_meta('body', str(data.get('body', '')))
        result.set_meta('user_reports', list(data.get('user_reports', [])))
        result.set_meta('mod_reports', list(data.get('mod_reports', [])))
        result.set_meta('author_name', str(data.get('author_name', '')))

        # update post text
        result.set_text((result.get_meta('title') + ' ' + result.get_meta('body')).strip())

        return result

    @staticmethod
    def reconstruct_threads_from_submission(comments):
        """
        Given a map of RedditPost instances from a single submission, it creates
        all paths and return a list of the threads
        :param comments: list of RedditPost
        """
        root, root_name = None, None

        # create an adjacency matrix
        adjacency_matrix = dict()
        for comment in comments.values():
            # is this the root?
            if comment.get_meta('parent_id') == '':
                assert root is None
                root = comment
                root_name = comment.get_meta('name')

            # find the parent and add parent->child entry
            parent_id = comment.get_meta('parent_id')
            child_id = comment.get_meta('name')

            if len(parent_id) > 0:
                if parent_id not in adjacency_matrix:
                    adjacency_matrix[parent_id] = dict()

                # check for "false" children - comments that only label the parent with
                # delta or rule violation
                rule_violation = comment.get_rule_violation()
                delta = comment.get_delta_awarded_bot()

                # only if parent didn't violate rules, add this edge to the matrix
                if rule_violation == 0 and not delta:
                    adjacency_matrix[parent_id][child_id] = comment

        # we must have a root
        assert root

        def _process_children(cur, name):
            if name not in adjacency_matrix:
                return cur

            adj = adjacency_matrix[name]
            for label, post in adj.items():
                comm = _process_children(post, label)
                cur._comments[comm.__hash__()] = comm

            return cur

        return _process_children(root, root_name)

    def get_rule_violation(self):
        """
        Returns an integer label if this comments is a meta-post describing the previous post
        as being removed for violating rules. Multiple labels do occur, so they are packed into
        a single (e.g., 1, 2 -> 12; 5, 2 -> 25)
        :return: integer or 0 if no rule violation is mentioned
        """
        body = self.get_meta('body')
        if "your comment has been removed" in body:
            pattern = re.compile("> Comment Rule (\d+)")
            # convert to an integer (e.g., 1, 2 -> 12; 5, 2 -> 25)

            result = RedditPost.create_rule_violation_label_from_str_set(set(pattern.findall(body)))
            return result

        return 0

    def get_rule_violation_author_name(self):
        """
        Finds the author of the deleted comment as mentioned in the comment; if no name
        is available, returns an empty string
        :return: author name or empty string
        """
        author_pattern = re.compile('(\S+), your comment has been removed:')
        found = author_pattern.findall(self.get_meta('body'))
        if len(found) == 1:
            return found[0]
        else:
            return ''

    @staticmethod
    def create_rule_violation_label_from_str_set(str_set):
        """
        Given a list of string integers ('violation rule' numbers), sorts them
        and return as a single integer; for example
        1, 2 -> 12
        or
        5, 2 -> 25
        :param str_set: set of strings
        :return: integer or 0 if empty list
        """
        str_value = ''.join(sorted(list(str_set)))
        return int(str_value) if str_value != '' else 0

    def get_delta_awarded_bot(self):
        return self.get_meta('body').startswith('Confirmed: 1 delta awarded to')

    def __hash__(self):
        return self._uid  # .__hash__()
