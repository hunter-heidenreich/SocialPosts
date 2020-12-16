# SocialPosts

SocialPosts is a Python package that provides a uniform interface
for representing
conversational threads on various social media platforms. 

The mission of this project/package is to enable research and study 
of conversational threads on social media.

## Core Objects

### Universal Post

UniversalPost (UP) is the universal base class that all 
other post objects inherit from. 
UPs have the following properties:
- `post_id (Any)`: A unique post identifier. Although it can be anything, it technically has to be a hashable type. This is the only _mandatory_ field when creating a post
- `text (str)`: The text of a post 
- `author (str)`: The username of the user that created the post (can be used for anonymization)
- `created_at (datetime.datetime)`: The creation time of a post as a Python datetime object
- `board_id (Any)` Unique identifier of the board/board-like entity that this post was created to
- `reply_to (set(Any))`: A set of the unique post_ids of the posts that this post is replying to
- `platform (str)`: The social media platform this post was posted to
- `lang (str)`: Language label for what language the post was written in

Any of these properties can be accessed via simple . (dot) operations:

```python
post = UniversalPost(-1)  # = UniversalPost(post_id=-1)
print(post.text)  # will simply print the text of the post
```

Functions supported by the UP interface include:
- `.to_json()`: Returns a Python dictionary of the UPs properties
- `.from_json(obj)`: Given a Python dictionary with a subset of the UPs properties, this function will load the properties into the object
- `.get_mentions()`: Returns a set of the usermentions identified in this post
- `.redact(redact_map)`: Given a map of terms to redact to their redaction counterpart, this function will redact all instances (including the author of the post). This is primarily used (and to be used) to anonymize posts.

### Universal Board

A general grouping of posts 
(which can be something like a SubReddit or a 4Chan board or even a Facebook page).
A board as the following properties:
- `board_id (Any)`: A unique identifier for the board
- `posts (dict(Any, UniversalPost))`: A dictionary mapping from UP post_id to UP object 
- `conversations (dict(Any, list(Any)))`:  Structured conversational pointers dictionary, mapping from conversation ID to a list of UP post_ids

Additionally, this interface has the following methods:
- `.chunk_conversations(force_refresh=False, min_path_len=2)`: Chunks a board's set of posts into conversations. These conversations can be easily stored as singular units on disk (and will be, if cached!). Additionally, the default minimum length is set to 2 to filter out singleton posts.
- `.load_conversations(data, post_cons)`: Loads a cached JSON form of a conversation unit
- `.filter_post(post)`: Returns a boolean based on language detection. If a post is reliably another language, then it should be filtered in language modeling contexts especially!
- `.add_post(post, check=True)`: Adds a post to the board with a boolean to determine whether language filtering will be applied
- `.remove_post(post_id)`: Given a post_id, deletes that post from the board
- `.generate_pairs()`: Returns a list of structured post-reply texts
- `.redact()`: Performs conversationally-scoped redaction of user mentions
- `.construct_conversations()`: Reconstructs all conversations from the set of posts included in this board
- `.build_convo_path(post)`: Recursively constructs a conversational post from this post to its conversational root within this board
- `.merge_board(board)`: Given another board (with an identical board_id), this function will absorb the second board's posts