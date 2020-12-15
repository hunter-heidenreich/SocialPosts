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