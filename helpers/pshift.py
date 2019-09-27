import math
import json
import requests
import itertools
import numpy as np
import time
import pandas as pd
from datetime import datetime, timedelta


def fire_away(uri):
    response = requests.get(uri)
    assert response.status_code == 200
    return json.loads(response.content)


def comments_to_dict(comments):
    return [
        {
            "id": comment.get("id"),
            "time": comment.get("created_utc"),
            "author": comment.get("author"),
            "is_OP": comment.get("is_submitter"),
            "body": comment.get("body"),
            "submission_id": comment.get("link_id"),
            "parent_id": comment.get("parent_id"),
            "permalink": comment.get("permalink"),
            "is_stickied": comment.get("stickied"),
            "upvotes": comment.get("score"),
        }
        for comment in comments
    ]


def submissions_to_dict(posts):
    return [
        {
            "id": post.get("id"),
            "title": post.get("title"),
            "time": post.get("created_utc"),
            "author": post.get("author"),
            "selfpost": post.get("is_self"),
            "text": post.get("selftext"),
            "link": post.get("url"),
            "num_comments": post.get("num_comments"),
            "permalink": post.get("permalink"),
            "upvotes": post.get("score"),
        }
        for post in posts
    ]


def make_request(uri, max_retries: int = 5):
    current_tries = 1

    while current_tries < max_retries:
        try:
            time.sleep(1)
            response = fire_away(uri)
            return response
        except:
            time.sleep(1)
            current_tries += 1

    return fire_away(uri)


def pull_posts_for(subreddit, start_at, end_at):
    SIZE = 500
    URI = r"https://api.pushshift.io/reddit/search/submission?subreddit={}&after={}&before={}&size={}"

    post_collections = submissions_to_dict(
        make_request(URI.format(subreddit, start_at, end_at, SIZE))["data"]
    )

    n = len(post_collections)
    while n == SIZE:
        last = post_collections[-1]
        new_start_at = last["time"] - (10)
        more_posts = submissions_to_dict(
            make_request(URI.format(subreddit, new_start_at, end_at, SIZE))["data"]
        )

        n = len(more_posts)
        post_collections.extend(more_posts)

    return post_collections


def pull_comments_for(post_id: str):
    SIZE = 1500
    URI = r"https://api.pushshift.io/reddit/search/comments?link_id={}"
    comment_collections = comments_to_dict(
        make_request(URI.format(post_id, SIZE))["data"]
    )
    return comment_collections


def give_me_intervals(start_at, number_of_days_per_interval=1):
    end_at = math.ceil(datetime.utcnow().timestamp())

    ## 1 day = 86400,
    period = 86400 * number_of_days_per_interval
    end = start_at + period
    yield (int(start_at), int(end))
    padding = 1
    while end <= end_at:
        start_at = end + padding
        end = (start_at - padding) + period
        yield int(start_at), int(end)


def get_reddit_data(subreddit: str) -> pd.DataFrame:
    start_at = math.floor((datetime.utcnow() - timedelta(days=365)).timestamp())

    posts = []
    comments = []

    for interval in give_me_intervals(start_at):
        posts.extend(pull_posts_for(subreddit, interval[0], interval[1]))
        time.sleep(0.500)

    for post in posts:
        comments.extend(pull_comments_for(post["id"]))
        print(post["title"], len(comments))
        time.sleep(0.500)

    return pd.DataFrame(posts), pd.DataFrame(comments)
