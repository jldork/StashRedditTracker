import praw
import pandas as pd
from time import sleep


def submission_to_dict(post: praw.models.Submission) -> dict:
    return {
        "id": post.id,
        "title": post.title,
        "time": post.created_utc,
        "author": post.author.name if post.author else None,
        "selfpost": post.is_self,
        "text": post.selftext,
        "link": post.url,
        "flair": post.link_flair_text,
        "num_comments": post.num_comments,
        "permalink": post.permalink,
        "upvotes": post.score,
        "upvote_ratio": post.upvote_ratio,
    }


def comment_to_dict(comment: praw.models.Comment) -> dict:
    return {
        "id": comment.id,
        "time": comment.created_utc,
        "author": comment.author.name if comment.author else None,
        "is_distinguished": comment.distinguished,
        "is_OP": comment.is_submitter,
        "body": comment.body,
        "submission_id": comment.link_id,
        "parent_id": comment.parent_id,
        "num_top_level_replies": len(comment.replies),
        "permalink": comment.permalink,
        "is_stickied": comment.stickied,
        "upvotes": comment.score,
    }


def get_reddit_data(conn: praw.Reddit, subreddit: str) -> tuple:
    submissions = []
    comments = []

    for submission in conn.subreddit(subreddit).top(limit=None):
        print(
            "Submission {num}: {title}".format(
                num=len(submissions), title=submission.title
            )
        )
        submissions.append(submission_to_dict(submission))

        # Now iterate over the comments
        post_comments = submission.comments.list()

        # Query API until we get all the comments
        while any(
            isinstance(comment, praw.models.MoreComments) for comment in post_comments
        ):
            sleep(1)
            submission.comments.replace_more(limit=None)
            post_comments = submission.comments.list()

        comments += [comment_to_dict(comment) for comment in post_comments]

    return pd.DataFrame(submissions), pd.DataFrame(comments)

