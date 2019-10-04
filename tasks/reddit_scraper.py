import os
import sys
import praw
import pandas as pd
from helpers.timer import Timer
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


class Scraper:
    def __init__(self, connection):
        self.conn = connection
        
    def get_reddit_data(self, subreddit: str) -> tuple:
        submissions = []
        comments = []

        for submission in self.conn.subreddit(subreddit).top(limit=None):
            # print(f"Submission {len(submissions)}: {submission.title}")
            submissions.append(submission_to_dict(submission))

            # Convenience function to check for has_not_fully_loaded_comment
            has_not_fully_loaded_comment = lambda comment: isinstance(comment, praw.models.MoreComments)
            
            post_comments = submission.comments.list()
            while any(map(has_not_fully_loaded_comment, post_comments)):
                sleep(1)
                submission.comments.replace_more(limit=None)
                post_comments = submission.comments.list()

            comments += map(comment_to_dict, post_comments)

        return pd.DataFrame(submissions), pd.DataFrame(comments)

    def write_to_csv(self, subreddit: str, directory:str = 'data') -> None:
        submissions, comments = self.get_reddit_data(subreddit)

        submissions.to_csv(f"{directory}/{subreddit}/submissions.csv".format(subreddit))
        comments.to_csv(f"{directory}/{subreddit}/comments.csv".format(subreddit))


if __name__ == "__main__":
    connection = praw.Reddit(
            client_id=os.getenv("CLIENT_ID"),
            client_secret=os.getenv("CLIENT_SECRET"),
            user_agent=os.getenv("USER_AGENT")
        )
    scraper = Scraper(connection)

    with Timer('Scraping Reddit Data and dumping to CSV'):
        scraper.write_to_csv('stashinvest')
    
    