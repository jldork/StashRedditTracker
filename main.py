import os
import sys
import praw
from scrape import get_reddit_data

def main(subreddit:str) -> None:
    reddit = praw.Reddit(
        client_id=os.getenv('CLIENT_ID'), 
        client_secret=os.getenv('CLIENT_SECRET'),
        user_agent=os.getenv('USER_AGENT')
    )
    submissions, comments = get_reddit_data(reddit, subreddit)
    
    submissions.to_csv('data/{}/submissions.csv'.format(subreddit))
    comments.to_csv('data/{}/comments.csv'.format(subreddit))
    

if __name__ == "__main__":
    # Check the arguments
    if len(sys.argv) == 1:
        sys.exit('Subreddit not supplied')
    elif len(sys.argv) > 2:
        sys.exit('Too many arguments')
    else:
        sub = sys.argv[1]
    
    # Run the scraper
    main(sub)
        
    
    
    
