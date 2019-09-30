import pandas as pd
from datetime import datetime
from helper.timer import Timer

def merge_parent_and_child_comments(submissions:pd.DataFrame, comments:pd.DataFrame):
    just_comments = comments[['id','time','body','parent_id', 'subreddit']].copy()
    rows_with_no_parent = just_comments[just_comments['parent_id'].apply(lambda x: type(x)) != str].index
    just_comments.drop(rows_with_no_parent, inplace=True, axis=0)
    just_comments['parent_id'] = just_comments['parent_id'].apply(lambda x: x.split('_')[1])

    parent_comments = just_comments.copy()
    parent_comments.drop(['parent_id', 'time', 'subreddit'], inplace=True, axis=1)
    parent_comments.rename(columns={"id": "parent_id"}, inplace=True)

    merged_comments = just_comments.merge(parent_comments, on='parent_id', how='left')
    merged_comments.columns = ['id','time','comment_text','parent_id','subreddit','parent_text']

    subs = submissions[['id','title','text']].copy()
    subs.rename(columns={'id':'parent_id'}, inplace=True)
    merged_comments = merged_comments.merge(subs, on='parent_id', how='left')

    merged_comments.fillna('', inplace=True)
    merged_comments['merged_text'] = merged_comments['comment_text'] + ' ' \
    + merged_comments['parent_text'] + \
    ' ' + merged_comments['title'] + \
    ' ' + merged_comments['text']
    
    df = merged_comments[['id','time','subreddit','merged_text']]
    
    def safe_convert_to_numeric(unknown):
        try:
            return float(unknown)
        except:
            return str(unknown)
    
    df['time'] = df['time'].apply(safe_convert_to_numeric)
    df['time'] = df['time'].apply(datetime.fromtimestamp)
    return df.rename(columns={'merged_text':'text'})

def run():
    with Timer('Merging Comment and Post Data from Subreddits'):
        submissions = dict()
        comments = dict()
        
        for subreddit in ['stashinvest', 'personalfinance']:
            submissions[subreddit] = pd.read_csv('./data/{}/submissions.csv'.format(subreddit))
            submissions[subreddit].drop(submissions[subreddit].columns[0], inplace=True, axis=1)
            submissions[subreddit]['subreddit'] = subreddit 

            comments[subreddit] = pd.read_csv('./data/{}/comments.csv'.format(subreddit))
            comments[subreddit].drop(comments[subreddit].columns[0], inplace=True, axis=1)
            comments[subreddit]['subreddit'] = subreddit

        submissions = pd.concat(submissions.values())
        comments = pd.concat(comments.values())

        merged = merge_parent_and_child_comments(submissions, comments)
        merged.to_csv('./data/tmp/merged.csv')
    
if __name__=='__main__':
    run()