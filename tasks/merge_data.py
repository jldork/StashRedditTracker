import pandas as pd
from datetime import datetime
from helpers.timer import Timer

def merge_parent_and_child_comments(submissions:pd.DataFrame, comments:pd.DataFrame):
    # Merge comments with their parent comment
    just_comments = comments[['id','time','body','parent_id', 'upvotes']].copy()
    just_comments['parent_id'] = just_comments['parent_id'].apply(lambda x: x.split('_')[1])
    
    # Merge comments with their parent comment
    parent_comments = just_comments.copy()
    parent_comments.drop(['parent_id', 'time'], inplace=True, axis=1)
    parent_comments.rename(columns={"id": "parent_id"}, inplace=True)

    # Coalesce some fields
    merged_comments = just_comments.merge(parent_comments, on='parent_id', how='left')
    merged_comments['upvotes'] = merged_comments['upvotes_x'].combine_first(merged_comments['upvotes_y'])
    merged_comments.drop(['upvotes_x','upvotes_y'], axis=1, inplace=True)
    merged_comments.columns = ['id','time','comment_text','parent_id','parent_text','upvotes']

    # Combine Submission to a Top-level comment
    subs = submissions[['id','title','text','time', 'upvotes']].copy()
    subs.rename(columns={'id':'parent_id'}, inplace=True)
    merged_comments = merged_comments.merge(subs, on='parent_id', how='outer')
    
    # Coalesce the columns so we don't have null values.
    merged_comments['time'] = merged_comments['time_x'].combine_first(merged_comments['time_y'])
    merged_comments['upvotes'] = merged_comments['upvotes_x'].combine_first(merged_comments['upvotes_y']).astype(int).fillna(0)
    merged_comments['id'] = merged_comments['id'].combine_first(merged_comments['parent_id']).fillna('')
    merged_comments['orig_text'] = merged_comments['comment_text'].combine_first(merged_comments['parent_text']).fillna('')
    
    # Add all the text togethaaaa
    merged_comments['merged_text'] = merged_comments['comment_text'].fillna('') + ' ' + merged_comments['parent_text'].fillna('') + \
                                    ' ' + merged_comments['title'].fillna('') + ' ' + merged_comments['text'].fillna('')
    
    df = merged_comments[['id','time','orig_text','merged_text','upvotes']].copy()
    
    df.loc[:,'time'] = df.loc[:,'time'].apply(datetime.fromtimestamp)
    return df.rename(columns={'merged_text':'text'})

def run():
    with Timer('Merging Comment and Post Data from Subreddits'):
        submissions = pd.read_csv('./data/stashinvest/submissions.csv')
        submissions.drop(submissions.columns[0], inplace=True, axis=1)
        submissions.drop(submissions[submissions['author'] == 'stashofficial'].index, axis=0, inplace=True)
        
        comments = pd.read_csv('./data/stashinvest/comments.csv')
        comments.drop(comments.columns[0], inplace=True, axis=1)
        comments.drop(comments[comments['author'] == 'stashofficial'].index, axis=0, inplace=True)
        comments.drop(comments[comments['body']=='[deleted]'].index, axis=0, inplace=True)

        merged = merge_parent_and_child_comments(submissions, comments)
        merged.to_pickle('./data/tmp/merged.pkl')

        print("\nSample")
        print(merged.head(),"\n")
        
    
if __name__=='__main__':
    run()
