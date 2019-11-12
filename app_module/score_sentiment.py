import flair
import operator
import argparse
import pandas as pd
from datetime import datetime
from helpers.timer import Timer
from nltk.sentiment.vader import SentimentIntensityAnalyzer

###########################
# Append Sentiment Scores #
###########################
def get_vader_sentiment(df):
    """
    VADER Sentiment Analysis. VADER (Valence Aware Dictionary and sEntiment Reasoner) is a lexicon and 
    rule-based sentiment analysis tool that is specifically attuned to sentiments expressed in social media, 
    and works well on texts from other domains.
    """
    sid = SentimentIntensityAnalyzer()
    
    df['vader'] = df['text'].apply(sid.polarity_scores)
    df['vader_neg'] = df['vader'].apply(operator.itemgetter('neg'))
    df['vader_neu'] = df['vader'].apply(operator.itemgetter('neu'))
    df['vader_pos'] = df['vader'].apply(operator.itemgetter('pos'))
    df['vader_compound'] = df['vader'].apply(operator.itemgetter('compound'))
    
    return df.drop('vader', axis = 1)



def get_flair_sentiment(df):
    """
    Flair sentiment model captures sentiments of misspelled words and out of vocabulary words. 
    This is powerful for social media like tweets and reddit, where the documents are not
    conventional.
    """
    flair_sentiment = flair.models.TextClassifier.load('en-sentiment')

    df['flair_sentence'] = df['text'].apply(flair.data.Sentence)
    df['flair_sentence'].apply(flair_sentiment.predict)
    df['flair_sentiment'] = df['flair_sentence'].apply(lambda x: x.labels[0].value)
    df['flair_confidence'] = df['flair_sentence'].apply(lambda x: x.labels[0].score) 

    return df.drop('flair_sentence', axis=1)


def run(env:str='edge'):
    df = pd.read_pickle('./data/tmp/scored.pkl')
    with Timer("Add Sentiment Analyses"):    
        with Timer('Append Vader Sentiment Scores'):
            df = get_vader_sentiment(df)

        # This takes approx. 2.5 hours... Not worth it at the moment
        # with Timer('Append Flair Sentiment Scores'):
        #     df = get_flair_sentiment(df)
        
        df = df[[
            'id','time','Topic: 1','Topic: 2',
            'Topic: 3','Topic: 4','Topic: 5','Topic: 6',
            'Topic','vader_neg','vader_neu','vader_pos',
            'vader_compound'
            ]]
        df.to_parquet(f's3://stash-de-source-{env}/source_social.db/reddit_scores/process_date={datetime.today().strftime("%Y-%m-%d")}/batch.parquet')

        print("\nSample")
        print(df.head(),"\n")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Score and upload to [env]')
    parser.add_argument(
        'env', type=str, help='Environment to create Hive tables and upload data:\n\n\tvalid values: edge | prod')
    args = parser.parse_args()
    run(args.env)
