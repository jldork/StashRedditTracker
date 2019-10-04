import flair
import operator
import pandas as pd
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
    df['flair_confidence'] = df['flair_sentence'].apply(lambda x: x.labels[0].value) 

    return df.drop('flair_sentence', axis=1)


def run():
    df = pd.read_pickle('./data/tmp/scored.pkl')
    with Timer("Add Sentiment Analyses"):    
        with Timer('Append Vader Sentiment Scores'):
            df = get_vader_sentiment(df)

        with Timer('Append Flair Sentiment Scores'):
            df = get_flair_sentiment(df)

        df.to_pickle('./data/tmp/final.pkl')

        print("\nSample")
        print(df.head(),"\n")
    
if __name__ == '__main__':
    run()