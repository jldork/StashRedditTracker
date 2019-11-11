import re
import string
import pandas as pd
from gensim.utils import tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from helpers.timer import Timer


class Preprocessor:
    def __init__(self):
        self.lemmatizer = WordNetLemmatizer()
        self.stops = set(stopwords.words("english"))

    # Stemming takes a while, so let's parallelize this
    def lemmatizing(self, list_of_tokens):
        return [self.lemmatizer.lemmatize(token) for token in list_of_tokens]

    def remove_stopwords(self, text):
        pattern = re.compile(r"\b(" + r"|".join(self.stops) + r")\b\s*")
        return pattern.sub("", text)

    def preprocess_df(self, df, text_column):
        # Deleted reddit comments
        remove_deleted = lambda x: x.replace("[deleted]", "")
        # /u/usernames
        remove_usernames = lambda text: re.sub("/u/\w*", "", text)
        # https://link_to_website_that_would_count_as_token.com/messing/up/model
        remove_links = lambda x: re.sub("http[\:\#\&\=\?\_\-\/\.\w]+", "", x)
        # Remove punctuation
        remove_punctuation = lambda x: x.translate(
            str.maketrans("", "", string.punctuation)
        )

        df["tokens"] = (
            df[text_column]
            .apply(str.lower)
            .apply(remove_deleted)
            .apply(remove_usernames)
            .apply(remove_links)
            .apply(self.remove_stopwords)
            .apply(tokenize)
            .apply(self.lemmatizing)
        )

        return df


def run():
    raw_data = pd.read_pickle("./data/tmp/merged.pkl")

    with Timer("Preprocess Text"):
        processor = Preprocessor()
        prepared_df = processor.preprocess_df(df=raw_data, text_column="text")
        prepared_df.to_pickle("./data/tmp/preprocessed.pkl")
        
        print("\nSample")
        print(prepared_df.head(),"\n")


if __name__ == "__main__":
    run()
