import os
import pickle
import pandas as pd
import matplotlib.pyplot as plt
from helpers.timer import Timer
from gensim.models import CoherenceModel, HdpModel
from gensim.models.wrappers import LdaMallet
from gensim.corpora.dictionary import Dictionary

##############################
# Build and choose the Model #
##############################
def topic_count_selection( 
    dictionary:Dictionary, 
    corpus:list, tokenized_docs:list, test_range:tuple) -> tuple:
    """
    Function to measure LDA topic coherence for different numbers of topics
    
    Returns:
    -------
    lm_list : List of LDA topic models
    c_v : Coherence values corresponding to the LDA model with respective number of topics
    """
    c_v = []
    lm_list = []
    for num_topics in range(test_range[0], test_range[1]):
        lm = LdaMallet(
            '/Users/jamesleung/Workspace/Mallet/bin/mallet', 
            corpus=corpus, 
            num_topics=num_topics, 
            id2word=dictionary, iterations=1000)
        lm_list.append(lm)
        
        cm = CoherenceModel(model=lm, texts=tokenized_docs, dictionary=dictionary, coherence='c_v')
        c_v.append(cm.get_coherence())
    
    import pdb;pdb.set_trace()
    return lm_list, c_v


def get_corpus_and_dict(df, tokens_column):
    """
    A Corpus is an iterable collection of Documents that your model is trained on. 
    - e.g. all news articles since 2018
    
    Dictionary is the vocabulary found in your corpus
    - e.g. Merriam Webster's dictionary
    
    We represent these tokens/words in bag-of-words format to optimize processing.
    """
    dictionary = Dictionary(documents=df['tokens'])
    df['bow'] = df['tokens'].apply(dictionary.doc2bow)
    corpus=list(df['bow'])
    
    return df, corpus, dictionary


def plot_coherence(test_range, scores):
    """
    Convenience Function to Visualize Coherence Across different # of Topics 
    
    We want to choose the maximum coherence while minimizing the number of topics
    So you can think of it as an elbow plot. Here we're choosing the maximum
    """
    # Show graph
    x = range(test_range[0], test_range[1])
    plt.plot(x, scores)
    plt.xlabel("num_topics")
    plt.ylabel("Coherence score")
    plt.legend(("c_v"), loc='best')
    plt.title('Deciding number of topics via Topic Coherence Metric')
    return plt.gcf()

# Append Topic Scores from the model
def get_topic_model_scores(df,model, seen=True):
    """
    The Model returns an array of tuples like 
        [ (0,12.34%), (1,1.49%), (2,30.31%) ... ]
    
    So we have to translate these into columns for our dataframe
    """
    scores = df['bow'].apply(lambda x: model[x])
    topic_columns = [f"Topic: {i}" for i in range(model.get_topics().shape[0])]
    df[topic_columns] = pd.DataFrame(0, index=df.index, columns=topic_columns)
    
    for i in range(len(scores)):
        topic_evaluation = scores[i]
        for topic in topic_evaluation:
            df.loc[i,f'Topic: {topic[0]}'] = topic[1]

    df['Topic'] = df[topic_columns].idxmax(axis=1).str.lstrip('Topic: ').astype(int)
    
    return df
    

def run():
    # Get the Preprocessed Dataset
    prepared_df = pd.read_pickle('./data/tmp/preprocessed.pkl')
    df, corpus, dictionary = get_corpus_and_dict(prepared_df, 'tokens')

    with Timer('Train the LDA Model') as t, open('./models/HDP/hdp_model.pkl','wb') as modelfile:
        hdp = HdpModel(corpus, dictionary)
        df = get_topic_model_scores(df, hdp) 
        
        # Save the outputs
        df.to_pickle('./data/tmp/scored.pkl')
        pickle.dump(hdp, modelfile)
    
    print("\nSample")
    print(df.head(),"\n")

if __name__ == '__main__':
    run()
