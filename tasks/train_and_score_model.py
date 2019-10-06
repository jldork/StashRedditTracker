import os
import pickle
import pandas as pd
from gsdmm import MovieGroupProcess
from helpers.timer import Timer




# Append Topic Scores from the model
def get_topic_model_scores(df,model, seen=True):
    """
    The Model returns an array of tuples like 
        [ (0,12.34%), (1,1.49%), (2,30.31%) ... ]
    
    So we have to translate these into columns for our dataframe
    """
    scores = model.load_document_topics() if seen else model[df['bow']]
    
    i = 0;
    # The index is in order, you can check with
    # set(range(len(df.index))) - set(df.index)
    for score in scores:
        for per_topic_score in score:
            topic_num = per_topic_score[0] + 1
            topic_prob = per_topic_score[1]
            df.loc[i,f'Topic: {topic_num}'] = topic_prob
        i+=1
    
    topic_columns = [col for col in df.columns if col.startswith("Topic")]
    df['Topic'] = df[topic_columns].idxmax(axis=1).str.lstrip('Topic: ').astype(int)
    
    return df
    

def run():
    # Get the Preprocessed Dataset
    df = pd.read_pickle('./data/tmp/preprocessed.pkl')
    
        
    with Timer('Train the LDA Model'):
        mgp = MovieGroupProcess(K=200, alpha=0.1, beta=0.5, n_iters=30)
            
        vocab_size = len(set(df['tokens'].sum()))
        df['Topics'] = mgp.fit(df['tokens'], vocab_size)
            
        with open('./models/GSDMM/gsdmm_model.pkl','wb') as modelfile:
            pickle.dump(mgp, modelfile)
    
    df.to_pickle('./data/tmp/scored.pkl')\
    
    print("\nSample")
    print(df.head(),"\n")

if __name__ == '__main__':
    run()
