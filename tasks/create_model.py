import pickle
import pandas as pd
from gsdmm import MovieGroupProcess

def run():
    with Timer('Creating GSDMM Model'):
        prepared_df = pd.read_csv('./data/tmp/preprocessed.csv')
        mgp_model = MovieGroupProcess(
            K=150, alpha=0.05, beta=0.5, n_iters=10
        )
        prepared_df['topic'] = mgp_model.fit(prepared_df['tokens'], len(dictionary.token2id))
        prepared_df.to_csv('./data/tmp/scored')
        
        with open('./models/gsdmm.pkl', 'wb') as model_file:
            pickle.dump(mgp_model, model_file)
        
if __name__ == '__main__':
    run()
