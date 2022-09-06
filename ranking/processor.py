import pyterrier as pt
import pandas as pd
import nltk
import xgboost as xgb
from sklearn.ensemble import RandomForestRegressor
import re
import argparse
## Para executar as submissões 4 e 5 instale o pyterrier_t5 
# from pyterrier_t5 import MonoT5ReRanker, DuoT5ReRanker
# monoT5 = MonoT5ReRanker()


def preprocessing_query(query):
    return nltk.RegexpTokenizer(r'\w+').tokenize(query.lower())

def process_queries(queries_path):
    df = pd.read_csv(queries_path, sep = ',', header=0, names=['qid', 'query'], dtype={'qid': object})
    queries_tokens = []
    for _, (_, query) in df.iterrows():
        q = preprocessing_query(query)
        queries_tokens.append(' '.join(q))
    df['query'] = queries_tokens
    return df

def process_qrels(rels_path):
    df = pd.read_csv(rels_path, sep=',', header=0, names=['qid', 'docno', 'label'], dtype={'qid': object, 'docno': object})
    return df

def split_train_validate(train_queries, train_qrels):
    topics = process_queries(train_queries)
    qrels = process_qrels(train_qrels) 
    topics_train = topics[:175]
    topics_val = topics[175:]
    return topics_train, topics_val, qrels

def sub_1(index, test_topics, qrels):
    ## CODIGO REFERENTE AO PIPELINE DA SUBMISSAO 1
    # Se quiser executar o GridSearch descomente o codigo abaixo
    ##################
    # print(pt.GridSearch(
    #     tf_idf,
    #     {
    #     tf_idf : {
    #         "c": [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1 ],
    #         "tf_idf.k_1":  [0.3, 0.6, 0.9, 1.2, 1.4, 1.6, 2]
    #     },
    #     train_topics,
    #     qrels,
    #     "ndcg_cut.100"
    # ))
    tf_idf = pt.BatchRetrieve(index, wmodel="TF_IDF", controls={'tf_idf.k_1': 1.2, 'c': 0.1}) % 100
    df = tf_idf.transform(test_topics)
    df.to_csv('tfidf_params.txt', columns=['qid', 'docno'], header=['QueryId', 'EntityId'], index=False) 

def sub_2(index, test_topics, qrels):
    ## CODIGO REFERENTE AO PIPELINE DA SUBMISSAO 2
    # print(pt.GridSearch(
    #     tf_idf,
    #     {
    #     tf_idf : {
    #         "c": [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1 ],
    #         "tf_idf.k_1":  [0.3, 0.6, 0.9, 1.2, 1.4, 1.6, 2]
    #     },
    #     # bo1 : {
    #     #     "fb_terms" : list(range(1, 12, 3)),
    #     #     "fb_docs" : list(range(2, 5, 1))
    #     # }
    #     },
    #     train_topics,
    #     qrels,
    #     "ndcg_cut.100"
    # ))
    tf_idf = pt.BatchRetrieve(index, wmodel="TF_IDF", controls={'tf_idf.k_1': 1.2, 'c': 0.1}) % 100
    bo1 = pt.rewrite.Bo1QueryExpansion(index, fb_terms=10, fb_docs=3)
    pipeline = tf_idf >> bo1 >> tf_idf
    df = pipeline.transform(test_topics)
    df.to_csv('pipeline_tfidf_bo1_tfidf_params.txt', columns=['qid', 'docno'], header=['QueryId', 'EntityId'], index=False) 

def sub_3(index, train_topics, val_topics, qrels, test_topics):
    ## CODIGO REFERENTE AO PIPELINE DA SUBMISSAO 3
    bm25 = pt.BatchRetrieve(index, wmodel="BM25", controls={'bm25.k_1': 1.2, 'c': 0.1, 'bm25.k_3': 0.5})
    fbr = pt.FeaturesBatchRetrieve(index, controls = {"wmodel": "BM25", 'bm25.k_1': 1.2, 'c': 0.1, 'bm25.k_3': 0.5}, features=["SAMPLE", "WMODEL:TF_IDF"]) 
    lmart_x = xgb.sklearn.XGBRanker(objective='rank:ndcg',
      learning_rate=0.1,
      gamma=1.0,
      min_child_weight=0.1,
      max_depth=6,
      random_state=42)
    print("test")
    lmart_x_pipe = fbr >> pt.ltr.apply_learned_model(lmart_x, form="ltr")
    lmart_x_pipe.fit(train_topics, qrels, val_topics, qrels)

    pipe2 = bm25 % 500 >> lmart_x_pipe
    
    # results = pt.Experiment([bm25, pipe2], val_topics, qrels, ["ndcg"], names=["TF_IDF Baseline", "LambdaMart"])
    
    df = pipe2.transform(test_topics)
    df = df.sort_values(by=['qid', 'rank'], ascending=True)
    df.groupby(by=["qid"]).head(100).to_csv('lmart_reorder.txt', columns=['qid', 'docno'], header=['QueryId', 'EntityId'], index=False) 

def sub_4(index, test_topics):
    ## CODIGO REFERENTE AO PIPELINE DA SUBMISSAO 4
    bm25 = pt.BatchRetrieve(index, wmodel="BM25", controls={'bm25.k_1': 1.2, 'c': 0.1, 'bm25.k_3': 0.5}) % 200
    kl = pt.rewrite.KLQueryExpansion(index, fb_terms=10, fb_docs=3)
    pipe = bm25 >> kl >> bm25
    mono_pipeline = pipe >> pt.text.get_text(index, 'text') >> monoT5 

    df = mono_pipeline.transform(test_topics)
    df_new = df.sort_values(by=['qid', 'rank'], ascending=True)
    df_new.groupby(by=["qid"]).head(100).to_csv('bm25_kl_monoT5_reorder.txt', columns=['qid', 'docno'], header=['QueryId', 'EntityId'], index=False) 

def sub_5(index, test_topics):
    ## CODIGO REFERENTE AO PIPELINE DA SUBMISSAO 5
    tfidf = pt.BatchRetrieve(index, wmodel="TF_IDF", controls={'tf_idf.k_1': 1.2, 'c': 0.1}, metadata=['docno', 'text', 'title', 'keywords']) % 500
    mono_pipeline = tfidf >> monoT5     

    df = mono_pipeline.transform(test_topics)
    df_new = df.sort_values(by=['qid', 'rank'], ascending=True)
    df_new.groupby(by=["qid"]).head(100).to_csv('tfidf_monoT5_fields_reorder.txt', columns=['qid', 'docno'], header=['QueryId', 'EntityId'], index=False) 


if __name__ == "__main__":
    # xgboost_model(index, train_topics, val_topics, qrels, test_topics)

    parser = argparse.ArgumentParser()

    parser.add_argument('-s', dest='submission_number', 
                        action='store', required=True, 
                        type=int, help='pyterrier indexing type')

    args = parser.parse_args()

    if not pt.started():
        pt.init()

    indexref = pt.IndexRef.of("./index/data.properties")
    index = pt.IndexFactory.of(indexref)

    train_topics, val_topics, qrels = split_train_validate('train_queries.csv', 'train_qrels.csv')
    test_topics = process_queries('test_queries.csv')
    topics = process_queries('train_queries.csv')

    if args.submission_number == 1:
        sub_1(index, test_topics, qrels)
    elif args.submission_number == 2:
        sub_2(index, test_topics, qrels)
    elif args.submission_number == 3:
        sub_3(index, train_topics, val_topics, qrels, test_topics)
    elif args.submission_number == 4:
        print("Precisa de GPU para executar. Para executar instale o pyterrier_t5.")
        sub_4(index, test_topics)
    elif args.submission_number == 5:
        print("Precisa de GPU para executar.Para executar instale o pyterrier_t5.")
        sub_5(index, test_topics)
    else:
        print("Escolha um número entre 1 e 5.")


## PARA EXECUTAR AS SUBMISSÕES 4 E 5 NO COLAB USE O LINK A SEGUIR
## https://colab.research.google.com/drive/1xCXgifKPZeFOqQ-ELk6cHawvtmntE-Ph?usp=sharing