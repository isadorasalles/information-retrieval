# You can (and must) freely edit this file (add libraries, functions and calls) to implement your query processor
import argparse
import nltk
from string import punctuation, digits
from queue import Queue, PriorityQueue
from threading import Lock, Thread
from heapq import heappop, heappush, heapify
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from collections import Counter

stopwords = nltk.corpus.stopwords.words('portuguese')
next_query = Queue()

MAX_THREADS = 4

mutex_print_stdout = Lock()

docid_to_url = {}
token_to_offsets = {}

def preprocess(text):
    tokens = nltk.RegexpTokenizer(r'\w+').tokenize(text.lower()) #tokenize and lowercase

    stemmer = nltk.SnowballStemmer('portuguese')
    stem_tokens = [stemmer.stem(token) for token in tokens if token not in stopwords] # remove stopwords and stemming
    return stem_tokens

def query_preprocess(queries):
    terms_used = set()
    for q in queries:
        prepro_q = preprocess(q)
        for t in prepro_q:
            terms_used.add(t)
        next_query.put((q.replace('\n', ''), prepro_q))
    return terms_used

def compute_idf(postings):
    return math.log((len_index+1)/len(postings)) # len(postings) = numero de documentos que a palavra acontece 

def compute_tf_idf(tf, postings):
    idf = compute_idf(postings)
    return tf*idf

def compute_bm25(tf, docid, postings):
    k1 = 1.2
    b = 0.75
    idf = compute_idf(postings)
    length = docid_to_url[docid][1] # tamanho da pagina
    return (tf * (k1 + 1)/(tf + k1 *(1 - b + b*(length/avg_doc_length))))*idf

def get_min_docid_from_pointers(pointers, query):
    # retorna o menor docid igual a todas as listas invertidas
    while(1):
        d = []
        for i, term in enumerate(query):
            if pointers[i] < len(postings[term]):
                d.append(postings[term][pointers[i]][0])
            else:
                return None
        if d.count(d[0]) == len(d): # todos os docids sao iguais entao retorna
            return d[0]
        else:
            for i, e in enumerate(d):
                if e == min(d): # se eh minimo e diferente dos outros anda com o ponteiro
                    pointers[i]+=1

def daat_thread(original, query, index, ranker):
    docs = []
    docs.clear()
    heapify(docs)
    new_query = []
    for term in query:
        if term not in postings:
            continue
        new_query.append(term)
    pointers = [0 for i in range(len(new_query))]
    
    while(1):
        docid = get_min_docid_from_pointers(pointers, new_query)
        if docid == None:
            break
        
        score = 0
        for i, term in enumerate(new_query):
            if pointers[i] < len(postings[term]):
                d, tf = postings[term][pointers[i]]
                if d == docid:
                    if ranker == 'TFIDF':
                        score += compute_tf_idf(tf, postings[term]) 
                    else:
                        score += compute_bm25(tf, docid, postings[term])
                    pointers[i] += 1
        heappush(docs, (-1*score, docid))
    

    results = []
    
    for i in range(min(len(docs), 10)):
        (score, docid) = heappop(docs)
        results.append({"URL": docid_to_url[docid][0], "Score": -1*score})
    
    mutex_print_stdout.acquire()
    to_print = {
        "Query": original, 
        "Results": results
    }
    print(json.dumps(to_print, ensure_ascii=False))
    results.clear()
    mutex_print_stdout.release()
   

def queries_scheduler(index, ranker):
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        while not next_query.empty():
            # pega a proxima consulta na fila
            query, query_p = next_query.get()

            # cria uma thread para processar a consulta atual
            executor.submit(daat_thread, query, query_p, index, ranker)

def get_postings(index, terms_used):
    global postings
    postings = {}
    with open(index, 'r') as ind:
        for term in terms_used:
            if term not in token_to_offsets or term in postings:
                continue
            ind.seek(int(token_to_offsets[term]))
            line = json.loads(ind.readline())
            postings[term] = line[term]
    

def main(path_queries, index, ranker):
    with open(path_queries, 'r') as f_queries:
        queries = f_queries.readlines()

    global num_queries 
    num_queries = len(queries)
    # pre processamento dos termos das consultas
    terms_used = query_preprocess(queries)
    queries.clear()
    
    with open("save_url_to_docid.txt", 'r') as f:
        urls = f.readlines()

    global avg_doc_length
    avg_doc_length = 0

    for url in urls:
        splitted = url.split(': ')
        avg_doc_length += int(splitted[2].replace('\n', ''))
        docid_to_url[int(splitted[0])] = (splitted[1], int(splitted[2].replace('\n', '')))
    urls.clear()

    global len_index
    len_index = len(docid_to_url)
    avg_doc_length /= len_index

    with open("save_token_offset.txt", 'r') as f:
        offsets = f.readlines()

    for of in offsets:
        splitted = of.split(': ')
        if splitted[0] in terms_used:
            token_to_offsets[splitted[0]] = int(splitted[1].replace('\n', ''))
    offsets.clear()

    # salva apenas listas invertidas de tokens que aparecem em alguma consulta
    get_postings(index, terms_used)
    terms_used.clear()
    token_to_offsets.clear()

    queries_scheduler(index, ranker)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        '-i',
        dest='index',
        required=True,
        type=str,
        help='path to an index file'
    )
    parser.add_argument(
        '-q',
        dest='queries',
        required=True,
        type=str,
        help='path to a file with the list of queries to process'
    )
    parser.add_argument(
        '-r',
        dest='ranker',
        required=True,
        type=str,
        help='ranking function'
    )

    args = parser.parse_args()

    main(args.queries, args.index, args.ranker)

    

