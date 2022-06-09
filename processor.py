# You can (and must) freely edit this file (add libraries, functions and calls) to implement your query processor
import argparse
import nltk
from string import punctuation, digits
from queue import Queue, PriorityQueue
from threading import Lock, Thread
from heapq import heappop, heappush, heapify
import json
import math

stopwords = nltk.corpus.stopwords.words('portuguese')
next_query = Queue()
count_queries_processed = 0
mutex_count_queries = Lock()

MAX_THREADS = 100
num_threads = 0
mutex_count_threads = Lock()

mutex_print_stdout = Lock()

docid_to_url = {}
token_to_offsets = {}


def check_end_query_processing():
    mutex_count_queries.acquire()
    if count_queries_processed < num_queries:
        mutex_count_queries.release()
        return False
    mutex_count_queries.release()
    return True
    

def preprocess(text):
    remove_digits = str.maketrans('', '', digits)
    text_no_numbers = text.translate(remove_digits)
    tokens = nltk.word_tokenize(text_no_numbers.lower()) # lowercase
    tokens = [token.strip(punctuation) for token in tokens if token not in stopwords] # remove pontuacoes e stopwords
    tokens = [token for token in tokens if token != '']
    stemmer = nltk.stem.RSLPStemmer() 
    stem_tokens = [stemmer.stem(token) for token in tokens] # stemming
    return stem_tokens

def query_preprocess_thread(queries):
    for q in queries:
        prepro_q = preprocess(q)
        next_query.put((q, prepro_q))
    
    # acabou de preprocessar todas as consultas, entao libera a thread
    global num_threads
    mutex_count_threads.acquire()
    num_threads -= 1
    mutex_count_threads.release()

def compute_idf(postings):
    return math.log(len_index/len(postings)) # numero de documentos que a palavra acontece 

def compute_tf_idf(tf, postings):
    idf = compute_idf(postings)
    return tf*idf

def compute_bm25(tf, postings):
    compute_idf(postings)

def daat_thread(original, query, index, ranker):
    results = []
    heapify(results)
    pos = [0 for i in range(len(query))]
    print(len_index)
    for target in range(0, len_index): # tamanho do indice = numero de documentos
        score = 0
        print(target)
        for i, term in enumerate(query):
            if term not in token_to_offsets:
                continue

            with open(index, 'r') as ind:
                ind.seek(int(token_to_offsets[term]))
                postings = json.loads(ind.readline())

            for j, (docid, tf) in enumerate(postings[term][pos[i]:]):
                if docid == target:
                    print(term, target)
                    if ranker == 'TFIDF':
                        # aqui que entra o calculo da metrica
                        score += compute_tf_idf(tf, postings) 
                    else:
                        score += compute_bm25(tf, postings)
                    pos[i] = j + 1
                    break

        heappush(results, (-1*score, target))

    to_print = {
        "Query": original, 
        "Results": 
    }
    mutex_print_stdout.acquire()

    mutex_print_stdout.release()
    return results
    

def queries_scheduler():
    while(1):
        # esse loop é usado para gerenciar a criaçao de novas threads para processar as consultas
        
        if check_end_query_processing():
            break

        end = 0
        while(1):
            # espera enquanto a fila esta vazia
            if not next_query.empty():
                break
            if check_end_query_processing():
                end = 1
                break

        if end == 1:
            break
        
        # pega a proxima consulta na fila
        query, query_p = next_query.get()
        print(query)
        # verifica se pode criar mais uma thread, caso contrário espera 
        global num_threads
        while(1):
            mutex_count_threads.acquire()
            if num_threads < MAX_THREADS:
                num_threads += 1
                mutex_count_threads.release()
                break
            mutex_count_threads.release()
        
        if check_end_query_processing():
            break
        
        # cria uma thread para processar a consulta atual
        # t = Thread (target = daat_thread, args = (query,), daemon = True) 
        # t.start()
        global count_queries_processed
        mutex_count_queries.acquire()
        count_queries_processed += 1
        mutex_count_queries.release()

def main(path_queries, index, ranker):
    with open(path_queries, 'r') as f_queries:
        queries = f_queries.readlines()
    
    with open("save_url_to_docid.txt", 'r') as f:
        urls = f.readlines()

    for url in urls:
        splitted = url.split(': ')
        docid_to_url[splitted[0]] = splitted[1].replace('\n', '')
    
    global len_index
    len_index = len(docid_to_url)

    with open("save_token_offset.txt", 'r') as f:
        offsets = f.readlines()

    for of in offsets:
        splitted = of.split(': ')
        token_to_offsets[splitted[0]] = int(splitted[1].replace('\n', ''))

    print(token_to_offsets['0'])
    global num_queries 
    num_queries = len(queries)
    print(num_queries)

    global num_threads
    num_threads += 1
    t = Thread (target = query_preprocess_thread, args = (queries,), daemon = True) 
    t.start()

    t.join()

    original, query = next_query.get()
    print(query)
    daat_thread(original, query, index, ranker)

    
    # queries_scheduler()


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

    

