import sys
import resource
import argparse
from warcio.archiveiterator import ArchiveIterator
import nltk
from threading import Lock
from collections import Counter
import json
import glob
from queue import Queue
import os
from heapq import heappop, heappush, heapify
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import time
import psutil

mini_index = defaultdict(list)
mutex_index = Lock()
MAX_THREADS = 5
count_files = 0

num_inverted_lists = 0
avg_size_lists = 0

stopwords = set(nltk.corpus.stopwords.words('portuguese'))

MEGABYTE = 1024 * 1024
def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

def preprocess(text):
    tokens = nltk.RegexpTokenizer(r'\w+').tokenize(text.lower()) #tokenize and lowercase

    stemmer = nltk.SnowballStemmer('portuguese')
    stem_tokens = [stemmer.stem(token) for token in tokens if token not in stopwords] # remove stopwords and stemming
    return Counter(stem_tokens).most_common(), len(stem_tokens)

def write_index_to_file():
    # cria pasta para salvar indice parcial
    if not os.path.exists('./partial_index'):
        os.makedirs('./partial_index')
    keys_sorted = sorted(mini_index)

    with open('partial_index/'+str(count_files)+'.txt', 'w') as fp:
        for key in keys_sorted:
            fp.write(json.dumps({key: mini_index[key]})+'\n')
        fp.write('$\n')
    mini_index.clear()

def create_mini_inverted_index(warc_file: str, memory_limit: int):
    global docid
    global count_files
    
    with open(warc_file, 'rb') as f:
        for record in ArchiveIterator(f):
            if record.rec_type == 'response':
                url = record.rec_headers.get_header('WARC-Target-URI')
                text = record.raw_stream.read().decode()
                postings, length = preprocess(text)
                
                # monta indice invertido para o record i
                # salva a url e incrementa o docid
                mutex_index.acquire()

                # se atingir 70% da memoria disponivel, salva num arquivo
                process = psutil.Process(os.getpid())
                if process.memory_info().vms/MEGABYTE > 0.7*memory_limit:
                    # write_index_to_file()
                    count_files += 1
                    mini_index.clear()
                    
                # salva tokens no indice global
                for token, tf in postings:
                    mini_index[token].append((docid, tf))

                # salva linha contendo docid para URLs e tamanho dos documentos
                # with open('save_url_to_docid.txt', 'a') as docid_to_url:
                #     docid_to_url.write(str(docid)+': '+url+': '+str(length)+'\n')

                if docid%1000 == 0:
                    print('Ja indexamos: ', docid)
                docid += 1 # incrementa docid porque um documento a mais foi indexado
                mutex_index.release()
    

def get_line(f, iterator):
    f_index = open(f, 'r')
    f_index.seek(iterator)
    line = f_index.readline() 
    f_index.close()

    return line, len(line)

def write_index_and_auxiliar(actual_inverted_list, key, seek_pointer, index_path):
    global num_inverted_lists
    num_inverted_lists += 1

    global avg_size_lists
    avg_size_lists += len(actual_inverted_list)

    # escreve linha no indice global
    with open(index_path, 'a') as f:
        save_line = json.dumps({key: actual_inverted_list})+'\n'
        f.write(save_line)
    
    # escreve linha contendo o offset do token salvo
    with open('save_token_offset.txt', 'a') as f:
        f.write(key+': '+str(seek_pointer)+'\n')
    
    seek_pointer += len(save_line)
    return seek_pointer

def merge_index(index, index_path):
    filelist = glob.glob(index+'/*.txt')
    files_sorted = sorted(filelist, key=lambda x: int(os.path.basename(x).replace('.txt', '')))
    # estruturas auxiliares
    iterator = [0 for i in range(len(files_sorted))]
    updated = [1 for i in range(len(files_sorted))]
    smallest_token = []
    heapify(smallest_token)
    seek_pointer = 0
    lines = []
    aux = []

    while(1):
        
        for i, f in enumerate(files_sorted):

            if iterator[i] == -1:  # se o arquivo de indice i já chegou ao fim
                lines.append((i, 0, []))
                continue
            if updated[i] == 0: # se o arquivo de indice i não sofreu update na ultima iteracao repete oq tinha antes
                lines.append(aux[i]) 
                continue

            # se o apontador do arquivo andou pega a proxima linha
            line, length = get_line(f, iterator[i]) 
            if line == "$\n":
                iterator[i] = -1
                lines.append((i, 0, []))
                continue

            updated[i] = 0
            line = json.loads(line)
            lines.append((i, length, line))

            # coloca o token no heap se não estiver
            key = list(line.keys())[0]
            if key not in smallest_token:
                heappush(smallest_token, key)
                
        aux.clear()
        
        if len(smallest_token) == 0:
            break

        # pega o token de menor ordem lexicografica
        key = heappop(smallest_token)

        # mescla as listas de onde a 'key' aparece
        actual_inverted_list = []
        for i, length, l in lines:
            if l == []:
                continue
            if list(l.keys())[0] == key:
                actual_inverted_list.extend(l[key])
                iterator[i] += length
                updated[i] = 1

        seek_pointer = write_index_and_auxiliar(actual_inverted_list, key, seek_pointer, index_path)
        
        # limpa estruturas para resgatar memória
        actual_inverted_list.clear()
        aux = lines.copy()
        lines.clear()

    smallest_token.clear()

def warcs_scheduler(corpus, memory_limit):
    next_file = Queue()
    files = glob.glob(corpus+'/*.kaggle')
    
    for f in files:
        next_file.put(f)

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # enquanto tiver arquivos WARC para processar 
        # manda para uma thread se ela estiver vazia
        while not next_file.empty():

            f = next_file.get()
            executor.submit(create_mini_inverted_index, f, memory_limit)
    
def main(memory_limit, corpus, index):
    """
    Your main calls should be added here
    """
    global docid
    docid = 0
    
    start_indexing = time.time()
    warcs_scheduler(corpus, memory_limit)
    if len(mini_index) != 0:
        write_index_to_file()

    # merge_index('./partial_index', index)

    # to_print = {"Index Size": os.path.getsize(index)/MEGABYTE,
    #             "Elapsed Time": time.time() - start_indexing,
    #             "Number of Lists": num_inverted_lists,
    #             "Average List Size": avg_size_lists/num_inverted_lists}
        
    print(json.dumps(to_print, ensure_ascii=False))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        '-m',
        dest='memory_limit',
        action='store',
        required=True,
        type=int,
        help='memory available'
    )
    parser.add_argument(
        '-c',
        dest='corpus',
        required=True,
        type=str,
        help='path to corpus'
    )
    parser.add_argument(
        '-i',
        dest='index',
        required=True,
        type=str,
        help='path to index file'
    )
    args = parser.parse_args()
    memory_limit(args.memory_limit)
    
    try:
        main(args.memory_limit, args.corpus, args.index)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)
