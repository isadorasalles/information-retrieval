import sys
import resource
import argparse
from warcio.archiveiterator import ArchiveIterator
import nltk
from threading import Thread, Lock
from collections import Counter
import json
import glob
from queue import Queue
import os
from heapq import heappop, heappush, heapify
# from memory_profiler import profile
from collections import defaultdict

mini_index = defaultdict(list)
mutex_index = Lock()
MAX_THREADS = 5
num_threads = 0
count_files = 0
mutex_count_threads = Lock()

stopwords = set(nltk.corpus.stopwords.words('portuguese'))

MEGABYTE = 1024 * 1024
def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

def peak_virtual_memory_mb():
    with open('/proc/self/status') as f:
        status = f.readlines()
        vmpeak = next(s for s in status if s.startswith("VmPeak:"))
        return vmpeak

def preprocess(text):
    tokens = nltk.RegexpTokenizer(r'\w+').tokenize(text) #tokenize

    stemmer = nltk.SnowballStemmer('portuguese')
    stem_tokens = [stemmer.stem(token.lower()) for token in tokens if token not in stopwords] # lowercase, remove stopwords and stemming
    return Counter(stem_tokens).most_common(), len(stem_tokens)

def write_index_to_file():
    if not os.path.exists('./data_final'):
        os.makedirs('./data_final')
    keys_sorted = sorted(mini_index)

    with open('data_final/'+str(count_files)+'.txt', 'w') as fp:
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

                if docid%10000 == 0 and docid != 0:
                    write_index_to_file()
                    count_files += 1

                for token, tf in postings:
                    mini_index[token].append((docid, tf))

                with open('save_url_to_docid.txt', 'a') as docid_to_url:
                    docid_to_url.write(str(docid)+': '+url+': '+str(length)+'\n')

                print(docid)
                docid += 1 # incrementa docid porque um arquivo a mais ja foi indexado
                mutex_index.release()
    
                
    global num_threads
    mutex_count_threads.acquire()
    num_threads -= 1
    mutex_count_threads.release()

# @profile
def merge_index(index):
    filelist = glob.glob(index+'/*.txt')
    files_sorted = sorted(filelist, key=lambda x: int(os.path.basename(x).replace('.txt', '')))
    print(files_sorted)
    print("Vamos começar a indexar")
    iterator = [0 for i in range(len(files_sorted))]
    updated = [1 for i in range(len(files_sorted))]
    smallest_token = []
    seek_pointer = 0
    heapify(smallest_token)
    lines = []
    aux = []
    
    while(1):
        
        for i, f in enumerate(files_sorted):
            if iterator[i] == -1:
                lines.append((i, 0, []))
                continue
            if updated[i] == 0:
                lines.append(aux[i])
                continue
            f_index = open(f, 'r')
            f_index.seek(iterator[i])
            line = f_index.readline() 
            f_index.close()
            if line == "$\n":
                # fim do arquivo
                iterator[i] = -1
                lines.append((i, 0, []))
                continue

            # if line == "\n":
            #     iterator[i] += 1
            #     lines.append((i, 0, []))
            #     continue

            updated[i] = 0
            length = len(line)
            line = json.loads(line)
            # except json.decoder.JSONDecodeError: 
            #     print(line, f, iterator[i])
            #     line = json.loads(line)
                # continue
            # print(line)
            lines.append((i, length, line))

            key = list(line.keys())[0]
            if key not in smallest_token:
                heappush(smallest_token, key)
                
        aux.clear()
        if len(smallest_token) == 0:
            break
        key = heappop(smallest_token)
        # print(key)
        actual_inverted_list = []
        for i, length, l in lines:
            if l == []:
                continue
            if list(l.keys())[0] == key:
                actual_inverted_list.extend(l[key])
                iterator[i] += length
                updated[i] = 1

        # if actual_inverted_list != []:
        with open('index_final2.txt', 'a') as f:
            save_line = json.dumps({key: actual_inverted_list})+'\n'
            f.write(save_line)
        
        with open('save_token_offset.txt', 'a') as f:
            f.write(key+': '+str(seek_pointer)+'\n')

        seek_pointer += len(save_line)
        actual_inverted_list.clear()
        aux = lines.copy()
        lines.clear()
    smallest_token.clear()

def wait_threads_end():
   
    while(1):
        # print(num_threads)
        mutex_count_threads.acquire()
        if num_threads == 0:
            mutex_count_threads.release()
            break
        mutex_count_threads.release()

def warcs_scheduler(corpus, memory_limit):
    global num_threads
    next_file = Queue()
    files = glob.glob(corpus+'/*.kaggle')
    
    for f in files:
        next_file.put(f)

    while not next_file.empty():

        f = next_file.get()

        while(1):
            mutex_count_threads.acquire()
            if num_threads < MAX_THREADS:
                num_threads += 1
                mutex_count_threads.release()
                break
            mutex_count_threads.release()

        t = Thread (target = create_mini_inverted_index, args = (f, memory_limit, ))
        t.start()
    
    wait_threads_end()

# @profile
def main(memory_limit, corpus):
    """
    Your main calls should be added here
    """
    global docid
    docid = 0
    
    # create_mini_inverted_index(corpus+'/part-0.warc.gz.kaggle', 1024)
    # warcs_scheduler(corpus, memory_limit)
    # if len(mini_index) != 0:
    #     write_index_to_file()

    merge_index('/home/isadorasalles/Documents/1_Sem_Mestrado/RI/pa2/data_final')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    # parser.add_argument(
    #     '-m',
    #     dest='memory_limit',
    #     action='store',
    #     required=True,
    #     type=int,
    #     help='memory available'
    # )
    parser.add_argument(
        '-c',
        dest='corpus',
        required=True,
        type=str,
        help='path to corpus'
    )
    # parser.add_argument(
    #     '-i',
    #     dest='index',
    #     required=True,
    #     type=str,
    #     help='path to index file'
    # # )
    args = parser.parse_args()
    # memory_limit(args.memory_limit)
    
    try:
        main(1024, args.corpus)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)


# You CAN (and MUST) FREELY EDIT this file (add libraries, arguments, functions and calls) to implement your indexer
# However, you should respect the memory limitation mechanism and guarantee
# it works correctly with your implementation