import sys
import resource
import argparse
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
import nltk
from string import punctuation, digits
from threading import Thread, Lock
from collections import Counter
import json
import pickle

# from memory_profiler import profile

mini_index = {}
mutex_index = Lock()
# docid = 0
mutex_docid = Lock()
stopwords = set(nltk.corpus.stopwords.words('portuguese'))
MEGABYTE = 1024 * 1024
def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

def preprocess(text):
    # remove_digits = str.maketrans('', '', digits)
    # text_no_numbers = text.translate(remove_digits)

    tokens = nltk.RegexpTokenizer(r'\w+').tokenize(text.lower()) # lowercase

    stemmer = nltk.SnowballStemmer('portuguese')
    stem_tokens = [stemmer.stem(token) for token in tokens if token not in stopwords] # stemming
    return Counter(stem_tokens).most_common()

def write_index_to_file():
    # joblib.dump(mini_index, "test.txt.gz", compress=3, protocol=None, cache_size=None)
    # with open('data.pkl', 'wb') as fp:
    #     pickle.dump(mini_index, fp, protocol=pickle.HIGHEST_PROTOCOL)
    global docid
    with open('data0.json', 'w') as fp:
        json.dump(mini_index, fp, sort_keys=True)
    mini_index.clear()

# @profile
def create_mini_inverted_index(warc_file: str, memory_limit: int):
    with open(warc_file, 'rb') as f:
        words_counter = {}
        global docid
        for record in ArchiveIterator(f):
            if record.rec_type == 'response':
                url = record.rec_headers.get_header('WARC-Target-URI')
                postings = preprocess(record.raw_stream.read().decode())
                
                # monta indice invertido para o record i
                # salva a url e incrementa o docid
                mutex_index.acquire()
                print('Size_of: ', sys.getsizeof(mini_index))
                # print()
                # if sys.getsizeof(mini_index) > 0.7*(memory_limit*MEGABYTE):
                    # escreve index num arquivo
                    # print("entrei aqui")
                    # write_index_to_file()
                # if docid == 1000:
                #     write_index_to_file()
                #     break
               
                for token, tf in postings:
                    try:
                        mini_index[token].append((docid, tf))
                    except:
                        mini_index[token] = [(docid, tf)]

                with open('save.txt', 'a') as docid_to_url:
                    docid_to_url.write(str(docid)+': '+url)
                print(docid)
                docid += 1
                mutex_index.release()
                words_counter.clear()
                    

def main(memory_limit):
    """
    Your main calls should be added here
    """
    global docid
    docid = 0
    t = Thread (target = create_mini_inverted_index, args = ("part-0.warc.gz.kaggle", memory_limit, ))
    t.start()
    # create_mini_inverted_index("part-0.warc.gz.kaggle", memory_limit)
    # t2 = Thread (target = create_mini_inverted_index, args = ("part-1.warc.gz.kaggle", memory_limit,))
    # t2.start()
    # t3 = Thread (target = create_mini_inverted_index, args = ("part-2.warc.gz.kaggle", memory_limit,))
    # t3.start()
    # t4 = Thread (target = create_mini_inverted_index, args = ("part-3.warc.gz.kaggle", memory_limit,))
    # t4.start()
    # t5 = Thread (target = create_mini_inverted_index, args = ("part-4.warc.gz.kaggle", memory_limit,))
    # t5.start()
    pass

## ler o WARC
## tokenizar, pre-processar e indexar

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
    # parser.add_argument(
    #     '-c',
    #     dest='corpus',
    #     required=True,
    #     type=str,
    #     help='path to corpus'
    # )
    # parser.add_argument(
    #     '-i',
    #     dest='index',
    #     required=True,
    #     type=str,
    #     help='path to index file'
    # # )
    args = parser.parse_args()
    memory_limit(args.memory_limit)
    
    try:
        main(args.memory_limit)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)


# You CAN (and MUST) FREELY EDIT this file (add libraries, arguments, functions and calls) to implement your indexer
# However, you should respect the memory limitation mechanism and guarantee
# it works correctly with your implementation