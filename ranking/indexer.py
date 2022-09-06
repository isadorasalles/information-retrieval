import pyterrier as pt
import json
import argparse
import nltk

def preprocessing(text):
    return ' '.join(nltk.RegexpTokenizer(r'\w+').tokenize(text.lower()))

def iter_file(filename):
    with open(filename, encoding='utf-8') as file:
        data = file.readlines()
    for i, l in enumerate(data):
      # assumes that each line contains 'docno', 'text' attributes
      # yields a dictionary for each json line
        if i % 100000 == 0:
            print(f'processing document {i}')
        line = json.loads(l)
    
        yield {'docno': line['id'], 'text': preprocessing(line['text']), 'title': preprocessing(line['title']), 'keywords': preprocessing(' '.join(line['keywords'])) }
    
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('-c', dest='corpus', 
                        action='store', required=True, 
                        type=str, help='path to corpus file')

    parser.add_argument('-i', dest='indexing_type', 
                        action='store', required=True, 
                        type=int, help='pyterrier indexing type')

    args = parser.parse_args()

    if not pt.started():
        pt.init()
    
    
    index = pt.IterDictIndexer("./index", meta={'docno': 20, 'text': 512, 'title': 128, 'keywords': 512}, type=pt.index.IndexingType(args.indexing_type), verbose=True)
    index = index.index(iter_file(args.corpus),  fields=['text', 'title', 'keywords'])