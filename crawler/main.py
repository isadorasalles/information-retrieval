from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
import argparse
from bs4 import BeautifulSoup
import requests
from urllib.request import urlparse
from threading import Thread, Lock
from queue import PriorityQueue
from time import sleep
import certifi
from url_normalize import url_normalize
from reppy.robots import Robots
import time
from io import BytesIO
import os
import json
import warnings
warnings.filterwarnings("ignore")

next_url = PriorityQueue()
# set com urls visitadas
visited_urls = set()
mutex_visited_urls = Lock()
# set com domínios ativos
domains_in_use = set()
mutex_domains = Lock()

mutex_statistics = Lock() # mutex para computar estatisticas
total_size = 0
total_time_fetch = 0
total_time_store = 0
count_crawled_pages = 0
mutex_url_count = Lock() # mutex para salvar o dado e contar URLs

num_threads = 0
MAX_THREADS = 60
mutex_count_thread = Lock()

def is_valid(url):
    # checa se é uma URL valida
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def check_visited_url(url):
    # checa se a url ja foi visitada
    mutex_visited_urls.acquire()
    if url in visited_urls:
        mutex_visited_urls.release()
        return True
    visited_urls.add(url)
    mutex_visited_urls.release()
    return False

def extract_domain(url):
    # funcao para extrair o dominio, tentando tirar o subdominio, por exemplo se temos www.a.exemplo.com queremos retornar exemplo.com
    domain_chunks = urlparse(url).netloc.split('.')
    if len(domain_chunks) > 2:
        return '.'.join(domain_chunks[-(2 if domain_chunks[-1] in {
            'com', 'net', 'org', 'io', 'ly', 'me', 'sh', 'fm', 'us'} else 3):])
    return '.'.join(domain_chunks)

def domain_in_use(url):
    # checa se o dominio esta ativo em uma thread
    domain = extract_domain(url)
    mutex_domains.acquire()
    if domain in domains_in_use:
        mutex_domains.release()
        return True
    domains_in_use.add(domain)
    mutex_domains.release()
    return False

def check_end_of_crawling():
    # checa se ja atingiu o limite de paginas para crawlear
    mutex_url_count.acquire()
    if count_crawled_pages >= n_pages:
        mutex_url_count.release()
        return True
    mutex_url_count.release()
    return False 

def check_robots_file(url, user_agent):
    # le arquivo robots
    try:
        robots = Robots.fetch(url+'/robots.txt', timeout=5)
    except:
        return 0.1
    # analisa se pode coletar
    if robots.allowed(url, user_agent):
        # checa se tem informacao de delay
        if robots.agent(user_agent).delay == None:
            return 0.1
        return robots.agent(user_agent).delay
    else:
        return -1

def fetch(url):
    # funcao que vai realizar a requisicao da pagina web
    headers = {
        'User-Agent': "Mozilla/5.0 (platform; rv:geckoversion) Gecko/geckotrail Firefox/firefoxversion",
        'Accept-Encoding': 'identity'
        }
    # verifica robots antes
    delay = check_robots_file(url, headers['User-Agent'])
    if delay == -1: # não é permitido crawlear 
        return None
    sleep(delay)
    # tenta coletar a pagina e verifica se ouver exeções especificas, como problema com o certificado
    try:
        page = requests.get(url, headers=headers, verify=certifi.where(), timeout=5)    
    except requests.exceptions.SSLError:
        try:
            page = requests.get(url, headers=headers, verify=False, timeout=5)
        except:
            return None
    except:
        return None
    
    if not page.ok:
        return None
    # verifica se a pagina é html
    if 'text/html' not in page.headers.get('content-type', ''):
        return None

    if page.url != url:
        if check_visited_url(page.url):
            return None

    sleep(delay)
    return page

def store(page, url):
    global count_crawled_pages

    mutex_url_count.acquire()
    # cria pasta para salvar
    if not os.path.exists('./data'):
        os.makedirs('./data')
    
    if count_crawled_pages < n_pages:
        save_file = 'data/pages_'+str(int(count_crawled_pages/1000))+'.warc.gz'
        count_crawled_pages += 1

        with open(save_file, 'ab') as output:
            writer = WARCWriter(output, gzip=True)
            try:
                headers_list = page.raw.headers.items()
                http_headers = StatusAndHeaders('200', headers_list, protocol='HTTP/1.0')
                http_headers.to_ascii_bytes()
            except UnicodeEncodeError: 
                # cria um header sem headers_list 
                http_headers = StatusAndHeaders('200', [], protocol='HTTP/1.0') 

            record = writer.create_warc_record(url, 'response',
                                                payload=BytesIO(page.text.encode()),
                                                http_headers=http_headers)
            writer.write_record(record)
    
    mutex_url_count.release()

def compute_statistics(url, end_fetch, end_store, page_size, tokens_count, domain):
    mutex_statistics.acquire()

    global total_time_fetch
    total_time_fetch += end_fetch
    
    global total_time_store
    total_time_store += end_store

    global total_size
    total_size += page_size

    with open('stats_number_of_tokens_per_page.txt', 'a') as f:
        f.write(url+': '+str(tokens_count)+'\n')

    with open('stats_domains.txt', 'a') as f:
        f.write(domain+'\n')

    mutex_statistics.release()
            

def crawl(url, debug_mode, is_internal):
    if check_end_of_crawling() == False:

        start_fetch = time.time()
        page = fetch(url)
        end_fetch = time.time() - start_fetch

        if page == None:
            return 0, []

        if check_end_of_crawling() == False:
            # salva a pagina
            start_store = time.time()
            store(page, url)
            end_store = time.time() - start_store
            # faz o parser da pagina
            data = page.text
            soup = BeautifulSoup(data, 'html.parser')

            # faz o parser do titulo e do texto para imprmir no modo debug
            try:
                title = soup.title.text
            except:
                title = ''
            try:
                text = ' '.join(soup.text[len(title)+1:].split()[:min(20, len(soup.text))])
            except:
                text = ''

            if debug_mode:
                to_print = {
                            "URL": url,
                            "Title": title,
                            "Text": text,
                            "Timestamp": time.time()
                           }
                j = json.dumps(to_print, ensure_ascii=False)
                print(j)
                
            current_domain = extract_domain(url)
            compute_statistics(url, end_fetch, end_store, len(data), len(soup.text.split()), current_domain)
            
            internal_urls = []

            # loop para encontrar os links internos e externos, feito apenas para urls nivel 0
            if not is_internal:
                for link in soup.find_all('a'):
                    if check_end_of_crawling():
                        break
                    href = link.get('href')
                    if href == None or not is_valid(href):
                        continue
                    if current_domain not in href:
                        # out links - podem ser processados por outra thread
                        try:
                            insert_queue(url_normalize(href))
                        except:
                            pass
                    elif href not in internal_urls:
                        # internal links - mesma thread
                        try:
                            internal_urls.append(url_normalize(href))
                        except:
                            pass
        
            return 1, internal_urls
    return 0, []

def short_term_scheduler(url, debug_mode):

    if check_end_of_crawling() == False:
        success, internal_urls = crawl(url, debug_mode, False)
        # loop para coletar os links internos retornados
        if success:
            for url in internal_urls:
                if check_end_of_crawling():
                    break
                try: 
                    if check_visited_url(url_normalize(url)):
                        continue
                except:
                    continue
                if check_end_of_crawling():
                    break
                _, _ = crawl(url, debug_mode, True)
    
    # libera thread
    global num_threads
    mutex_count_thread.acquire()
    num_threads -= 1
    mutex_count_thread.release()

    mutex_domains.acquire()
    try:
        domains_in_use.remove(extract_domain(url))
    except:
        pass
    mutex_domains.release()


def insert_queue(url):
    # calcula a prioridade da URL de acordo com o número de barras e pontos
    p = 0
    for i in range (0, len(url)):
        if url[i] == '/' or url[i] == '.':
            p += 1
        if i == len(url)-1 and url[i] == '/':
            p -= 1
    
    # insere a URL na fila de prioridades
    next_url.put((p, url))


def long_term_scheduler(debug_mode):
    ## priority queue usando a profundidade das urls
    while(1):
        if check_end_of_crawling(): 
            break

        while(1):
            ## nesse loop sera dada para uma nova thread a próxima URL na fila de prioridades
            ## que ainda não foi coletada e que possui um domínio que não esta ativo, ou seja,
            ## nao esta sendo coletado por nenhuma thread no momento

            end = 0
            while(1):
                # espera enquanto a fila esta vazia
                if not next_url.empty():
                    break
                if check_end_of_crawling():
                    end = 1
                    break
            
            if end == 1:
                break

            priority, url = next_url.get()
            try:
                if check_visited_url(url_normalize(url)):
                    continue
            except:
                continue

            if is_valid(url) == False:
                continue

            if domain_in_use(url):
                # se o dominio esta sendo visitado coloca a URL de volta na fila com uma prioridade menor
                next_url.put((priority+1, url))
                continue
            
            break

        if end == 1:
            break
            
        if check_end_of_crawling(): 
            break

        global num_threads

        # ja temos a proxima url so falta esperar ter thread desocupada para coletá-la
        while (1):
            mutex_count_thread.acquire()
            if num_threads < MAX_THREADS:
                num_threads += 1
                mutex_count_thread.release()
                break
            mutex_count_thread.release()
        
        if check_end_of_crawling(): 
            break
        
        t = Thread (target = short_term_scheduler, args = (url, debug_mode), daemon = True) #verificar 
        t.start()
        

def parse_args():
    parser = argparse.ArgumentParser(
        description='Crawler.')

    parser.add_argument('--seed', '-s',
                        required=True, type=str,
                        help='Path of the seed.')

    parser.add_argument('--limit', '-n',
    					required=True, type=int,
                        help='The target number of webpages to be crawled.')

    parser.add_argument('--debug', '-d',
                        action="store_true",
                        help='Path to folder to save trained model.')

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    global n_pages 
    n_pages = args.limit
    
    # insere seeds na fila de prioridades
    with open(args.seed, 'r') as seeds_file:
        seeds = seeds_file.readlines()
    for seed in seeds:
        insert_queue(url_normalize(seed))

    long_term_scheduler(args.debug)

    print("Paginas coletadas: ", count_crawled_pages)
    print("Tempo medio para coletar uma pagina: ", total_time_fetch/count_crawled_pages)
    print("Tempo medio para salvar uma pagina: ", total_time_store/count_crawled_pages)
    print("Tamanho medio de uma pagina: ", total_size/count_crawled_pages)