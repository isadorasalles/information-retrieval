#include "chilkat/include/CkSpider.h"

#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <vector>
#include <set>
#include <queue>

#include <chrono>
#include <thread>
#include <mutex>

using namespace std;

#define CRAWL_MAX 10000
#define THREAD_MAX 100

int crawled_count = 0, num_threads = 0, seed_urls = 0, qtd_lvl1 = 0;
long long total_size = 0;
long long total_time = 0;

// fila de prioridadeds para o escalonador de longo prazo
typedef pair<int, string> pi;
priority_queue<pi, vector<pi>, greater<pi> > next_URL; 

set<string> visited;            // conjunto de URLs visitadas
set<string> visited_domains;    // conjunto de dominios das threads ativas

mutex mutex_url_count;
mutex mutex_queue;
mutex mutex_thread_count;
mutex mutex_visited_url;
mutex mutex_statistics;
mutex mutex_visited_domain;

void insert_queue(string nextLink){
    // calcula a prioridade da URL de acordo com numero de barras e pontos
    int count = 0;
    for (unsigned int j = 0; j < nextLink.size(); j++){
        if (nextLink[j] == '/' or nextLink[j] == '.')
            count++;
        if (j == nextLink.size()-1 and nextLink[j] == '/')
            count--;
    }

    // insere a URL na fila de prioridades
    lock_guard<mutex> lk(mutex_queue);
    next_URL.push(make_pair(count, nextLink));
}

bool check_visited_URLs(string nextLink){
    // verifica se a URL ja foi crawleada
    lock_guard<mutex> lk(mutex_visited_url);
    if (visited.count(nextLink)){
        return true;
    }
    visited.insert(nextLink);
    return false;
}

bool check_visited_domains(string nextDomain){
    // verifica se o dominio esta sendo crawleado
    lock_guard<mutex> lk(mutex_visited_domain);
    if (visited_domains.count(nextDomain)){
        return true;
    }
    visited_domains.insert(nextDomain);
    return false;
}

bool check_end_of_crawling(){
    // testa se ja crawleou o suficiente
    lock_guard<mutex> lk(mutex_url_count);
    if (crawled_count >= CRAWL_MAX)
        return 1;
    return 0;
}


bool crawl(CkSpider &spider, bool seed){
    
	auto begin = chrono::steady_clock::now();
	if (!spider.CrawlNext()) return false;
	auto end = chrono::steady_clock::now();
	
    // verifica se funcoes do chilkat retornam NULL
    const char *url = spider.lastUrl();
    if (url == NULL)
        return false;
    const char *html = spider.lastHtml();
    if (html == NULL)
        return false;
    
    {   // coloca o HTML e a url num arquivo
        lock_guard<mutex> lock (mutex_statistics);
        std::ofstream output("htmls/"+ std::to_string(std::hash<std::string>{}(url))+ ".html");
        output << "<!-- " << url << " -->" << endl;
        output << html;
        output.close();

        // incrementa tamanho da pagina, tempo total contagem de seeds
        total_time += chrono::duration_cast<chrono::milliseconds>(end - begin).count();
        total_size += strlen(html);
        if (seed) seed_urls++;
        else qtd_lvl1++;
    }
    {   // incrementa contador de urls crawleadas
        lock_guard<mutex> lock_count(mutex_url_count);
        crawled_count++;
        cout << url << "\n";
    }
    return true;
}

void short_term_scheduler(string initial){

	int num_Unspidered, num_Outbound, count = 0;
    bool success = 0;

    CkSpider spider;
    spider.Initialize(initial.c_str());
    spider.put_Utf8(true);
    spider.put_ReadTimeout(5);
    spider.put_ConnectTimeout(5); 
    spider.put_MaxResponseSize(1024*1024);
    
    // crawleia pagina inicial
    success = crawl(spider, true);
    spider.SleepMs(1000);

    if (success == 1){
    
        // salvar o numero de links internos e externos na pagina atual
        num_Unspidered = spider.get_NumUnspidered();
        num_Outbound = spider.get_NumOutboundLinks();
        
        // colocar URLs externas na fila de prioridades do escalonador de longo prazo
        for (int i = 0; i < num_Outbound; i++){
            const char *nextOutbound = spider.getOutboundLink(i); 
            if (nextOutbound == NULL)
                continue;
            insert_queue(nextOutbound);
        }

        // crawleia as URLs internas
        for (int i = 0; i < num_Unspidered; i++){
            if (check_end_of_crawling()) break;
            const char *nextUnspidered = spider.getUnspideredUrl(0); 
            if (nextUnspidered == NULL){
                spider.SkipUnspidered(0);
                continue;
            }
            // se a URL ja tiver sido crawleada move da lista de unspidered pra spidered
            bool exists = check_visited_URLs(nextUnspidered);
            if (exists == 1){
                spider.SkipUnspidered(0);
                continue;
            }

            if (check_end_of_crawling()) break;
            if (crawl(spider, false)) count++;
            spider.SleepMs(1000);
        }

        spider.ClearOutboundLinks();
        spider.ClearSpideredUrls();
        spider.ClearFailedUrls();
    }

   
    {   // decrementa contador de threads ativas
        lock_guard<mutex> lock_thread(mutex_thread_count);
        num_threads--;
        if (success == 1)
            cout << initial << ": " << count << " links nivel 1\n";
    }

    {   // retira o dominio da URL atual do conjunto de dominios ativos
        lock_guard<mutex> lock_domain (mutex_visited_domain);
        const char *domain = spider.getUrlDomain(initial.c_str());
        if (domain != NULL)
            visited_domains.erase(domain);
    }
}


void long_term_scheduler(){
    
    while(1){

        if (check_end_of_crawling()) break;

        {   // testa se a fila ta vazia e nao tem thread ativa
            lock_guard<mutex> lock_q (mutex_queue);
            lock_guard<mutex> lock_t (mutex_thread_count);
            if (next_URL.size() == 0 and num_threads == 0){
                break;
            }
        }
        
        CkSpider spider;
        string url;
        bool end = 0;
        
        while(1){
            while(1){
                {   // espera enquanto a fila ta vazia
                    lock_guard<mutex> lock_q (mutex_queue);
                    if (next_URL.size() > 0){
                        break;
                    }
                }
                if (check_end_of_crawling()){
                    end = 1;
                    break;
                }
            }
            if (end == 1)
                break;

            // pega a proxima URL da fila nao tenha sido crawleada e nao tenha dominio ativo
            lock_guard<mutex> lock_q (mutex_queue);
            url = next_URL.top().second;
            bool exists_url = check_visited_URLs(url);
            if (exists_url == 1){
                next_URL.pop();
                continue;
            }
            // checa se o dominio ta sendo crawleado por alguma thread
            const char* domain = spider.getUrlDomain(url.c_str());
            if (domain == NULL){
                next_URL.pop();
                continue;
            }
            bool exists_domain = check_visited_domains(domain);
            if (exists_domain == 0){
                next_URL.pop();
                break;
            }
            else {
                // se o dominio esta em uso diminui a prioridade da URLf
                next_URL.push(make_pair(next_URL.top().first+1, url));
                next_URL.pop();
            }
        }

        if (check_end_of_crawling()) break;

        //verifica se pode criar mais threads
        while(1){
            lock_guard<mutex> lock_t(mutex_thread_count);
            if (num_threads < THREAD_MAX){
                num_threads++;
                break;
            }
        }
        
        thread thread (short_term_scheduler, url);
        thread.detach();
    }
}

int main(int argc, char *argv[]){

    ifstream input(argv[1]);
	if (input.fail()) {
		cout << "Arquivo invalido" << "\n";
		return 1;
	}

    string url;
    while(input >> url)
        insert_queue(url);

    auto begin = chrono::steady_clock::now();
	long_term_scheduler();
	auto end = chrono::steady_clock::now();
    double time = chrono::duration_cast<chrono::milliseconds>(end - begin).count();

    cout << "\nPaginas crawleadas: " << crawled_count << "\n";
    cout << "Tempo medio por pagina: " << double(total_time/crawled_count) << " ms\n";
    cout << "Tamanho medio de uma pagina: " << total_size/crawled_count << " KiB\n";
    cout << "Quantidade de paginas de nivel 0 visitadas: " << seed_urls << "\n";
    cout << "Quantidade de paginas de nivel 1 visitadas: " << qtd_lvl1 << "\n";
    cout << "Tempo total: " << double(time/1000) << " s \n";
    return 0;
}