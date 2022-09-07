FLAGS = -lchilkat-9.5.0 -L chilkat/lib -Wl,-rpath=chilkat/lib -pthread
DEBUG_FLAGS = -fsanitize=address,undefined -fno-omit-frame-pointer -g -Wall -Wshadow

crawler: crawl.cpp
	g++ -std=c++17 -O2 -o crawl crawl.cpp $(FLAGS)

debug:
	g++ -std=c++17 -O2 -o crawl crawl.cpp $(FLAGS) $(DEBUG_FLAGS)