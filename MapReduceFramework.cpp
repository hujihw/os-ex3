// MapReduceFramework.cpp

#include <map>
#include "MapReduceFramework.h"
#include "MapManager.h"

class MapReduceFramework
{
public:
    static MapReduceFramework& getInstance();

    MapReduceFramework(MapReduceFramework const&) = delete;

    void operator=(MapReduceFramework const&) = delete;

private:
    MapReduceFramework();

};

unsigned int numberOfThreads;

int InputIndex; // todo mutex

// todo container with pointers to the threads' <K2, V2> containers for the MapManager
std::map<pthread_t, std::vector> threadsMap;

// todo container with pointers to the threads' <K3, V3> containers for the ExecReduce

// todo function to distribute the array indexes for MapManager threads

// todo container for shuffled data (map)

// todo create a pool of MapManager threads

// todo create the Shuffle thread, and put Yusuf to sleep

// todo shuffle management

// todo close MapManager Threads

// todo create ExecReduce threads

// todo sort the output data

// todo log file time measuring (consider different class\namespace)

// todo error handling function


OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase &mapReduce,
                                     IN_ITEMS_LIST &itemsList,
                                     int multiThreadLevel) {
    MapManager *execMap = new MapManager();
    execMap->MapFunctionExec();

    return std::list<OUT_ITEM>();
}
