// MapReduceFramework.cpp

#include <map>
#include "MapReduceFramework.h"
#include "MapManager.h"

using namespace std;

typedef map<k2Base* ,list<v2Base*>> ShuffledMap; // maybe typedef the list also

pthread_mutex_t mapExecThreadMapMutex;

std::map<pthread_t, mutexContainer*> _threadsMap;

//  the map that contains the output of the shuffle function.
ShuffledMap _shuffledMap;

/**
 * The function that runs the framework.
 */
OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase &mapReduce,
                                     IN_ITEMS_LIST &itemsList,
                                     int multiThreadLevel)
{
    // mutex to lock the index of the input data structure
    pthread_mutex_t input_index_mutex = PTHREAD_MUTEX_INITIALIZER;

    // mutex for the ExecMap threads map
    pthread_mutex_t em_threads_mutex = PTHREAD_MUTEX_INITIALIZER;

    // create the vector to hold the Map phase threads
    std::vector<pthread_t*> threads;

    // create the map that maps threads to mutex and type2 list


    // a loop that create the threads and maps them to a type2 container


    // create the shuffle thread todo should come before the map threads?

    // a loop to join the map threads phase

    // join the shuffle thread

    // todo test! should have a shuffled data structure of type2

    return std::list<OUT_ITEM>();
}
