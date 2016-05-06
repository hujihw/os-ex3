// MapReduceFramework.cpp

#include <map>
#include <vector>
#include <iostream>
#include <pthread.h>
#include "MapReduceFramework.h"

using namespace std;

// ------------------------------- typedefs ------------------------------------
typedef struct mutexContainer
{
    vector<pair<k2Base*, v2Base*>> threadContainer;
    pthread_mutex_t threadMutex;

    mutexContainer() : threadContainer(), threadMutex(PTHREAD_MUTEX_INITIALIZER)
    {

    }
} mutexContainer;

typedef map<k2Base* ,list<v2Base*>> ShuffledMap; // maybe typedef the list also

// ----------------------------- declarations ----------------------------------

// mutex for the map phase threads map
pthread_mutex_t em_threads_mutex;

// create the vector to hold the Map phase threads
vector<pthread_t*> threads;

// a maps threads to containers with mutexes
map<pthread_t, mutexContainer*> threadsMap;

//  the map that contains the output of the shuffle function.
ShuffledMap _shuffledMap;


// ------------------------------- functions -----------------------------------


void Emit2(k2Base *, v2Base *) {
    cout << "emit2" << endl;
}


void Emit3(k3Base *, v3Base *) {
    cout << "emit3" << endl;
}

void* dummyFunction(void *)
{
    cout << "thread id: " + pthread_self() << endl;
    return nullptr;
}

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
    vector<pthread_t*> threads;

    // create the map that maps threads to mutex and type2 list
    map<pthread_t, mutexContainer*> threadsMap;

    // a loop that create the threads and maps them to a type2 container
    // lock the threads map so shuffle could not read from it while initializing
    pthread_mutex_lock(&em_threads_mutex);

    // create the threads to execute map/reduce function
    for (long unsigned int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_create(threads[i], nullptr ,dummyFunction, nullptr); // todo error handling
        threadsMap[*threads[i]] = new mutexContainer();
    }

    // unlock the thread map after initialization
    pthread_mutex_unlock(&em_threads_mutex);

    // create the shuffle thread todo should come before the map threads?

    // a loop to join the map threads phase

    // join the shuffle thread

    // todo test! should have a shuffled data structure of type2

    return list<OUT_ITEM>();
}
