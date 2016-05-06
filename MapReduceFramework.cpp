
#include <pthread.h>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <vector>
#include <map>
#include <sys/errno.h>
#include <sys/time.h>
#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <time.h>
#include <stdio.h>

#define CHUNK 10
#define DATE_MAX_SIZE 80
#define EXEC_MAP_THREAD 1
#define SHUFFLE_THREAD 2
#define EXEC_REDUCE_THREAD 3
#define NANO_CONVERTER 1000
#define MICRO_CONVERTER 1000000
#define MICRO_ADDER 10000000
#define ERROR_RETURN -1
#define START_BASE_INDEX 0
#define SYS_CALL_ERROR_EXIT_CODE 1
#define INITIAL_NUM_CONSUMED 0
/**
 * @brief - Comparator for pthreads that includes a comparison operator
 */
struct cmpPthread{

    /**
     * @brief - Function receives two pthreads and compares them
     * @a - First pthread
     * @b - Second pthread
     * @return - True if a and b are equal, False otherwise
     */
    bool operator()(const pthread_t a, const pthread_t b) const
    {
        return pthread_equal(a, b);
    }
};
struct cmpk2{
    bool operator()(const k2Base* key1, const k2Base* key2) const
    {
        return *key1 < *key2;
    }
};
// Various typedefs to make code more readable
typedef std::pair<k2Base*, v2Base*> LEVEL_TWO_ITEM;
typedef std::list<LEVEL_TWO_ITEM> LEVEL_TWO_LIST;
typedef std::vector<std::pair<pthread_t, std::pair<LEVEL_TWO_LIST*,
                    pthread_mutex_t*>>> LEVEL_TWO_VECTOR;
typedef std::vector<std::pair<pthread_t, OUT_ITEMS_LIST*>> LEVEL_THREE_VECTOR;
//typedef std::map<k2Base*, V2_LIST, > SHUFFLED_MAP;
typedef std::map<k2Base*, V2_LIST, cmpk2> SHUFFLED_MAP;

// lock for the indexed access to startData
pthread_mutex_t baseIndexLock = PTHREAD_MUTEX_INITIALIZER;
unsigned int baseIndex;

//log file and its mutex
FILE *logFile;
pthread_mutex_t logFileLock = PTHREAD_MUTEX_INITIALIZER;

/**
 * @brief - Comparator for k2Base pointers that includes a comparison operator
 */
struct cmpK2{

    /**
    * @brief - Function receives two k2Base pointers and compares them
    * @a - First k2Base pointer
    * @b - Second k2Base pointer
    * @return - True if a and b are equal, False otherwise
     */
    bool operator()(const k2Base* a, const k2Base* b) const
    {
        return !(*a < *b || *b < *a);
    }
};

// flags that symbolize when the mapping or reduction has ended
bool mapEndFlag;
bool reduceEndFlag;

// vector holding the received start data for indexed access
std::vector<IN_ITEM> startData;

// vector holding the mutexes of each mappedThread and iterator to current place
std::vector<pthread_mutex_t*> mapExecMutexVector;
std::vector<pthread_mutex_t*>::iterator freeMutex;

// map of items mapped in the map stage of mapReduce
LEVEL_TWO_VECTOR mappedLists;
// map for shuffled items and iterator to the shuffledMap
SHUFFLED_MAP shuffledMap;
SHUFFLED_MAP::iterator shuffledIt;

// map of items in the reduce stage of mapReduce
LEVEL_THREE_VECTOR reducedLists;

// final merged and sorted map to be returned
OUT_ITEMS_LIST finalLists;

// initialization of various mutexes and condition
pthread_cond_t shufflerCondition = PTHREAD_COND_INITIALIZER;
pthread_mutex_t conditionMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t workerThreadsMutex = PTHREAD_MUTEX_INITIALIZER;

void initializeGlobals(int numThreads)
{
    reducedLists = LEVEL_THREE_VECTOR();
    reducedLists.reserve(numThreads);
    shuffledMap =SHUFFLED_MAP();
    mappedLists = LEVEL_TWO_VECTOR();
    mappedLists.reserve(numThreads);
    mapExecMutexVector = std::vector<pthread_mutex_t*>();
    mapEndFlag = false;
    reduceEndFlag = false;
    baseIndex = START_BASE_INDEX;

}
std::string logGetFormattedTime()
{
    char timeBuffer [DATE_MAX_SIZE];
    time_t curTime;
    struct tm *tv;
    if(time(&curTime) == -1){
        std::cerr << "MapReduceFramework Failure: time() failed.";
        exit(SYS_CALL_ERROR_EXIT_CODE);
    };
    tv = localtime(&curTime);
    strftime(timeBuffer,DATE_MAX_SIZE,"[%d.%m.%Y %X]", tv);
    return timeBuffer;
}

void logCreateThread(int threadType){
    std::string curTime = logGetFormattedTime();
    if (pthread_mutex_lock(&logFileLock))
    {
        std::cerr << "MapReduceFramework Failure: pthread_mutex_lock failed.";
        exit(SYS_CALL_ERROR_EXIT_CODE);
    }
    if(threadType == EXEC_REDUCE_THREAD){
        std::fprintf(logFile,"Thread ExecReduce created %s\n", curTime.c_str());
    }
    else if (threadType == EXEC_MAP_THREAD)
    {
        std::fprintf(logFile,"Thread ExecMap created %s\n", curTime.c_str());
    }else{
        std::fprintf(logFile,"Thread Shuffle created %s\n", curTime.c_str());
    }
    if (pthread_mutex_unlock(&logFileLock))
    {
        std::cerr << "MapReduceFramework Failure: pthread_mutex_unlock failed.";
        exit(SYS_CALL_ERROR_EXIT_CODE);
    }
    return;
}

void logTerminateThread(int threadType){
    std::string curTime = logGetFormattedTime();
    if (pthread_mutex_lock(&logFileLock)) {
        std::cerr <<
        "MapReduceFramework Failure: pthread_mutex_lock failed.";
        exit(SYS_CALL_ERROR_EXIT_CODE);
    }
    if (threadType == EXEC_REDUCE_THREAD) {
        std::fprintf(logFile, "Thread ExecReduce terminated %s\n",
                     curTime.c_str());
    }
    else if (threadType == EXEC_MAP_THREAD) {
        std::fprintf(logFile, "Thread ExecMap terminated %s\n",
                     curTime.c_str());
    } else {
        std::fprintf(logFile, "Thread Shuffle terminated %s\n",
                     curTime.c_str());
    }
    if (pthread_mutex_unlock(&logFileLock)) {
        std::cerr <<
        "MapReduceFramework Failure: pthread_mutex_unlock failed.";
        exit(SYS_CALL_ERROR_EXIT_CODE);
    }
}

/**
 * @brief Calculates the time between operations in nanoseconds
 * @param microsecond part of start time
 * @param microsecond part of end time
 * @param second part of start time
 * @param second part of end time
 * @param number of iterations
 * @return the elapsed times between start and end times in nanoseconds
 */
unsigned long getElapsedTime(double start_time, double end_time,
                             double start_sec, double end_sec){
    // calculate the elapsed time and return it
    unsigned long elapsed_time = (NANO_CONVERTER *(MICRO_CONVERTER*
                                   (end_sec-start_sec)+(end_time-start_time)));
    return elapsed_time;
}

/**
 * @brief - This function receuves a LEVEL_TWO_LIST and adds it to the shuffled
 * map.
 * @listToConsume - list of items in the thread's mapped bucket which we
 * wish to "shuffle"
 */
void consume(LEVEL_TWO_LIST* listToConsume)
{
    // iterate over listToConsume
    for(auto it = (*listToConsume).begin(); it != (*listToConsume).end(); it++)
    {
        // get the current pair's key and value
        k2Base* currKey = (it->first);
        v2Base* currVal = it->second;
        // if this isn't the first iteration then:
        if(shuffledMap.size() != 0) {
            // if the key exists then add the value to the list of values
            // else emplace new list.
            auto it2 = shuffledMap.find(currKey);
            // otherwise if we could not find the key then create a new pair
            if(it2 == shuffledMap.end()){
                shuffledMap.emplace(std::pair<k2Base *, std::list<v2Base *>>
                                            (currKey, V2_LIST{currVal}));
            }
            else{
                it2->second.push_back(currVal);
            }
            // if it is the first iteration then add the pair
        }else{
            shuffledMap.emplace(std::pair<k2Base *, std::list<v2Base *>>
                                        (currKey, V2_LIST{currVal}));
        }
    }
    listToConsume->clear();
}

/**
 * @brief - This function is given to the shuffleThread and shuffles the item
 * emmited by the execMap threads
 */
void* shuffle(void*)
{
    logCreateThread(SHUFFLE_THREAD);
    // initialize the total number of items consumed
    unsigned int numConsumed = INITIAL_NUM_CONSUMED;
    // while not all of the items have been flagged
    while (!mapEndFlag)
    {
        // create a timespec and timeval struct to set out condtimedwait
        struct timespec ts;
        struct timeval tp;
        gettimeofday(&tp, NULL);
        ts.tv_sec = tp.tv_sec;
        // set our condtimedwait to 0.01ms from the current timeofday
        ts.tv_nsec = tp.tv_usec * NANO_CONVERTER + MICRO_ADDER;
        int rc = pthread_cond_timedwait(&shufflerCondition,
                                        &conditionMutex, &ts);
        // if we receieved a timeout and already consumed everything then break
        if (rc == ETIMEDOUT)
        {
            if (numConsumed == startData.size())
            {
                break;
            }
        }
        // iterate over each thread's map and consume it's items
        for(LEVEL_TWO_VECTOR::iterator it = mappedLists.begin();
            it != mappedLists.end(); it++)
        {
            if( !(*(it->second).first).empty())
            {
                // attempt to lock the mutex which corresponds to the thread's
                // bucket mutex
                if (pthread_mutex_lock((it->second).second))
                {
                    std::cerr << "MapReduceFramework Failure: "
                                         "pthread_mutex_lock failed.";
                    exit(SYS_CALL_ERROR_EXIT_CODE);
                }
                numConsumed += (*(it->second).first).size();
                consume((it->second).first);
                if (pthread_mutex_unlock((it->second).second))
                {
                    std::cerr << "MapReduceFramework Failure: "
                                         "pthread_mutex_unlock failed.";
                    exit(SYS_CALL_ERROR_EXIT_CODE);
                }
                break;
            }
        }
    }
    // Once everything was mapped, go over each bucket and consume if not empty
    for (auto it = mappedLists.begin(); it != mappedLists.end(); it++)
    {
        if (pthread_mutex_lock((it->second).second))
        {
            std::cerr << "MapReduceFramework Failure: "
                                 "pthread_mutex_lock failed.";
            exit(SYS_CALL_ERROR_EXIT_CODE);
        }
        numConsumed += (*(it->second).first).size();
        consume(it->second.first);
        if (pthread_mutex_unlock((it->second).second))
        {
            std::cerr << "MapReduceFramework Failure: "
                                 "pthread_mutex_unlock failed.";
            exit(SYS_CALL_ERROR_EXIT_CODE);
        }
    }

    // set the shuffledIt iterator to the start of shuffledMap
    shuffledIt = shuffledMap.begin();
    logTerminateThread(SHUFFLE_THREAD);
    return NULL;
}



/**
 * @brief - Function given to each execMapThread in order to map the startData
 * @mapReduce - a MapReduceBase object
 */
void* ExecMap(void* mapReduce)
{
    logCreateThread(EXEC_MAP_THREAD);
    auto mapReduceBase = (MapReduceBase*) mapReduce;
    // initialize the thread's mapped bucket
    try{
        LEVEL_TWO_LIST* current_list = new LEVEL_TWO_LIST;
    // attempt to get the mutex that locks the mappedList
    pthread_mutex_lock(&workerThreadsMutex);
    // once we get the mutex, figure out the thread's personal bucket mutex
    pthread_mutex_t* threadMutex  = *freeMutex;
    freeMutex++;
    // create a pair of the thread's bucket and mutex
    auto innerPair = std::pair<LEVEL_TWO_LIST*,
            pthread_mutex_t*>(current_list, threadMutex);
    // insert the pair and the thread's id into the mappedLists then unlock
    auto outerPair = std::pair<pthread_t, std::pair<LEVEL_TWO_LIST*,
            pthread_mutex_t*>>(pthread_self(), innerPair) ;
    mappedLists.push_back(outerPair);
    pthread_mutex_unlock(&workerThreadsMutex);
    // while there are still items to be mapped
    while (!mapEndFlag){
        // lock the thread's bucket from the shuffle thread
        if (pthread_mutex_lock(threadMutex))
        {
            std::cerr << "MapReduceFramework Failure: "
                                 "pthread_mutex_lock failed.";
            exit(SYS_CALL_ERROR_EXIT_CODE);
        }
        int base;
        // attempt to get the mutex that locks the global baseIndex to startData
        if(pthread_mutex_lock(&baseIndexLock)){
            std::cerr << "MapReduceFramework Failure: "
                                 "pthread_mutex_lock failed.";
            exit(SYS_CALL_ERROR_EXIT_CODE);
        }
        // iterate over a CHUNK of items in startData and map them
        base = baseIndex;
        baseIndex += CHUNK;
        // if we are at the end of startData turn on the flag
        if(baseIndex >= startData.size()){
            mapEndFlag = true;
        }
        if(pthread_mutex_unlock(&baseIndexLock)){
            std::cerr << "MapReduceFramework Failure: "
                                 "pthread_mutex_unlock failed.";
            exit(SYS_CALL_ERROR_EXIT_CODE);
        }
        // iterate over the items the unmapped items and map them
        int total = std::min(base + CHUNK, (int)startData.size());
        for(int i=base; i < total; ++i)
        {
            mapReduceBase->Map(startData[i].first, startData[i].second);
        }
        // unlock the mutual mutex with the shuffler for the thread's bucket
        if (pthread_mutex_unlock(threadMutex))
        {
            std::cerr << "MapReduceFramework Failure: "
                                 "pthread_mutex_unlock failed.";
        }
        //  send a signal telling shuffle thread that items have been mapped
        pthread_cond_signal(&shufflerCondition);
    }
    logTerminateThread(EXEC_MAP_THREAD);
    return NULL;
    }catch(std::bad_alloc e){
        std::cerr << "MapReduceFramework Failure: new failed.";
        exit(SYS_CALL_ERROR_EXIT_CODE);
    }
}

/**
 * @brief - This function is used by the user
 * implemented Map in order to emit the <k2Base*, v2Base*> pair and add it to
 * the container used by the relevant thread to store the mapped pairs.
 * @k2Item - The k2Base* object to emit.
 * @v2Item - The v2Base* object to emit.
 */
void Emit2 (k2Base* k2Item, v2Base* v2Item)
{
    // Check what the current thread is.
    const pthread_t curr_thread = pthread_self();
    /**
    LEVEL_TWO_MAP::iterator foundItem;
    while((foundItem = mappedLists.find(curr_thread)) == mappedLists.end()){
        std:: cout << " EMIT 2 PROBLEM" << std::endl;
    }
    if (foundItem == mappedLists.end()) {
        std::cout << " EMIT 2 PROBLEM HERE" << std::endl;
    }
    ((foundItem->second).first)->
            push_back(std::move(LEVEL_TWO_ITEM{k2Item, v2Item}));
    **/
    // Iterate through the map, using pthread_equal to check for equality.
    // We avoid using [], at, or find since they are
    // not thread safe and can cause problems.
    for (auto it = mappedLists.begin(); it != mappedLists.end();it++)
    {
        if (pthread_equal(curr_thread, it -> first))
        {
            // If the relevant thread is found, we add <k2Item, v2Item> to the
            // list. We assume the relevant thread will
            // always be found since it adds this pair to the mappedLists
            // container upon creation.
            ((it->second).first)->
                    push_back(std::move(LEVEL_TWO_ITEM{k2Item, v2Item}));
            return;
        }
    }
}

/**
 * @brief This function is used by the user implementation of Map in
 * order to send the Reduced pair to the container
 * used by the relevant ExecReduce thread. (Later on is merged).
 * @k3Item - The k3Base* being emitted.
 * @v3Item - The v3Base* being emitted.
 */
void Emit3 (k3Base* k3Item, v3Base* v3Item)
{
    // Check what the current thread is.
    const pthread_t curr_thread = pthread_self();
    /**
    LEVEL_THREE_MAP::iterator foundItem;
    while((foundItem = reducedLists.find(curr_thread)) == reducedLists.end()){
        std:: cout << " EMIT 3 PROBLEM" << std::endl;
    }
    (foundItem->second)->push_back(std::move(OUT_ITEM{k3Item, v3Item}));
    **/
    // Iterate through the map, using pthread_equal to check for equality.
    // We avoid using [], at, or find since they are not thread safe and
    // can cause problems.
    for (auto it = reducedLists.begin(); it != reducedLists.end();it++)
    {
        // If the relevant thread is found, we add <k3Item, v3Item> to the list.
        // We assume the relevant thread will always be found since it adds this
        // pair to the reducedLists container upon creation.
        if (pthread_equal(curr_thread, it -> first))
        {
            (it->second->push_back(std::move(OUT_ITEM{k3Item, v3Item})));
            return ;
        }
    }
}

/**
 * @brief - This function is the routine called by the threads used to reduce
 * the Shuffler's output to containers, to
 * be merged later on.
 * @param mapReduce - This is the MapReduceBase* containing the Reduce function
 * entered by the user in the function which runs the framework. The type is
 * void* in order to comply with the pthread API.
 */
void* ExecReduce(void* mapReduce)
{
    logCreateThread(EXEC_REDUCE_THREAD);
    // Cast void* in order to use the reduce function.
    auto mapReduceBase = (MapReduceBase*) mapReduce;

    // Dynamically allocate the container to be used by each ExecReduce thread.
    try {
        OUT_ITEMS_LIST *current_list = new OUT_ITEMS_LIST;

        /*
         * Lock the reducedLists container to enable safe addition of the
         * pair which identifies the thread and its relevant container.
        */
        pthread_mutex_lock(&workerThreadsMutex);
        reducedLists.push_back(std::pair<pthread_t, OUT_ITEMS_LIST *>
                                       (pthread_self(), current_list));
        pthread_mutex_unlock(&workerThreadsMutex);

        /*
         * Mutex serves as a gate which does not enable Emit3() to be called
         * until all the ExecThreads have been created and added themselves to a
         * reducedLists container (which holds all the pthreads and relevant
         * containers)
         * This is done to ensure that during iteration over the reducedLists
         * container (this happens during the emit
         * method), the iterator is not invalidated by the addition of a key.
         */
        while (!reduceEndFlag) {
            // Lock baseIndex mutex, ensures safe usage of reduceEndFlag
            if (pthread_mutex_lock(&baseIndexLock)) {
                std::cerr << "MapReduceFramework Failure: "
                        "pthread_mutex_lock failed.";
                exit(SYS_CALL_ERROR_EXIT_CODE);
            }
            // The const iterator used to go over the shuffledMap.
            SHUFFLED_MAP::iterator currentIt = shuffledIt;

            // Increments the iterator indicating up to where the shuffledMap
            // has been read, and turns on the reduceEndFlag
            // boolean if necessary.
            for (int i = 0; i < CHUNK; ++i) {
                if (shuffledIt != shuffledMap.end()) {
                    shuffledIt++;
                }
                else {
                    reduceEndFlag = true;
                    break;
                }

            }
            // unlock baseIndex
            if (pthread_mutex_unlock(&baseIndexLock)) {
                std::cerr << "MapReduceFramework Failure: "
                        "pthread_mutex_unlock failed.";
                exit(SYS_CALL_ERROR_EXIT_CODE);
            }
            /*
             * Reduces the chunked elements.
             */
            for (int j = 0; j < CHUNK; ++j) {
                if (currentIt == shuffledMap.end()) {
                    break;
                }
                mapReduceBase->Reduce(currentIt->first, (currentIt->second));
                currentIt++;
            }
        }
        logTerminateThread(EXEC_REDUCE_THREAD);
        return NULL;
    }catch(std::bad_alloc e){
        std::cerr << "MapReduceFramework Failure: new failed.";
        exit(SYS_CALL_ERROR_EXIT_CODE);
    }
}

/**
 * @brief - This auxiliary method takes the values of the map (which are lists)
 * and merges them. @reducedMap - The LEVEL_THREE_MAP which contains lists of
 * <k3Base*, v3Base*> as values.
 * @return - A merged list of all the lists in the original reducedMap.
 */
OUT_ITEMS_LIST merge(LEVEL_THREE_VECTOR* reducedMap)
{
    // List to be returned by value.
    OUT_ITEMS_LIST result = OUT_ITEMS_LIST();
    // Merges the list.
    for(LEVEL_THREE_VECTOR::iterator it = (*reducedMap).begin();
        it != (*reducedMap).end(); it++)
    {
        result.insert(result.end(),((*it).second)->begin(),
                      ((*it).second)->end());
    }
    // Release all dynamic memory which was allocated.
    for (auto it = reducedMap->begin(); it != reducedMap->end(); it++)
    {
        delete it->second;
    }
    return result;
}


/**
 * @brief - This function runs the framework, and returns an OUT_ITEMS_LIST of
 * <k3Base*, v3Base*>
 * @mapReduce - The up-casted class that contains the map and reduce functions.
 * @itemsList - The preliminary items to run the framework on.
 * @multiThreadLevel - The amount of threads to create.
 * @return - OUT_ITEMS_LIST of <k3Base*, v3Base*> created after the framework
 * has finished running.
 */
OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce,
                                     IN_ITEMS_LIST& itemsList,
                                     int multiThreadLevel)
{
    try {
        initializeGlobals(multiThreadLevel);
        logFile = std::fopen(".MapReduceFramework.log", "w");
        std::fprintf(logFile, "runMapReduceFramework started with %d threads\n",
                     multiThreadLevel);
        struct timeval start, finish;
        int res1 = gettimeofday(&start, NULL);
        // Create the groundwork for the shuffler and the ExecMap threads.
        pthread_t shuffleThread;
        pthread_t *mapThreadArray = new pthread_t[multiThreadLevel];
        // Transfer starting data to make easier access.
        startData = {std::make_move_iterator(std::begin(itemsList)),
                     std::make_move_iterator(std::end(itemsList))};
        /* Initalize all the mutexes that will be used later on by the shuffler
         * and each thread
         * when they compete for access to their shared container).
         */
        for (int i = 0; i < multiThreadLevel; ++i) {
            pthread_mutex_t tempMutex = PTHREAD_MUTEX_INITIALIZER;
            mapExecMutexVector.push_back(&tempMutex);
        }
        // Iterator for the mutex vector (each ExecMap takes a mutex).
        freeMutex = mapExecMutexVector.begin();
        /*
         * This mutex ensures that Emit2() cannot happen while threads are still
         * being added to mappedLists.
         * Any change of the mappedLists (i.e an ExecMap adding its pair) could
         * potentially invalidate the iterator used
         * to find the key for the relevant pthread during the emit function.
         */

        // Create all threads
        for (int i = 0; i < multiThreadLevel; ++i) {
            if (pthread_create(&mapThreadArray[i], NULL, ExecMap,
                               (void *) &mapReduce)) {
                std::cerr <<
                "MapReduceFramework Failure: pthread_create failed.";
                exit(SYS_CALL_ERROR_EXIT_CODE);
            }
        }
        if(pthread_create(&shuffleThread, NULL, shuffle, NULL)){
            std::cerr << "MapReduceFramework Failure: pthread_create failed.";
            exit(SYS_CALL_ERROR_EXIT_CODE);
        }


     // Wait for all threads to complete.
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        if (pthread_join(mapThreadArray[i], NULL))
        {
            std::cerr << "MapReduceFramework Failure: pthread_join failed.";
            exit(SYS_CALL_ERROR_EXIT_CODE);
        }
    }
    if (pthread_join(shuffleThread, NULL))
    {
        std::cerr << "MapReduceFramework Failure: pthread_join failed.";
        exit(SYS_CALL_ERROR_EXIT_CODE);
    }
    // Clear all memory from mappedLists
    for (auto it = mappedLists.begin(); it != mappedLists.end(); it++)
    {
        if (it->second.first != NULL)
        {
            delete it->second.first;
        }
    }

    delete[] mapThreadArray;

        int res2 = gettimeofday(&finish, NULL);
        if (res1 == ERROR_RETURN || res2 == ERROR_RETURN) {
            std::cerr << "MapReduceFramework Failure: gettimeofday failed.";
            exit(SYS_CALL_ERROR_EXIT_CODE);

        }
        // return the elapsed time calculation and print it to log
        unsigned long elapsedTime = getElapsedTime(start.tv_usec,
                                                   finish.tv_usec,
                                                   start.tv_sec, finish.tv_sec);

        // Wait for shuffle to complete and release memory.
        res1 = gettimeofday(&start, NULL);
        // Same flow as before, creates groundwork for pthreads
        pthread_t *reduceThreadArray = new pthread_t[multiThreadLevel];

        // Use same mutex to prevent Emit3 and change to reducedLists
        // (similar to earlier)
        for (int i = 0; i < multiThreadLevel; ++i) {
            if (pthread_create(&reduceThreadArray[i], NULL, ExecReduce,
                               (void *) &mapReduce)) {
                std::cerr <<
                "MapReduceFramework Failure: pthread_create failed.";
                exit(SYS_CALL_ERROR_EXIT_CODE);
            }

        }

        // Wait for all ExecReduce threads to finish
        for (int i = 0; i < multiThreadLevel; ++i) {
            if (pthread_join(reduceThreadArray[i], NULL)) {
                std::cerr << "MapReduceFramework Failure: pthread_join failed.";
                exit(SYS_CALL_ERROR_EXIT_CODE);
            }
        }
        delete[] reduceThreadArray;

        // Merge all the containers into a final list to be returned
        finalLists = merge(&reducedLists);

        /*
         * Sort the list via lambda function which compares pairs according to
         * the k3Base comparator (used from the class derived from k3Base.
         */
        finalLists.sort([](const std::pair<k3Base *, v3Base *> &x,
                           const std::pair<k3Base *, v3Base *> &y) -> bool {
            const k3Base *firstKey = (x.first);
            const k3Base *secondKey = (y.first);
            return (*firstKey < *secondKey);
        });
        res2 = gettimeofday(&finish, NULL);
        if (res1 == ERROR_RETURN || res2 == ERROR_RETURN) {
            std::cerr << "MapReduceFramework Failure: gettimeofday failed.";
        }
        // return the elapsed time calculation and print it to log
        std::fprintf(logFile, "Map and Shuffle took %lu ns\n", elapsedTime);
        elapsedTime = getElapsedTime(start.tv_usec, finish.tv_usec,
                                     start.tv_sec, finish.tv_sec);
        std::fprintf(logFile, "Reduce took %lu ns\n", elapsedTime);
        std::fprintf(logFile, "runMapReduceFramework finished\n");
        std::fclose(logFile);
        return finalLists;
    }catch(std::bad_alloc e){
        std::cerr << "MapReduceFramework Failure: new failed.";
        exit(SYS_CALL_ERROR_EXIT_CODE);
    }
}

