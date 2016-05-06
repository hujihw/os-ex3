#include "MapReduceFramework.h"
#include <pthread.h>
#include <mutex>
#include <unordered_map>
#include <queue>
#include <map>
#include <sys/time.h>
#include <iostream>
#include <fstream>
#define SEC_TO_MSEC 1000000
#define MSEC_TO_NSEC 1000

using namespace std;

typedef std::pair<k2Base *, v2Base *> INTER_ITEM;

#define CHUCK 10
#define THREAD_EXIT_SUCCESS 0
#define THREAD_EXIT_FAILURE 1
#define PTHREAD_COND_SIGNAL_SUCCESS 0
#define GET_TIME_OF_DAY_SUCCESS 0
#define RESET 0
#define THREAD_CREATE_FAILURE 0
#define THREAD_JOIN_SUCCESS 0


/******************************************************************************/
/*                    Supporting Functions Declaration                        */
/******************************************************************************/

void printError(string function_name);
double calculateTime(const timeval &start, const timeval &end);
string curTimeGetter();
int getChunk();
void * execMapFunction(void *);
void * execReduceFunction(void *);
void shuffleAction();
void * shuffleFunction(void *);


/******************************************************************************/
/*                               Global Fields                                */
/******************************************************************************/

MapReduceBase* mapReduceFunc;
IN_ITEMS_LIST * input_list;
unsigned long input_list_size;
unsigned long shuffle_map_size;
ofstream logFile;


/******************************************************************************/
/*                              Data Structures                               */
/******************************************************************************/

// Comparators
struct k2PointComparator {
    bool operator() (const k2Base* first, const k2Base* second) const{
        return (*first < *second);
    }
};

struct threadComparator {
    bool operator() (const pthread_t& first, const pthread_t& second) const {
        return pthread_equal(first, second) != 0;
    }
};

bool k3Comparator (const OUT_ITEM first, const OUT_ITEM second){
    k3Base * k3first = first.first;
    k3Base * k3second = second.first;

    return (*k3first < *k3second);
}


// Threads Kits
typedef struct execMapKit {
    pthread_mutex_t mutex;
    list<INTER_ITEM> * container;
    pthread_t thread_id;

    execMapKit(pthread_t id){
        container = new list<INTER_ITEM>;
        mutex = PTHREAD_MUTEX_INITIALIZER;
        thread_id = id;
    }

} execMapKit;


typedef struct execReduceKit {
    OUT_ITEMS_LIST * container;
    pthread_t thread_id;

    execReduceKit(pthread_t id){
        container = new OUT_ITEMS_LIST;
        thread_id = id;
    }
} execReduceKit;


unordered_map <pthread_t, execMapKit *,
        std::hash<pthread_t>, threadComparator> execMapThreadsKits;

unordered_map <pthread_t, execReduceKit *,
        std::hash<pthread_t>, threadComparator> execReduceThreadsKits;

map<k2Base *, V2_LIST, k2PointComparator> shuffledMap;



/******************************************************************************/
/*                               Mutex and CV                                 */
/******************************************************************************/

pthread_mutex_t chunk_mutex;
pthread_mutex_t shuffle_mutex;
pthread_mutex_t wait_to_start;

pthread_cond_t shuffle_cond;

int mutex_index;
bool execMapThreadsFinished;



/******************************************************************************/
/*                           Supporting Functions                             */
/******************************************************************************/


/*
 * In case of error, print the function that failed, and exit with exit code 0.
 */
void printError(string function_name){
    cerr << "MapReduceFramework Failure: " << function_name << " failed." <<
            endl;
}



/*
 * Calculating the time in NS between start and end
 */
double calculateTime(const timeval &start, const timeval &end){

    // Start and End time in mili-seconds
    double start_time = (start.tv_sec * SEC_TO_MSEC) + start.tv_usec;
    double end_time = (end.tv_sec * SEC_TO_MSEC) + end.tv_usec;

    double time_diff = end_time - start_time;

    // Convert to nano-seconds
    return (time_diff * MSEC_TO_NSEC);
}



/*
 * Returns the current time, in the log file format
 */
string curTimeGetter() {
    time_t original;
    struct tm *timeData;
    char result[80];

    time (&original);
    timeData = localtime(&original);

    strftime (result, 80, "[%d.%m.%Y %X]", timeData);
    return result;
}



/*
 * Get the index of the end of the next chunk
 */
int getChunk(){

    mutex_index += CHUCK;
    return mutex_index;
}



/*
 * The ExecMap thread function, getting a chunk from the main items list, and
 * mapping them. If the chunk is over the main list's size, the thread is done.
 */
void * execMapFunction(void *){

    // Waiting of all the execMap thread to be created
    pthread_mutex_lock(&wait_to_start);
    pthread_mutex_unlock(&wait_to_start);

    // Getting first chunk
    pthread_mutex_lock(&chunk_mutex);
    int chunkEnd = getChunk();
    pthread_mutex_unlock(&chunk_mutex);

    int i;
    int int_input_size = (int) input_list_size;

    // While the chunk is in the input list range
    while ((chunkEnd - CHUCK) < int_input_size) {

        // Setting the iterator's starting point
        IN_ITEMS_LIST::iterator it = input_list->begin();
        i = (chunkEnd - CHUCK);

        try{
            advance(it, i);
        } catch (...){
            printError("advance");
            pthread_exit((void *) THREAD_EXIT_FAILURE);
        }

        int loop_end = CHUCK + i;

        // Looping over chunk
        for (; (i < loop_end) && (i < int_input_size); i++) {
            mapReduceFunc->Map(it->first, it->second);
            it++;
        }

        if(pthread_cond_signal(&shuffle_cond) != PTHREAD_COND_SIGNAL_SUCCESS){
            printError("pthread_cond_signal");
            pthread_exit((void *) THREAD_EXIT_FAILURE);
        }

        // Getting next chunk
        pthread_mutex_lock(&chunk_mutex);
        chunkEnd = getChunk();
        pthread_mutex_unlock(&chunk_mutex);
    }

    pthread_exit((void *) THREAD_EXIT_SUCCESS);
}



/*
 * The ExecMap thread function, getting a chunk from the main items list, and
 * mapping them. If the chink is over the main list's size, the thread is done.
 */
void * execReduceFunction(void *){

    // Waiting of all the execReduce thread to be created
    pthread_mutex_lock(&wait_to_start);
    pthread_mutex_unlock(&wait_to_start);

    // Getting first chunk
    pthread_mutex_lock(&chunk_mutex);
    int chunkEnd = getChunk();
    pthread_mutex_unlock(&chunk_mutex);

    int i;
    int int_shuffle_size = (int) shuffle_map_size;

    // While the chunk is in the shuffle map range
    while ((chunkEnd - CHUCK) < int_shuffle_size) {

        // Setting the iterator's starting point
        map<k2Base *, V2_LIST>::iterator it = shuffledMap.begin();
        i = (chunkEnd - CHUCK);

        try{
            advance(it, i);
        } catch (...){
            printError("advance");
            pthread_exit((void *) THREAD_EXIT_FAILURE);
        }

        int loop_end = CHUCK + i;

        // Looping over the chunk
        for (; (i < loop_end) && (i < int_shuffle_size); i++) {
            mapReduceFunc->Reduce(it->first, it->second);
            it++;
        }

        // Getting next chunk
        pthread_mutex_lock(&chunk_mutex);
        chunkEnd = getChunk();
        pthread_mutex_unlock(&chunk_mutex);
    }

    pthread_exit((void *) THREAD_EXIT_SUCCESS);
}



/*
 * The shuffle's loop action
 */
void shuffleAction(){

    k2Base * curKey;
    v2Base * curVal;
    V2_LIST * curV2List;
    list<INTER_ITEM> * threadList;

    // Lopping over the execMap kits
    for(auto execMapKit : execMapThreadsKits){

        // Locking the thread's mutex and accessing the container
        pthread_mutex_lock(&execMapKit.second->mutex);
        threadList = execMapKit.second->container;

        // Mapping the V2 to the K2 list
        for(auto k2v2Pair : * threadList){
            curKey = k2v2Pair.first;
            curVal = k2v2Pair.second;

            curV2List = &shuffledMap[curKey];
            curV2List->push_back(curVal);
        }

        // Emptying the container
        threadList->clear();

        // Unlocking the thread's mutex
        pthread_mutex_unlock(&execMapKit.second->mutex);
    }
}



/*
 * The shuffle thread main function.
 */
void * shuffleFunction(void *) {

    struct timespec time_to_wake;
    struct timeval now;

    while(!execMapThreadsFinished){
        shuffleAction();

        // Setting the timed wait time
        if(gettimeofday(&now, NULL) != GET_TIME_OF_DAY_SUCCESS){
            printError("gettimeofday");
            pthread_exit((void *) THREAD_EXIT_FAILURE);
        }
        time_to_wake.tv_sec = now.tv_sec;
        time_to_wake.tv_nsec = (now.tv_usec + 1000);
        pthread_cond_timedwait(&shuffle_cond, &shuffle_mutex, &time_to_wake);
    }

    // Calling action one last time.
    shuffleAction();
    pthread_exit((void *) THREAD_EXIT_SUCCESS);
}



/******************************************************************************/
/*                             Library Functions                              */
/******************************************************************************/

OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_LIST&
itemsList, int multiThreadLevel){


    /*************************** Initializations ******************************/

    int i, res;
    OUT_ITEMS_LIST finalResult;

    // Time declarations
    struct timeval map_shuffle_timer_start;
    struct timeval map_shuffle_timer_end;
    struct timeval reduce_timer_start;
    struct timeval reduce_timer_end;
    double mapShuffleTime;
    double reduceTime;

    // Mutex and CV initialization
    chunk_mutex = PTHREAD_MUTEX_INITIALIZER;
    shuffle_mutex = PTHREAD_MUTEX_INITIALIZER;
    shuffle_cond = PTHREAD_COND_INITIALIZER;
    wait_to_start = PTHREAD_MUTEX_INITIALIZER;

    // Thread declarations
    pthread_t shuffleThread;
    vector<pthread_t> execMapThreads((unsigned long) multiThreadLevel);
    vector<pthread_t> execReduceThreads((unsigned long) multiThreadLevel);
    void * thread_return_val;


    // Global values setting
    input_list = &itemsList;
    mapReduceFunc = &mapReduce;
    input_list_size = input_list->size();


    // Initializing the log file
    logFile.open(
            "/cs/stud/benella/ClionProjects/OS Ex3/.MapReduceFramework.log",
            ofstream::out | ofstream::app);

    logFile << "runMapReduceFramework started with " << multiThreadLevel <<
    " threads" << endl;



    /*********************** Start Map and Shuffle ****************************/

    // Map ans Shuffle start time
    if (gettimeofday(&map_shuffle_timer_start, NULL)
        != GET_TIME_OF_DAY_SUCCESS) {
        printError("gettimeofday");
        exit(0);
    }

    mutex_index = RESET;
    execMapThreadsFinished = false;


    // Creating the shuffle thread
    res = pthread_create(&shuffleThread, NULL, shuffleFunction, NULL);

    if (res < THREAD_CREATE_FAILURE) {
        printError("pthread_create");
        exit(0);
    }

    logFile << "Thread Shuffle created " << curTimeGetter() << endl;


    // Creating the ExecMap threads
    pthread_mutex_lock(&wait_to_start);

    for (i = 0 ; i < multiThreadLevel ; i++) {

        res = pthread_create(&execMapThreads[i], NULL, execMapFunction, NULL);

        if (res < THREAD_CREATE_FAILURE) {
            printError("pthread_create");
            exit(0);
        }

        logFile << "Thread ExecMap created " << curTimeGetter() << endl;

        try{
            execMapThreadsKits[execMapThreads[i]] =
                    new execMapKit(execMapThreads[i]);
        } catch (...){
            printError("new");
            exit(0);
        }
    }

    pthread_mutex_unlock(&wait_to_start);


    // Joining the execMap threads
    for (i = 0 ; i < multiThreadLevel ; i++) {

        if(pthread_join(execMapThreads[i], &thread_return_val) !=
           THREAD_JOIN_SUCCESS){
            printError("pthread_join");
            exit(0);
        }

        if (thread_return_val != THREAD_EXIT_SUCCESS) {
            // Errors was already printed
            exit(0);
        }

        logFile << "Thread ExecMap terminated " << curTimeGetter() << endl;
    }

    execMapThreadsFinished = true;

    // Joining the Shuffle thread
    if(pthread_join(shuffleThread, &thread_return_val) !=
       THREAD_JOIN_SUCCESS){
        printError("pthread_join");
        exit(0);
    }

    if (thread_return_val != THREAD_EXIT_SUCCESS) {
        // Errors was already printed
        exit(0);
    }

    /********************* Map and Shuffle Finished ***************************/



    // Map ans Shuffle end time
    if (gettimeofday(&map_shuffle_timer_end, NULL)
        != GET_TIME_OF_DAY_SUCCESS) {
        printError("gettimeofday");
        exit(0);
    }

    logFile << "Thread Shuffle terminated " << curTimeGetter() << endl;



    /****************************** Start Reduce ******************************/

    // Reduce start time
    if (gettimeofday(&reduce_timer_start, NULL)
        != GET_TIME_OF_DAY_SUCCESS) {
        printError("gettimeofday");
        exit(0);
    }

    mutex_index = RESET;
    shuffle_map_size = shuffledMap.size();

    // Creating the ExecReduce threads
    pthread_mutex_lock(&wait_to_start);

    for (i = 0; i < multiThreadLevel; i++) {

        res = pthread_create(&execReduceThreads[i], NULL,
                             execReduceFunction, NULL);

        if (res < THREAD_CREATE_FAILURE) {
            printError("pthread_create");
            exit(0);
        }

        logFile << "Thread ExecReduce created " << curTimeGetter() << endl;

        try{
            execReduceThreadsKits[execReduceThreads[i]] =
                    new execReduceKit(execReduceThreads[i]);
        } catch (...){
            printError("new");
            exit(0);
        }
    }

    pthread_mutex_unlock(&wait_to_start);


    // Joining the ExecReduce threads
    for (i = 0 ; i < multiThreadLevel ; i++) {

        if(pthread_join(execReduceThreads[i], &thread_return_val) !=
           THREAD_JOIN_SUCCESS){
            printError("pthread_join");
            exit(0);
        }

        if (thread_return_val != THREAD_EXIT_SUCCESS) {
            // Errors was already printed
            exit(0);
        }

        logFile << "Thread ExecReduce terminated " << curTimeGetter() << endl;
    }

    // Reduce end time
    if (gettimeofday(&reduce_timer_end, NULL)
        != GET_TIME_OF_DAY_SUCCESS) {
        printError("gettimeofday");
        exit(0);
    }

    /**************************** Reduce Finished *****************************/



    // Printing timers
    mapShuffleTime = calculateTime(map_shuffle_timer_start,
                                   map_shuffle_timer_end);
    reduceTime = calculateTime(reduce_timer_start, reduce_timer_end);

    logFile << "Map and Shuffle took " << mapShuffleTime << " ns" << endl;
    logFile << "Reduce took " << reduceTime << " ns" << endl;


    // Sorting and merging
    for(auto reduceInfo : execReduceThreadsKits){
        reduceInfo.second->container->sort(k3Comparator);
        finalResult.merge(*reduceInfo.second->container, k3Comparator);
    }
    logFile << "runMapReduceFramework finished" << endl;

    logFile.close();


    /********************* Clearing and Freeing Memory ************************/

    for(auto execMapInfo : execMapThreadsKits){
        delete(execMapInfo.second->container);
        delete(execMapInfo.second);
    }

    for(auto execReduceInfo : execReduceThreadsKits){
        delete(execReduceInfo.second->container);
        delete(execReduceInfo.second);
    }

    execMapThreads.clear();
    execReduceThreads.clear();

    pthread_mutex_destroy(&chunk_mutex);
    pthread_mutex_destroy(&shuffle_mutex);
    pthread_mutex_destroy(&wait_to_start);
    pthread_cond_destroy(&shuffle_cond);

    return finalResult;
}



void Emit2 (k2Base* k2, v2Base* v2){

    // Getting the EcexMap thread's kit
    execMapKit *curThreadKit = execMapThreadsKits[pthread_self()];

    // Locking and adding to the container
    pthread_mutex_lock(&(curThreadKit->mutex));
    curThreadKit->container-> push_back(make_pair(k2, v2));
    pthread_mutex_unlock(&(curThreadKit->mutex));
}



void Emit3 (k3Base* k3, v3Base* v3){

    // Adding to the ExecReduce thread's container
    execReduceThreadsKits[pthread_self()]->
            container->push_back(make_pair(k3, v3));
}