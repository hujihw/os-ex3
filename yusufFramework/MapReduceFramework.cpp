// MapReduceFramework.cpp


//---------------my code-----------------------------------------------
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <pthread.h>
#include <sys/time.h>
#include <vector>
#include <map>
#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <time.h>
#include <stdio.h>
#include <sys/errno.h>


using namespace std;

#define NANO_SEC 1000
#define MICRO_SEC 1000000
#define ADD_MICRO_SEC 10000000
#define MAX_DATE_SIZE 80
#define EXEC_MAP_THREAD_ID 1
#define SHUFFLE_THREAD_ID 2
#define EXEC_REDUCE_THREAD_ID 3

//======================comparator structs================

struct k2Comparator{
    bool operator()(const k2Base * key1, const k2Base * key2) const {
        return * key1 < * key2;
    }
};

//struct k2Comparator {
//    bool operator()(const k2Base * a, const k2Base * b) const {
//        return !(* a < * b || * b < * a);
//    }
//};

struct cmpPthread{
    bool operator()(const pthread_t a, const pthread_t b) const {
        return pthread_equal(a, b);
    }
};


//================type defs============================
typedef std::pair<k2Base *, v2Base *> LvlTwoPair;
typedef std::list<LvlTwoPair> LvlTwoList;
typedef std::vector<std::pair<pthread_t, std::pair<LvlTwoList *,pthread_mutex_t *>>> LvlTwoVec;
typedef std::map<k2Base*, V2_LIST, k2Comparator> ShuffledMap;
typedef std::vector<std::pair<pthread_t, OUT_ITEMS_LIST*>> LvlThreeVec;

//================global variables=====================

// the log file and the mutex locking its access
pthread_mutex_t logMutex = PTHREAD_MUTEX_INITIALIZER;
FILE *logFile;

// access index and its mutex
pthread_mutex_t idxMutex = PTHREAD_MUTEX_INITIALIZER; // todo change
unsigned int baseIdx; // todo change

// conditionals and their mutex
pthread_cond_t condTimeWaitShuffle = PTHREAD_COND_INITIALIZER;
pthread_mutex_t condTimeWaitMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t activeThreadsMutex = PTHREAD_MUTEX_INITIALIZER;


// booleans that symbolizes that the mapping or reduction has finished
bool mappingPhaseFinished;
bool reductionPhaseFinished;

//==============data structures====================

// vector holding the received start data for indexed access
std::vector<IN_ITEM> inputVec;

// vector of items mapped in the mapping phase
LvlTwoVec lvlTwoVec;

ShuffledMap shuffledMap;
ShuffledMap::iterator iterShuffled;

std::vector<pthread_mutex_t *> mapExecMutexVec;
std::vector<pthread_mutex_t *>::iterator mutexIter;

LvlTwoVec mappedLists;

LvlThreeVec lvlThreeVec;

OUT_ITEMS_LIST resultedOutput;

//===============functions=========================

string getLogTime(){
    char timeArray[MAX_DATE_SIZE];
    time_t currentTime;
    struct tm *tm;
    if (time(& currentTime) == -1){
        std::cerr << "MapReduceFramework Failure: time failed.";
        exit(1);
    };
    tm = localtime(& currentTime);
    strftime(timeArray, MAX_DATE_SIZE, "[%d.%m.%Y %X]", tm);
    return timeArray; //TODO check warning
}

unsigned long calcTimeSpan(double start_time, double end_time, double start_sec, double end_sec){
    return (unsigned long) (NANO_SEC * (MICRO_SEC * (end_sec - start_sec) + (end_time - start_time)));
}

void initFramework(int multiThreadLevel) // todo check orig
{
    lvlThreeVec = LvlThreeVec();
    lvlThreeVec.reserve((unsigned long) multiThreadLevel);

    shuffledMap = ShuffledMap();

    lvlTwoVec = LvlTwoVec();
    lvlTwoVec.reserve((unsigned long) multiThreadLevel);

    mapExecMutexVec = std::vector<pthread_mutex_t *>();

    mappingPhaseFinished = false;
    reductionPhaseFinished = false;

    baseIdx = 0;
}

/**
 * @brief - This function is called by the Map
 * function (implemented by the user) in order to add a new
 * pair of <k2,v2> to the framework's internal data structures.
 * This function is part of the MapReduceFramework's API.
 * @k2Item - The k2Base pointer object to emit.
 * @v2Item - The v2Base pointer object to emit.
 */
void Emit2(k2Base* k2Item, v2Base* v2Item)
{
    const pthread_t currThreadId = pthread_self();

    for(auto iter = mappedLists.begin(); iter != mappedLists.end(); iter++) {
        if (pthread_equal(currThreadId, iter-> first)) {

            ((iter->second).first)->push_back(std::move(LvlTwoPair{k2Item, v2Item}));
//            std::cout << "mappedList size: " << mappedLists.size() << std::endl;
//            std::cout << "first thread list size: " << mappedLists.end().operator*().second.first->size() << std::endl;
//            std::cout << "k2Item " << ((mappedLists.end().base()->second).first->end().operator*()).first << std::endl; // todo remove
            return;
        }
    }
}

/**
 * @brief - The Emit3 function is used by the Reduce
 * function in order to add a pair of <k3, v3> to the final output.
 * @k3Item - The k3Base pointer being emitted.
 * @v3Item - The v3Base pointer being emitted.
 */
void Emit3(k3Base* k3Item, v3Base* v3Item) {
    // Check what the current thread is.
    const pthread_t curr_thread = pthread_self();

    for (auto it = lvlThreeVec.begin(); it != lvlThreeVec.end();it++){
        if (pthread_equal(curr_thread, it -> first)){

            (it->second->push_back(std::move(OUT_ITEM{k3Item, v3Item})));
            return;
        }
    }
}

/**
 * @brief - receives a LvlTwoList and adds it to the shuffledMap.
 * @inputList - list of items to shuffle
 */
void addToShuffledMap(LvlTwoList * inputList) {

    for (auto iter = (* inputList).begin(); iter != (* inputList).end(); iter++) {

        k2Base *key = (iter->first);
        v2Base *val = iter->second;

        if (shuffledMap.size() != 0) {

            auto newIter = shuffledMap.find(key);

            if (newIter == shuffledMap.end()){
                shuffledMap.emplace(std::pair<k2Base *, std::list<v2Base *>>(key, V2_LIST{val}));
            }
            else {
                newIter->second.push_back(val);
            }
        }
        else {
            shuffledMap.emplace(std::pair<k2Base *, std::list<v2Base *>>(key, V2_LIST{val}));
        }
    }
    inputList->clear();
}

OUT_ITEMS_LIST mergePhase(LvlThreeVec *lvlThreeVec)
{
    OUT_ITEMS_LIST retList = OUT_ITEMS_LIST();
    for(LvlThreeVec::iterator iter = (* lvlThreeVec).begin();
        iter != (* lvlThreeVec).end(); iter++)
    {
        retList.insert(retList.end(),((*iter).second)->begin(),
                       ((*iter).second)->end());
    }

    for (auto iter = lvlThreeVec->begin(); iter != lvlThreeVec->end(); iter++)
    {
        delete iter->second;
    }
    return retList;
}

void writeToLogCreation(int threadId){

    string currentTime = getLogTime();

    if (pthread_mutex_lock(& logMutex)) {
        std::cerr << "MapReduceFramework Failure: pthread_mutex_lock failed.";
        exit(1);
    }
    if (threadId == EXEC_REDUCE_THREAD_ID){
        std::fprintf(logFile, "Thread ExecReduce created %s\n", currentTime.c_str());
    }
    else if (threadId == EXEC_MAP_THREAD_ID) {
        std::fprintf(logFile, "Thread ExecMap created %s\n", currentTime.c_str());
    }
    else {
        std::fprintf(logFile, "Thread Shuffle created %s\n", currentTime.c_str());
    }
    if (pthread_mutex_unlock(&logMutex)) {
        std::cerr << "MapReduceFramework Failure: pthread_mutex_unlock failed.";
        exit(1);
    }
    return;
}

void writeToLogTermination(int threadId){

    std::string curTime = getLogTime();

    if (pthread_mutex_lock(& logMutex)) {
        std::cerr << "MapReduceFramework Failure: pthread_mutex_lock failed.";
        exit(1);
    }
    if (threadId == EXEC_REDUCE_THREAD_ID) {
        std::fprintf(logFile, "Thread ExecReduce terminated %s\n", curTime.c_str());
    }
    else if (threadId == EXEC_MAP_THREAD_ID) {
        std::fprintf(logFile, "Thread ExecMap terminated %s\n", curTime.c_str());
    } else {
        std::fprintf(logFile, "Thread Shuffle terminated %s\n", curTime.c_str());
    }
    if (pthread_mutex_unlock(& logMutex)) {
        std::cerr << "MapReduceFramework Failure: pthread_mutex_unlock failed.";
        exit(1);
    }
}

/**
 * @brief - the function which the shuffle thread runs.
 */
void* shuffle(void *){
    writeToLogCreation(SHUFFLE_THREAD_ID);

    unsigned int consumed = 0;

    while (!mappingPhaseFinished) {

        // for condtimedwait
        struct timespec ts;
        struct timeval tp;

        gettimeofday(& tp, NULL);
        ts.tv_sec = tp.tv_sec;

        // init condtimedwait to with 0.01 ms shift as instructed.
        ts.tv_nsec = tp.tv_usec * NANO_SEC + ADD_MICRO_SEC;

        int cond_timedwait = pthread_cond_timedwait(&condTimeWaitShuffle, &condTimeWaitMutex, &ts);


        if (cond_timedwait == ETIMEDOUT) {
            if (consumed == inputVec.size()) {
                // if we mapped the whole input after getting the time out
                break;
            }
        }

        for(LvlTwoVec::iterator iter = lvlTwoVec.begin(); iter != lvlTwoVec.end(); iter++) {

            if(!(* (iter->second).first).empty())
            {
                // try locking the mutex
                if (pthread_mutex_lock((iter->second).second)) {
                    std::cerr << "MapReduceFramework Failure: pthread_mutex_lock failed.";
                    exit(1);
                }

                consumed += (* (iter->second).first).size();

                addToShuffledMap((iter->second).first);

                if (pthread_mutex_unlock((iter->second).second)) {
                    std::cerr << "MapReduceFramework Failure: pthread_mutex_unlock failed.";
                    exit(1);
                }
                break;
            }
        }
    }

    // last sweep over the level two vector to check we shuffled all of the items
    for (auto iter2 = lvlTwoVec.begin(); iter2 != lvlTwoVec.end(); iter2++)
    {
        if (pthread_mutex_lock((iter2->second).second))
        {
            std::cerr << "MapReduceFramework Failure: pthread_mutex_lock failed.";
            exit(1);
        }
        consumed += (* (iter2->second).first).size();

        addToShuffledMap(iter2->second.first);

        if (pthread_mutex_unlock((iter2->second).second))
        {
            std::cerr << "MapReduceFramework Failure: pthread_mutex_unlock failed.";
            exit(1);
        }
    }

    iterShuffled = shuffledMap.begin();

    writeToLogTermination(SHUFFLE_THREAD_ID);
    return NULL;
}

/**
 * @brief - the function which each execThread thread runs.
 * @mapReduce - the MapReduceBase object
 */
void* ExecMap(void* mapReduce) {
    writeToLogCreation(EXEC_MAP_THREAD_ID);

    auto mapReduceBase = (MapReduceBase*) mapReduce;

    try{
        LvlTwoList * lvlTwoList = new LvlTwoList;

        pthread_mutex_lock(& activeThreadsMutex);

        pthread_mutex_t * threadMutex  = * mutexIter;
        mutexIter++;

        auto inPair = std::pair<LvlTwoList *, pthread_mutex_t *>(lvlTwoList, threadMutex);
        auto outPair = std::pair<pthread_t, std::pair<LvlTwoList *, pthread_mutex_t *>>(pthread_self(), inPair);

        mappedLists.push_back(outPair);

        pthread_mutex_unlock(& activeThreadsMutex);

        while (!mappingPhaseFinished){
            if (pthread_mutex_lock(threadMutex)) {
                std::cerr << "MapReduceFramework Failure: pthread_mutex_lock failed.";
                exit(1);
            }

            int idx;

            if(pthread_mutex_lock(&idxMutex)){
                std::cerr << "MapReduceFramework Failure: pthread_mutex_lock failed.";
                exit(1);
            }

            idx = baseIdx;
            baseIdx += 10; // the chunk size

            if (baseIdx >= inputVec.size()){
                mappingPhaseFinished = true;
            }

            if (pthread_mutex_unlock(&idxMutex)){
                std::cerr << "MapReduceFramework Failure: pthread_mutex_unlock failed.";
                exit(1);
            }

            int total = std::min(idx + 10, (int) inputVec.size());

            for (int i= idx; i < total; ++i) {
                mapReduceBase->Map(inputVec[i].first, inputVec[i].second);
            }

            if (pthread_mutex_unlock(threadMutex)) {
                std::cerr << "MapReduceFramework Failure: pthread_mutex_unlock failed.";
                exit(1);
            }
            pthread_cond_signal(& condTimeWaitShuffle);
        }

        writeToLogTermination(EXEC_MAP_THREAD_ID);
        return NULL;

    }catch(std::bad_alloc e){
        std::cerr << "MapReduceFramework Failure: new failed.";
        exit(1);
    }
}

/**
 * @brief - the function which the reduce thread runs.
 * @param mapReduce - the MapReduceBase pointer containing the user's reduce function
 */
void* ExecReduce(void* mapReduce)
{
    writeToLogCreation(EXEC_REDUCE_THREAD_ID);

    auto mapReduceBase = (MapReduceBase *) mapReduce;

    try {
        OUT_ITEMS_LIST * outItemsList = new OUT_ITEMS_LIST;

        pthread_mutex_lock(& activeThreadsMutex);

        lvlThreeVec.push_back(std::pair<pthread_t, OUT_ITEMS_LIST *>(pthread_self(), outItemsList));

        pthread_mutex_unlock(& activeThreadsMutex);

        while (!reductionPhaseFinished){
            if (pthread_mutex_lock(&idxMutex)){
                std::cerr << "MapReduceFramework Failure: pthread_mutex_lock failed.";
                exit(1);
            }

            ShuffledMap::iterator currentIter = iterShuffled;

            for (int i = 0; i < 10; ++i) {
                if (iterShuffled != shuffledMap.end()) {
                    iterShuffled++;
                }
                else {
                    reductionPhaseFinished = true;
                    break;
                }

            }

            if (pthread_mutex_unlock(& idxMutex)) {
                std::cerr << "MapReduceFramework Failure: pthread_mutex_unlock failed.";
                exit(1);
            }

            for (int j = 0; j < 10; ++j) {
                if (currentIter == shuffledMap.end()) {
                    break;
                }
                mapReduceBase->Reduce(currentIter->first, (currentIter->second));
                currentIter++;
            }
        }
        writeToLogTermination(EXEC_REDUCE_THREAD_ID);
        return NULL;

    }catch(std::bad_alloc e){
        std::cerr << "MapReduceFramework Failure: new failed.";
        exit(1);
    }
}

/**
 * The function that runs the framework.
 */
OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase &mapReduce,
                                     IN_ITEMS_LIST &itemsList,
                                     int multiThreadLevel) {
    try{
        initFramework(multiThreadLevel);

        logFile = std::fopen(".MapReduceFramework.log", "w");

        std::fprintf(logFile, "runMapReduceFramework started with %d threads\n", multiThreadLevel);

        struct timeval startStamp, finishStamp;

        // --------mapping phase---------

        int error = gettimeofday(&startStamp, NULL);
        if (error == -1) {
            std::cerr << "MapReduceFramework Failure: gettimeofday failed.";
            exit(1);
        }

        pthread_t shuffleThread;

        pthread_t * mapThreadArray = new pthread_t[multiThreadLevel];

        // change the input dast to vector
        inputVec = {std::make_move_iterator(std::begin(itemsList)),
                    std::make_move_iterator(std::end(itemsList))};

        for (int i = 0; i < multiThreadLevel; ++i) {
            pthread_mutex_t tempMutex = PTHREAD_MUTEX_INITIALIZER;

            mapExecMutexVec.push_back(&tempMutex);
        }

        mutexIter = mapExecMutexVec.begin();

        for (int i = 0; i < multiThreadLevel; ++i) {
            if (pthread_create(& mapThreadArray[i], NULL, ExecMap, (void *) & mapReduce)) {
                std::cerr << "MapReduceFramework Failure: pthread_create failed.";
                exit(1);
            }
        }

        if (pthread_create(& shuffleThread, NULL, shuffle, NULL)){
            std::cerr << "MapReduceFramework Failure: pthread_create failed.";
            exit(1);
        }

        for (int i = 0; i < multiThreadLevel; ++i)
        {
            if (pthread_join(mapThreadArray[i], NULL))
            {
                std::cerr << "MapReduceFramework Failure: pthread_join failed.";
                exit(1);
            }
        }

        if (pthread_join(shuffleThread, NULL))
        {
            std::cerr << "MapReduceFramework Failure: pthread_join failed.";
            exit(1);
        }

        for (auto iter = mappedLists.begin(); iter != mappedLists.end(); iter++)
        {
            if (iter->second.first != NULL)
            {
                delete iter->second.first;
            }
        }

        delete[] mapThreadArray;

        int error2 = gettimeofday(&finishStamp, NULL);
        if (error2 == -1){
            std::cerr << "MapReduceFramework Failure: gettimeofday failed.";
            exit(1);
        }

        unsigned long totalTime = calcTimeSpan(startStamp.tv_usec, finishStamp.tv_usec, startStamp.tv_sec, finishStamp.tv_sec);

        error = gettimeofday(& startStamp, NULL);
        if (error == -1) {
            std::cerr << "MapReduceFramework Failure: gettimeofday failed.";
            exit(1);
        }

        std::cout<<"finished the mapping pahse!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"<<std::endl; // todo remove

//        ------reduce phase-------

        pthread_t * reduceThreadArray = new pthread_t[multiThreadLevel];

        for (int i = 0; i < multiThreadLevel; ++i) {
            if (pthread_create(& reduceThreadArray[i], NULL, ExecReduce, (void *) &mapReduce)){
                std::cerr << "MapReduceFramework Failure: pthread_create failed.";
                exit(1);
            }
        }

        for (int i = 0; i < multiThreadLevel; ++i) {
            if (pthread_join(reduceThreadArray[i], NULL)) {
                std::cerr << "MapReduceFramework Failure: pthread_join failed.";
                exit(1);
            }
        }

        delete[] reduceThreadArray;

        // merge the reduce phase outputs
        resultedOutput = mergePhase(&lvlThreeVec);

        resultedOutput.sort([](const std::pair<k3Base *, v3Base *> &first,
                               const std::pair<k3Base *, v3Base *> &second) -> bool{
            const k3Base * firstKey = (first.first);
            const k3Base * secondKey = (second.first);
            return (* firstKey < * secondKey);
        });

//        for (auto )

        error = gettimeofday(&finishStamp, NULL);
        if (error == -1){
            std::cerr << "MapReduceFramework Failure: gettimeofday failed.";
        }

        std::fprintf(logFile, "Map and Shuffle took %lu ns\n", totalTime);

        totalTime = calcTimeSpan(startStamp.tv_usec, finishStamp.tv_usec,
                                   startStamp.tv_sec, finishStamp.tv_sec);

        std::fprintf(logFile, "Reduce took %lu ns\n", totalTime);
        std::fprintf(logFile, "runMapReduceFramework finished\n");

        std::fclose(logFile);

        std::cout<<"finished the whole thing!!!!"<<std::endl; // todo remove
        return resultedOutput;
    }
    catch(std::bad_alloc e){
        std::cerr << "MapReduceFramework Failure: new failed.";
        exit(1);
    }
}
