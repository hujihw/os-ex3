#include <pthread.h>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <vector>
#include <map>
#include <sys/time.h>
#include <iostream>
#include <unordered_map>
#include <algorithm>

//======================define==============================
#define NANO_SEC 1000
#define MICRO_SEC 1000000
#define ADD_MICRO_SEC 10000000

#define MAP_THREAD_ID 0
#define SHUFFLE_THREAD_ID 1
#define REDUCE_THREAD_ID 2


//====================operator structs=======================
/**
 * @brief - Comparator for identifying threads
 */
struct threadCmp {
    bool operator()(const pthread_t first, const pthread_t second) const {
        return pthread_equal(first, second);
    }
};

/**
 * @brief - Comparator for level 2 keys
 */
struct k2Cmp{
    bool operator()(const k2Base * k2Base1, const k2Base * k2Base2) const {
        return * k2Base1 < * k2Base2;
    }
};

//======================type defs======================
typedef std::pair<k2Base *, v2Base *> LvlTwoPair;
typedef std::list<LvlTwoPair> LvlTwoList;
typedef std::vector<std::pair<pthread_t, std::pair<LvlTwoList *,
        pthread_mutex_t*>>> LevelTwoVec;
typedef std::vector<std::pair<pthread_t, OUT_ITEMS_LIST*>> LevelThreeVec;
typedef std::map<k2Base*, V2_LIST, k2Cmp> ShuffledMap;


//======================mutexs==============================
pthread_cond_t pthreadCondTShuffle = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutexCondition = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t activeThreadsMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t idxMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t logFileMutex = PTHREAD_MUTEX_INITIALIZER;

//========================vars and dast======================
unsigned int idx;
FILE *logFile;
std::vector<IN_ITEM> inputVec;
LevelTwoVec levelTwoVec;
ShuffledMap shuffledMap;
ShuffledMap::iterator shuffledIter;
LevelThreeVec levelThreeVec;
std::vector<pthread_mutex_t*> mutexVector;
std::vector<pthread_mutex_t*>::iterator mutexIter;
OUT_ITEMS_LIST resultedOutputLists;
bool mappingFinished;
bool reductionFinished;

//=================functions===============================
// initialize all of the global vars and dast
void initFramework(int multiThreadLevel){
    mappingFinished = false;
    reductionFinished = false;
    idx = 0;
    mutexVector = std::vector<pthread_mutex_t*>();
    levelThreeVec = LevelThreeVec();
    levelThreeVec.reserve(multiThreadLevel);
    levelTwoVec = LevelTwoVec();
    levelTwoVec.reserve(multiThreadLevel);
    shuffledMap = ShuffledMap();
}


void mapReduceFrameworkFailure(std::string funcName) {
    std::cerr << "MapReduceFramework Failure: " << funcName << " failed.";
    exit(1);
}

std::string getLogTimeFormat(){
    char timeArray[80];
    time_t currentTime;

    struct tm *tm;
    if(time(&currentTime) == -1){
        mapReduceFrameworkFailure("time");
    };
    tm = localtime(&currentTime);

    strftime(timeArray, 80, "[%d.%m.%Y %X]", tm);
    return timeArray;
}

void writeToLogCreation(int threadId){
    std::string currentTime = getLogTimeFormat();

    if (pthread_mutex_lock(&logFileMutex))
    {
        mapReduceFrameworkFailure("pthread_mutex_lock");
    }
    if(threadId == REDUCE_THREAD_ID){
        std::fprintf(logFile,"Thread ExecReduce created %s\n",
                     currentTime.c_str());
    }
    else if (threadId == MAP_THREAD_ID)
    {
        std::fprintf(logFile,"Thread ExecMap created %s\n",
                     currentTime.c_str());
    }else{
        std::fprintf(logFile,"Thread Shuffle created %s\n",
                     currentTime.c_str());
    }
    if (pthread_mutex_unlock(&logFileMutex))
    {
        mapReduceFrameworkFailure("pthread_mutex_unlock");
    }
}

void writeToLogTermination(int threadId){
    std::string currentTime = getLogTimeFormat();

    if (pthread_mutex_lock(&logFileMutex)) {
        std::cerr <<
        "MapReduceFramework Failure: pthread_mutex_lock failed.";
        exit(1);
    }
    if (threadId == REDUCE_THREAD_ID) {
        std::fprintf(logFile, "Thread ExecReduce terminated %s\n",
                     currentTime.c_str());
    }
    else if (threadId == MAP_THREAD_ID) {
        std::fprintf(logFile, "Thread ExecMap terminated %s\n",
                     currentTime.c_str());
    } else {
        std::fprintf(logFile, "Thread Shuffle terminated %s\n",
                     currentTime.c_str());
    }
    if (pthread_mutex_unlock(&logFileMutex)) {
        std::cerr <<
        "MapReduceFramework Failure: pthread_mutex_unlock failed.";
        exit(1);
    }
}

void Emit2 (k2Base* k2Item, v2Base* v2Item) {
    const pthread_t currentThread = pthread_self();
    for (auto iter = levelTwoVec.begin(); iter != levelTwoVec.end(); iter++) {
        if (pthread_equal(currentThread, iter-> first)) {
            ((iter->second).first)->
                    push_back(std::move(LvlTwoPair{k2Item, v2Item}));
            return;
        }
    }
}

void Emit3 (k3Base* k3Item, v3Base* v3Item) {
    const pthread_t currentThread = pthread_self();
    for (auto iter = levelThreeVec.begin(); iter != levelThreeVec.end();
         iter++) {
        if (pthread_equal(currentThread, iter-> first)) {
            (iter->second->push_back(std::move(OUT_ITEM{k3Item, v3Item})));
            return ;
        }
    }
}
unsigned long getTimeSpan(double startTime, double endTime, double startSec,
                          double endSec){
    return (unsigned long) (NANO_SEC * (MICRO_SEC * (endSec - startSec) +
                                        (endTime - startTime)));
}

void addToShuffledMap(LvlTwoList * lvlTwoList) {
    for (auto iter = (*lvlTwoList).begin(); iter != (*lvlTwoList).end();
         iter++) {

        k2Base* key = (iter->first);
        v2Base* val = iter->second;

        if(shuffledMap.size() != 0) {

            auto newIter = shuffledMap.find(key);

            if(newIter == shuffledMap.end()){
                shuffledMap.emplace(std::pair<k2Base *, std::list<v2Base *>>
                                            (key, V2_LIST{val}));
            }
            else {
                newIter->second.push_back(val);
            }
        }else{
            shuffledMap.emplace(std::pair<k2Base *, std::list<v2Base *>>
                                        (key, V2_LIST{val}));
        }
    }
    lvlTwoList->clear();
}

void* ExecMap(void* mapReduce) {
    writeToLogCreation(MAP_THREAD_ID);

    auto mapReduceBase = (MapReduceBase*) mapReduce;

    try{
        LvlTwoList *lvlTwoList = new LvlTwoList;

        pthread_mutex_lock(&activeThreadsMutex);
        pthread_mutex_t* threadMutex  = *mutexIter;
        mutexIter++;
        auto inPair = std::pair<LvlTwoList *,
                pthread_mutex_t*>(lvlTwoList, threadMutex);
        auto outPair = std::pair<pthread_t, std::pair<LvlTwoList *,
                pthread_mutex_t*>>(pthread_self(), inPair) ;

        levelTwoVec.push_back(outPair);
        pthread_mutex_unlock(&activeThreadsMutex);

        while (!mappingFinished){
            if (pthread_mutex_lock(threadMutex)) {
                mapReduceFrameworkFailure("pthread_mutex_lock");
            }
            int index;
            if(pthread_mutex_lock(&idxMutex)){
                mapReduceFrameworkFailure("pthread_mutex_lock");
            }

            index = idx;
            idx += 10;

            if(idx >= inputVec.size()){
                mappingFinished = true;
            }

            if(pthread_mutex_unlock(&idxMutex)){
                mapReduceFrameworkFailure("pthread_mutex_unlock");
            }

            int total = std::min(index + 10, (int) inputVec.size());
            for(int i= index; i < total; ++i) {
                mapReduceBase->Map(inputVec[i].first, inputVec[i].second);
            }

            if (pthread_mutex_unlock(threadMutex)) {
                mapReduceFrameworkFailure("pthread_mutex_unlock");
            }
            pthread_cond_signal(&pthreadCondTShuffle);
        }
        writeToLogTermination(MAP_THREAD_ID);

        return NULL;

    }catch(std::bad_alloc e){
        mapReduceFrameworkFailure("new");
    }
    return nullptr;
}

void* shuffle(void*) {
    writeToLogCreation(SHUFFLE_THREAD_ID);
    unsigned int addedCounter = 0;
    while (!mappingFinished) {
        struct timespec ts;
        struct timeval tp;

        gettimeofday(&tp, NULL);
        ts.tv_sec = tp.tv_sec;

        ts.tv_nsec = tp.tv_usec * NANO_SEC + ADD_MICRO_SEC;
        int cond_timedwait = pthread_cond_timedwait(&pthreadCondTShuffle,
                                                    &mutexCondition, &ts);
        if (cond_timedwait == ETIMEDOUT) {
            if (addedCounter == inputVec.size())
            {
                break;
            }
        }
        for(LevelTwoVec::iterator iter = levelTwoVec.begin();
            iter != levelTwoVec.end(); iter++) {
            if( !(*(iter->second).first).empty()) {
                if (pthread_mutex_lock((iter->second).second)) {
                    mapReduceFrameworkFailure("pthread_mutex_lock");
                }
                addedCounter += (*(iter->second).first).size();
                addToShuffledMap((iter->second).first);

                if (pthread_mutex_unlock((iter->second).second)) {
                    mapReduceFrameworkFailure("pthread_mutex_unlock");
                }
                break;
            }
        }
    }
    for (auto iter = levelTwoVec.begin(); iter != levelTwoVec.end(); iter++){
        if (pthread_mutex_lock((iter->second).second)) {
            mapReduceFrameworkFailure("pthread_mutex_lock");
        }
        addedCounter += (*(iter->second).first).size();

        addToShuffledMap(iter->second.first);

        if (pthread_mutex_unlock((iter->second).second)) {
            mapReduceFrameworkFailure("pthread_mutex_unlock");
        }
    }
    shuffledIter = shuffledMap.begin();

    writeToLogTermination(SHUFFLE_THREAD_ID);
    return NULL;
}


OUT_ITEMS_LIST addToLvlThreeVec(LevelThreeVec *reducedMap) {
    OUT_ITEMS_LIST outItemsList = OUT_ITEMS_LIST();
    for(LevelThreeVec::iterator iter = (*reducedMap).begin();
        iter != (*reducedMap).end(); iter++) {
        outItemsList.insert(outItemsList.end(), ((*iter).second)->begin(),
                            ((*iter).second)->end());
    }

    for (auto iter2 = reducedMap->begin(); iter2 != reducedMap->end(); iter2++){
        delete iter2->second;
    }
    return outItemsList;
}


void* ExecReduce(void* mapReduce)
{
    writeToLogCreation(REDUCE_THREAD_ID);

    auto mapReduceBase = (MapReduceBase*) mapReduce;

    try {
        OUT_ITEMS_LIST *outItemsList = new OUT_ITEMS_LIST;

        pthread_mutex_lock(&activeThreadsMutex);
        levelThreeVec.push_back(std::pair<pthread_t, OUT_ITEMS_LIST *>
                                       (pthread_self(), outItemsList));
        pthread_mutex_unlock(&activeThreadsMutex);

        while (!reductionFinished) {
            if (pthread_mutex_lock(&idxMutex)) {
                mapReduceFrameworkFailure("pthread_mutex_lock");
            }
            ShuffledMap::iterator shuffledIterator = shuffledIter;

            for (int i = 0; i < 10; ++i) {
                if (shuffledIter != shuffledMap.end()) {
                    shuffledIter++;
                }
                else {
                    reductionFinished = true;
                    break;
                }

            }
            if (pthread_mutex_unlock(&idxMutex)) {
                mapReduceFrameworkFailure("pthread_mutex_unlock");
            }
            for (int j = 0; j < 10; ++j) {
                if (shuffledIterator == shuffledMap.end()) {
                    break;
                }
                mapReduceBase->Reduce(shuffledIterator->first,
                                      (shuffledIterator->second));

                shuffledIterator++;
            }
        }
        writeToLogTermination(REDUCE_THREAD_ID);

        return NULL;

    }catch(std::bad_alloc e){
        mapReduceFrameworkFailure("new");
    }
    return nullptr;
}


OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce,
                                     IN_ITEMS_LIST& itemsList,
                                     int multiThreadLevel) {
    try {
        initFramework(multiThreadLevel);

        logFile = std::fopen(".MapReduceFramework.log", "w");

        std::fprintf(logFile, "runMapReduceFramework started with %d threads\n",
                     multiThreadLevel);

        struct timeval startTime, finishTime;
        int err1 = gettimeofday(&startTime, NULL);

        pthread_t shuffleThread;
        pthread_t *threadMapArray = new pthread_t[multiThreadLevel];
        inputVec = {std::make_move_iterator(std::begin(itemsList)),
                    std::make_move_iterator(std::end(itemsList))};

        for (int i = 0; i < multiThreadLevel; ++i) {
            pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
            mutexVector.push_back(&mutex);
        }
        mutexIter = mutexVector.begin();

        for (int i = 0; i < multiThreadLevel; ++i) {
            if (pthread_create(&threadMapArray[i], NULL, ExecMap,
                               (void *) &mapReduce)) {
                mapReduceFrameworkFailure("pthread_create");
            }
        }
        if(pthread_create(&shuffleThread, NULL, shuffle, NULL)){
            mapReduceFrameworkFailure("pthread_create");
        }


    for (int i = 0; i < multiThreadLevel; ++i) {
        if (pthread_join(threadMapArray[i], NULL)) {
            mapReduceFrameworkFailure("pthread_join");
        }
    }
    if (pthread_join(shuffleThread, NULL)) {
        mapReduceFrameworkFailure("pthread_join");
    }
    for (auto iter = levelTwoVec.begin(); iter != levelTwoVec.end(); iter++) {
        if (iter->second.first != NULL) {
            delete iter->second.first;
        }
    }

    delete[] threadMapArray;

        int err2 = gettimeofday(&finishTime, NULL);

        if (err1 == -1 || err2 == -1) {
            mapReduceFrameworkFailure("gettimeofday");

        }
        unsigned long totalTime = getTimeSpan(startTime.tv_usec,
                                              finishTime.tv_usec,
                                              startTime.tv_sec,
                                              finishTime.tv_sec);

        err1 = gettimeofday(&startTime, NULL);

        pthread_t *threadReduceArray = new pthread_t[multiThreadLevel];

        for (int i = 0; i < multiThreadLevel; ++i) {
            if (pthread_create(&threadReduceArray[i], NULL, ExecReduce,
                               (void *) &mapReduce)) {
                mapReduceFrameworkFailure("pthread_create");
            }
        }

        for (int i = 0; i < multiThreadLevel; ++i) {
            if (pthread_join(threadReduceArray[i], NULL)) {
                mapReduceFrameworkFailure("pthread_join");
            }
        }

        delete[] threadReduceArray;

        resultedOutputLists = addToLvlThreeVec(&levelThreeVec);

        resultedOutputLists.sort([](const std::pair<k3Base *, v3Base *> &first,
                                    const std::pair<k3Base *, v3Base *> &second)
                                         -> bool {
            const k3Base *firstKey = (first.first);
            const k3Base *secondKey = (second.first);

            return (*firstKey < *secondKey);
        });

        err2 = gettimeofday(&finishTime, NULL);
        if (err1 == -1 || err2 == -1) {
            mapReduceFrameworkFailure("gettimeofday");
        }

        std::fprintf(logFile, "Map and Shuffle took %lu ns\n", totalTime);

        totalTime = getTimeSpan(startTime.tv_usec, finishTime.tv_usec,
                                startTime.tv_sec, finishTime.tv_sec);

        std::fprintf(logFile, "Reduce took %lu ns\n", totalTime);

        std::fprintf(logFile, "runMapReduceFramework finished\n");

        std::fclose(logFile);

        return resultedOutputLists;

    }catch(std::bad_alloc e){
        mapReduceFrameworkFailure("new");
    }
    return std::list<OUT_ITEM>();
}

