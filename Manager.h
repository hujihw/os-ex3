// Manager.h

#include <vector>
#include <pthread.h>
#include <map>
//#include <bits/unordered_map.h>
#include "MapReduceFramework.h"

#ifndef EX3_MANAGER_H
#define EX3_MANAGER_H


typedef struct mutexContainer
{
    std::list<k2Base, v2Base> threadContainer;
    pthread_mutex_t threadMutex;

    mutexContainer() : threadContainer(), threadMutex(PTHREAD_MUTEX_INITIALIZER)
    {

    }
} mutexContainer;


class Manager {
public:
    Manager(int multiThreadLevel);

    virtual ~Manager() = delete;

    void threadCreator(void* (*start_routine) (void *), std::map &threadsMap);

protected:

    static int _multiThreadLevel;

    pthread_mutex_t mapExecThreadMapMutex;

    std::vector<pthread_t*> _threads;

    std::map<pthread_t, mutexContainer*> _threadsMap;
};


#endif //EX3_MANAGER_H
