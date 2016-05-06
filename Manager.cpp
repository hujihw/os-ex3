// Manager.cpp

#include "Manager.h"


Manager::Manager(int multiThreadLevel) : _multiThreadLevel(multiThreadLevel)
{

}


void Manager::threadCreator(void* (*start_routine) (void *),
                            std::map &threadsMap)
{
    // lock the threads map so shuffle could not read from it while initializing
    mapExecThreadMapMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&mapExecThreadMapMutex);

    // create the threads to execute map/reduce function
    for (int i = 0; i < Manager::_multiThreadLevel; ++i)
    {
        pthread_create(Manager::_threads[i], nullptr ,start_routine, nullptr); // todo error handling
        _threadsMap[*Manager::_threads[i]] = new mutexContainer();
    }

    // unlock the thread map after initialization
    pthread_mutex_unlock(&mapExecThreadMapMutex);

    // join all threads when they finish
    for (int j = 0; j < _multiThreadLevel; ++j) {
        pthread_join(*_threads[j], nullptr);
    }
}
