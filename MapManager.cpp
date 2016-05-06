// ExecMap.cpp

#include "MapManager.h"

///////////////////////////////////
// MapManager Class Implementations //
///////////////////////////////////

// ------------------------ Singleton Methods ----------------------------------

MapManager::MapManager(int multiThreadLevel,
                       IN_ITEMS_LIST &inItemsList) : Manager(multiThreadLevel),
                                                     _inItemsList(inItemsList){}

MapManager &MapManager::getInstance(int multiThreadLevel,
                                    IN_ITEMS_LIST &inItemsList)
{
    static MapManager instance(multiThreadLevel, inItemsList);
    return instance;
}

// --------------------------- Other Methods -----------------------------------


ShuffledMap* MapManager::RunMappingPhase(void (*start_routine)(const k1Base *const, const v1Base *const)) { // todo
    // create threads
    std::cout << "starting the MAPPING PHaSE" << std::endl;
    threadCreator(MapManager::ExecMap, MapManager::_threadsMap);
}

void * MapManager::ExecMap(void *start_routine) { //todo
    std::cout << "inaal dinak" << std::endl;
//    int chunkSize = 10;
//
//    // try to get a chunk from the input container
//        // if index is locked (?)
//        // if remainder is less than 10, get only the remainder // todo talk to benben
//
//    // run Map function in a loop
//    for (int i = 0; i < chunkSize; ++i)
//    {
//
//    }
}

void Emit2(k2Base *, v2Base *) // todo
{

}

// todo map function executor
    // loop over "iterations"
        // while there is still available data
            // call Map function on the relevant index
            // add value to container located in the thread using Emit2 function
    // todo what happens when iterations end? (maybe add container to the general container?)
