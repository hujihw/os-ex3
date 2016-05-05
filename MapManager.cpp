// ExecMap.cpp

#include "MapManager.h"

///////////////////////////////////
// MapManager Class Implementations //
///////////////////////////////////

// ------------------------ Singleton Methods ----------------------------------

void MapManager::MapManager() { }

MapManager &MapManager::getInstance() {
    static MapManager instance;
    return instance;
}

// --------------------------- Other Methods -----------------------------------


void Emit2(k2Base *, v2Base *) // todo
{

}

// todo map function executor
    // loop over "iterations"
        // while there is still available data
            // call Map function on the relevant index
            // add value to container located in the thread using Emit2 function
    // todo what happens when iterations end? (maybe add container to the general container?)
