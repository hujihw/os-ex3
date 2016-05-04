// ExecMap.cpp

#include "ExecMap.h"

///////////////////////////////////
// ExecMap Class Implementations //
///////////////////////////////////

// ------------------------ Singleton Methods ----------------------------------

void ExecMap::ExecMap() { }

ExecMap &ExecMap::getInstance() {
    static ExecMap instance;
    return instance;
}

// --------------------------- Other Methods -----------------------------------

void ExecMap::MapFunctionExec(MapReduceBase &mapReduce,
                              unsigned int iterations, IN_ITEMS_LIST& itemsList)
{
    // todo choose container (located here and in the framework)
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
