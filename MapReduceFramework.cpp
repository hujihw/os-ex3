// MapReduceFramework.cpp

#include <map>
#include "MapReduceFramework.h"
#include "MapManager.h"



// todo sort the output data

// todo log file time measuring (consider different class\namespace)

// todo error handling function

typedef std::map<k2Base* ,std::list<v2Base*>> ShuffledMap;

OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase &mapReduce,
                                     IN_ITEMS_LIST &itemsList,
                                     int multiThreadLevel) {

    //  the map that contains the output of the shuffle function.
    ShuffledMap shuffledMap;

    // get a map manager instance

    // call the start mapping phase method

    // get a reduce manager instance

    // call the start reduce phase method


    return std::list<OUT_ITEM>();
}
