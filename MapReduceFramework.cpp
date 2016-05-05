// MapReduceFramework.cpp

#include <map>
#include "MapReduceFramework.h"
#include "MapManager.h"

using namespace std;

typedef map<k2Base* ,list<v2Base*>> ShuffledMap; // maybe typedef the list also

class MapReduceFramework
{
public:
    // singleton design pattern
    static MapReduceFramework& getInstance();

    void insertToShuffledMap(k2Base * k2, list<v2Base *> v2Lists);
    ShuffledMap::const_iterator getIteratorShuffledMap();

private:
    // singleton design pattern
    MapReduceFramework() {};
    MapReduceFramework(MapReduceFramework const&);
    void operator=(MapReduceFramework const&);

//  the map that contains the output of the shuffle function.
    ShuffledMap _shuffledMap;

};

MapReduceFramework &MapReduceFramework::getInstance() {
    static MapReduceFramework instance;
    return instance;
}

void MapReduceFramework::insertToShuffledMap(k2Base * k2, list<v2Base *> v2Lists) {
    _shuffledMap[k2] = v2Lists;
}

ShuffledMap::const_iterator MapReduceFramework::getIteratorShuffledMap() {
    return _shuffledMap.begin();
}

// todo sort the output data

// todo log file time measuring (consider different class\namespace)

// todo error handling function


OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase &mapReduce,
                                     IN_ITEMS_LIST &itemsList,
                                     int multiThreadLevel) {
    // create a map manager instance

    // call the start mapping phase function

    //





    return std::list<OUT_ITEM>();
}
